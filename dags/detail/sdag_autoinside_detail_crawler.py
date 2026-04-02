import csv
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from util.common_util import CommonUtil
from util.playwright_util import GotoSpec, goto_with_retry, images_enabled, install_route_blocking


# ═══════════════════════════════════════════════════════════════════
#  상수
# ═══════════════════════════════════════════════════════════════════

SOURCE_LIST_TABLE = "ods.ods_car_list_autoinside"
TARGET_DETAIL_TABLE = "ods.ods_car_detail_autoinside"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"  # Airflow Variable 키
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"  # 예: /home/limhayoung/data/img
SITE_NAME = "오토인사이드"

DETAIL_CSV_FIELDS = [
    "model_sn",
    "product_id",
    "car_name",
    "year",
    "km",
    "car_spec",
    "car_num",
    "car_color",
    "category",
    "inspection",
    "insurance",
    "car_opt",
    "car_history",
    "car_inspect",
    "car_imgs",
    "date_crtr_pnttm",
    "create_dt",
]


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의
# ═══════════════════════════════════════════════════════════════════


@dag(
    dag_id="sdag_autoinside_detail_crawl",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "autoinside", "detail", "crawler"],
)
def autoinside_detail_crawl():
    """오토인사이드 상세페이지 크롤링 DAG (register_flag = 'A' 신규, 최신 data_crtr_pnttm 배치만)."""

    @task
    def fetch_target_urls() -> list[dict[str, str]]:
        """
        ods.ods_car_list_autoinside 에서
        register_flag = 'A' 이고 data_crtr_pnttm 이 테이블 내 최신 적재일자와 같은 행만 조회한다.
        """
        sql = f"""
        SELECT
            l.product_id,
            l.detail_url,
            l.register_flag
        FROM {SOURCE_LIST_TABLE} l
        WHERE TRIM(COALESCE(l.register_flag, '')) = 'A'
          AND l.detail_url IS NOT NULL
          AND TRIM(l.detail_url) != ''
          AND l."data_crtr_pnttm" IS NOT NULL
          AND l."data_crtr_pnttm" = (
              SELECT MAX(m."data_crtr_pnttm")
              FROM {SOURCE_LIST_TABLE} m
              WHERE m."data_crtr_pnttm" IS NOT NULL
          )
        ORDER BY l.model_sn
        """
        logging.info("select_target_urls_stmt ::: %s", sql)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        conn = hook.get_conn()
        rows = []
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT MAX(m."data_crtr_pnttm")
                    FROM {SOURCE_LIST_TABLE} m
                    WHERE m."data_crtr_pnttm" IS NOT NULL
                    """
                )
                max_row = cur.fetchone()
                latest_pnttm = max_row[0] if max_row else None
                logging.info(
                    "오토인사이드 detail 수집 기준 data_crtr_pnttm(최신): %s",
                    latest_pnttm,
                )
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(cols, row)))
        finally:
            try:
                conn.close()
            except Exception:
                pass

        logging.info("수집 대상: %d건", len(rows))
        if not rows:
            raise ValueError("수집 대상 URL이 없습니다. 테이블을 확인하세요.")
        return rows

    @task
    def summarize_targets(target_rows: list[dict[str, str]]) -> list[dict[str, str]]:
        n = len(target_rows)
        with_url = sum(1 for r in target_rows if str(r.get("detail_url") or "").strip())
        logging.info("상세 크롤 준비: 총 %d건, detail_url 있음 %d건", n, with_url)
        if not target_rows:
            raise ValueError("summarize_targets: 대상이 비어 있습니다.")
        return target_rows

    @task
    def crawl_and_save_csv(target_rows: list[dict[str, str]]) -> str:
        # Airflow DAG 파싱 단계에서 playwright 미설치/무거운 import로 DAGFileProcessor가 죽는 문제 방지
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"autoinside_detail_{run_ts}.csv"
        logging.info("출력 파일: %s", csv_path)

        detail_img_dir = _get_detail_img_dir()
        detail_img_dir.mkdir(parents=True, exist_ok=True)
        CommonUtil.clear_image_files(detail_img_dir, recursive=False)
        logging.info("상세 이미지 저장 디렉터리: %s", detail_img_dir.resolve())

        total = len(target_rows)
        collected = 0
        failed = 0
        skipped = 0
        recycle_every = 300

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )
            install_route_blocking(context)
            page = context.new_page()
            page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            for idx, row in enumerate(target_rows, 1):
                product_id = str(row.get("product_id") or "").strip()
                detail_url = str(row.get("detail_url") or "").strip()

                if not detail_url:
                    skipped += 1
                    continue

                if idx == 1 or idx % 50 == 0 or idx == total:
                    logging.info(
                        "[%d/%d] 호출 대상 - product_id=%s, detail_url=%s",
                        idx,
                        total,
                        product_id,
                        detail_url,
                    )

                try:
                    detail_data = _crawl_one(page, idx, product_id, detail_url, detail_img_dir)
                    if detail_data:
                        _save_to_csv_append(csv_path, DETAIL_CSV_FIELDS, detail_data)
                        collected += 1
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    logging.exception(
                        "[%d/%d] 상세 수집 예외 - product_id=%s, detail_url=%s, err=%s",
                        idx,
                        total,
                        product_id,
                        detail_url,
                        e,
                    )

                if idx % 100 == 0 or idx == total:
                    logging.info(
                        "상세 수집 진행: processed=%d/%d, collected=%d, failed=%d, skipped=%d",
                        idx,
                        total,
                        collected,
                        failed,
                        skipped,
                    )

                if idx % recycle_every == 0 and idx < total:
                    try:
                        page.close()
                    except Exception:
                        pass
                    try:
                        context.close()
                    except Exception:
                        pass
                    context = browser.new_context(
                        user_agent=(
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/122.0.0.0 Safari/537.36"
                        ),
                        viewport={"width": 1920, "height": 1080},
                    )
                    install_route_blocking(context)
                    page = context.new_page()
                    page.add_init_script(
                        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                    )
                    logging.info("브라우저 컨텍스트 재생성 완료: processed=%d/%d", idx, total)

                time.sleep(0.3)

            browser.close()

        logging.info(
            "✅ 완료: collected=%d, failed=%d, skipped=%d, total=%d → %s",
            collected,
            failed,
            skipped,
            total,
            csv_path,
        )
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV 생성 실패: {csv_path}")
        return str(csv_path)

    @task
    def load_detail_csv_to_ods(csv_path: str) -> dict[str, Any]:
        """
        crawl_and_save_csv 결과 CSV를 ods.ods_car_detail_autoinside로 적재.
        - 테이블 컬럼 기준으로 CSV 컬럼을 자동 필터링
        - truncate 없이 append insert
        """
        p = Path(str(csv_path or ""))
        if not p.is_file():
            raise FileNotFoundError(f"적재 대상 CSV가 없습니다: {p}")

        rows = _read_csv_rows(p)
        if not rows:
            raise ValueError(f"적재할 CSV 데이터가 없습니다: {p}")

        hook = PostgresHook(postgres_conn_id="car_db_conn")
        _bulk_insert_rows(hook, TARGET_DETAIL_TABLE, rows, truncate=False, allow_only_table_cols=True)
        _mark_autoinside_list_rows_processed(hook, SOURCE_LIST_TABLE, rows)
        table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
        logging.info(
            "오토인사이드 detail CSV 적재 완료: table=%s, inserted_rows=%d, table_count=%d, csv=%s",
            TARGET_DETAIL_TABLE,
            len(rows),
            table_count,
            p,
        )
        return {
            "done": True,
            "target_table": TARGET_DETAIL_TABLE,
            "row_count": len(rows),
            "table_count": table_count,
            "csv_path": str(p),
        }

    @task_group(group_id="prepare_detail_crawl")
    def prepare_detail_crawl():
        rows = fetch_target_urls()
        return summarize_targets(rows)

    @task_group(group_id="crawl_and_persist")
    def crawl_and_persist(target_rows: list[dict[str, str]]):
        return crawl_and_save_csv(target_rows)

    prepared = prepare_detail_crawl()
    csv_path = crawl_and_persist(prepared)
    load_detail_csv_to_ods(csv_path)


dag_object = autoinside_detail_crawl()


if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    dag_object.test(
        execution_date=datetime(2025, 10, 10, 8, 0),
        conn_file_path=conn_path,
    )


# ═══════════════════════════════════════════════════════════════════
#  경로 유틸
# ═══════════════════════════════════════════════════════════════════


def _get_output_dir() -> Path:
    """
    Airflow Variable: used_car_final_file_path 기준 오늘 날짜 경로 반환.
    예) /home/limhayoung/data/crawl/2026년/오토인사이드/20260327
    """
    try:
        base = Path(Variable.get(FINAL_FILE_PATH_VAR))
    except Exception:
        base = Path("/home/limhayoung/data/crawl")
        logging.warning(
            "Airflow Variable '%s' 조회 실패 → 기본 경로 사용: %s",
            FINAL_FILE_PATH_VAR,
            base,
        )
    return CommonUtil.build_dated_site_path(base, SITE_NAME, datetime.now())


def _get_detail_img_dir() -> Path:
    """
    Airflow Variable used_car_image_file_path 기준
    {img_root}/YYYY년/오토인사이드/detail
    예) /home/limhayoung/data/img/2026년/오토인사이드/detail
    """
    try:
        img_root = Path(str(Variable.get(IMAGE_FILE_PATH_VAR)).strip())
    except Exception:
        img_root = Path("/home/limhayoung/data/img")
        logging.warning(
            "Airflow Variable '%s' 조회 실패 → 기본 경로 사용: %s",
            IMAGE_FILE_PATH_VAR,
            img_root,
        )
    year_site = CommonUtil.build_year_site_path(img_root, SITE_NAME, datetime.now())
    return year_site / "detail"


# ═══════════════════════════════════════════════════════════════════
#  CSV 유틸
# ═══════════════════════════════════════════════════════════════════


def _get_now_times() -> tuple[str, str]:
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%Y%m%d%H%M")


def _csv_cell_excel_text(val: Any) -> Any:
    """숫자-숫자 패턴 Excel 날짜 변환 방지."""
    if val is None:
        return ""
    s = (val if isinstance(val, str) else str(val)).strip()
    if s and re.match(r"^\d+-\d+$", s):
        return "'" + s
    return s


def _save_to_csv_append(file_path: Path, fieldnames: list[str], data: dict[str, Any]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = file_path.exists()
    row = {
        k: _csv_cell_excel_text(v) if (isinstance(v, str) or v is None) else v
        for k, v in data.items()
    }
    with open(file_path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _read_csv_rows(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _split_schema_table(full_name: str) -> tuple[str, str]:
    if "." in full_name:
        schema, table = full_name.split(".", 1)
        return schema.strip(), table.strip()
    return "public", full_name.strip()


def _get_table_columns(hook: PostgresHook, full_table_name: str) -> list[str]:
    schema, table = _split_schema_table(full_table_name)
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = %s
    ORDER BY ordinal_position
    """
    rows = hook.get_records(sql, parameters=(schema, table))
    return [r[0] for r in rows]


def _bulk_insert_rows(
    hook: PostgresHook,
    full_table_name: str,
    rows: list[dict[str, Any]],
    *,
    truncate: bool = False,
    allow_only_table_cols: bool = True,
) -> None:
    if not rows:
        return

    table_cols = _get_table_columns(hook, full_table_name) if allow_only_table_cols else []
    table_col_set = set(table_cols)

    candidate_cols: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in candidate_cols:
                candidate_cols.append(key)

    insert_cols = [c for c in candidate_cols if c in table_col_set] if table_cols else candidate_cols
    if not insert_cols:
        raise ValueError(f"insert 가능한 컬럼이 없습니다. table={full_table_name}")

    values = [tuple(row.get(col) for col in insert_cols) for row in rows]

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            if truncate:
                cur.execute(f"TRUNCATE TABLE {full_table_name}")

            from psycopg2.extras import execute_values

            cols_sql = ", ".join([f'"{col}"' for col in insert_cols])
            sql = f"INSERT INTO {full_table_name} ({cols_sql}) VALUES %s"
            execute_values(cur, sql, values, page_size=2000)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _mark_autoinside_list_rows_processed(
    hook: PostgresHook,
    source_list_table: str,
    detail_rows: list[dict[str, Any]],
    *,
    key_col: str = "product_id",
    flag_col: str = "register_flag",
) -> int:
    """
    detail 적재 성공 후, 해당 list row의 register_flag를 'Y'로 표시한다.
    register_flag가 'A'이면서 data_crtr_pnttm이 테이블 전체 최신값인 행만 갱신한다.
    """
    product_ids = sorted({str(r.get(key_col) or "").strip() for r in detail_rows if str(r.get(key_col) or "").strip()})
    if not product_ids:
        return 0

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {source_list_table} u
                SET "{flag_col}" = 'Y'
                WHERE u."{key_col}" = ANY(%s)
                  AND TRIM(COALESCE(u."{flag_col}", '')) = 'A'
                  AND u."data_crtr_pnttm" IS NOT NULL
                  AND u."data_crtr_pnttm" = (
                      SELECT MAX(m."data_crtr_pnttm")
                      FROM {source_list_table} m
                      WHERE m."data_crtr_pnttm" IS NOT NULL
                  )
                """,
                (product_ids,),
            )
            updated = int(cur.rowcount or 0)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    logging.info("오토인사이드 list register_flag 처리완료 업데이트: updated=%d", updated)
    return updated


# ═══════════════════════════════════════════════════════════════════
#  파싱 헬퍼
# ═══════════════════════════════════════════════════════════════════


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _safe_text(locator) -> str:
    try:
        return _norm_space(locator.first.inner_text() or "")
    except Exception:
        return ""


def _safe_attr(locator, attr: str) -> str:
    try:
        return (locator.first.get_attribute(attr) or "").strip()
    except Exception:
        return ""


def _collect_data_nm(li_locator) -> str:
    try:
        n = li_locator.count()
        parts: list[str] = []
        for i in range(n):
            nm = (li_locator.nth(i).get_attribute("data-nm") or "").strip()
            if nm:
                parts.append(_norm_space(nm))
        return " | ".join(parts)
    except Exception:
        return ""


def _download_image(page, image_url: str, save_path: Path) -> bool:
    if not images_enabled():
        return False
    try:
        headers = {
            "Referer": (page.url or "https://www.autoinside.co.kr/").split("#")[0],
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
        }
        resp = page.request.get(image_url, timeout=30000, headers=headers)
        if not resp or not resp.ok:
            return False
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(resp.body())
        return True
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════
#  단일 상세페이지 크롤링
# ═══════════════════════════════════════════════════════════════════


def _crawl_one(
    page,
    idx: int,
    product_id: str,
    detail_url: str,
    detail_img_dir: Path,
) -> dict[str, Any] | None:
    d_pnttm, c_dt = _get_now_times()
    data: dict[str, Any] = {f: "" for f in DETAIL_CSV_FIELDS}
    data["model_sn"] = idx
    data["product_id"] = product_id
    data["car_imgs"] = str(detail_img_dir.resolve())
    data["date_crtr_pnttm"] = d_pnttm
    data["create_dt"] = c_dt

    for attempt in range(3):
        try:
            goto_with_retry(
                page,
                GotoSpec(
                    detail_url,
                    wait_until="commit",
                    timeout_ms=90_000,
                    ready_selectors=(".page.car_view_wrap,body",),
                    ready_timeout_ms=20_000,
                ),
                logger=logging.getLogger(__name__),
                attempts=1,
            )
            # 고정 대기 대신 최소 대기(대부분 selector 로드로 충분)
            page.wait_for_timeout(250)
            break
        except Exception as e:
            if attempt < 2:
                logging.warning("재시도 (%d/3): %s - %s", attempt + 2, product_id, e)
                time.sleep(2)
            else:
                logging.error("접속 실패: %s - %s", product_id, e)
                return None

    root = page.locator("#wrap #frm .container .container_inn .page.car_view_wrap").first
    if root.count() == 0:
        # fallback: 페이지 구조가 다를 때
        root = page.locator(".page.car_view_wrap").first

    try:
        # ── 우측 사이드: 차량명/스펙/기타정보 ────────────────────────────
        side = root.locator(".car_view_side .car_view_side_inn").first
        price_wrap = side.locator(".car_view_price_wrap").first

        data["car_name"] = _safe_text(price_wrap.locator(".car_nm.carName"))

        spec_spans = price_wrap.locator(".car_spec span")
        try:
            if spec_spans.count() >= 1:
                data["year"] = _norm_space(spec_spans.nth(0).inner_text() or "")
            if spec_spans.count() >= 2:
                data["km"] = _norm_space(spec_spans.nth(1).inner_text() or "")
            if spec_spans.count() >= 3:
                data["car_spec"] = _norm_space(spec_spans.nth(2).inner_text() or "")
            if spec_spans.count() >= 4:
                data["car_num"] = _norm_space(spec_spans.nth(3).inner_text() or "")
            if spec_spans.count() >= 5:
                data["car_color"] = _norm_space(spec_spans.nth(4).inner_text() or "")
        except Exception:
            pass

        etc_lis = side.locator(".car_info_etc li")
        try:
            if etc_lis.count() >= 1:
                data["category"] = _safe_text(
                    etc_lis.nth(0).locator(".etc_box.link_tooltip .txt.main")
                )
            if etc_lis.count() >= 2:
                data["inspection"] = _safe_text(
                    etc_lis.nth(1).locator(".etc_box.link_tooltip .txt")
                )
            if etc_lis.count() >= 3:
                data["insurance"] = _safe_text(
                    etc_lis.nth(2).locator(".etc_box.link_tooltip .txt")
                )
        except Exception:
            pass

        # ── 옵션: data-nm 속성 join ─────────────────────────────────────
        # 오토인사이드는 옵션명이 보통 `li > a.item[data-nm]`에 존재한다.
        # car_opt 섹션이 여러 번 등장할 수 있어(중첩/레이아웃 분기),
        # 모든 섹션을 순회하며 data-nm 값을 |로 합친다.
        try:
            opt_sections = root.locator(".car_view_content .section.car_opt")
            seen: set[str] = set()
            opt_parts: list[str] = []
            for si in range(opt_sections.count()):
                sec = opt_sections.nth(si)
                # 1) 일반 케이스: a.item[data-nm]
                nm_nodes = sec.locator(".list a.item[data-nm]")
                # 2) fallback: 리스트 내부의 어떤 엘리먼트든 data-nm가 있으면 수집
                if nm_nodes.count() == 0:
                    nm_nodes = sec.locator(".list [data-nm]")

                for ni in range(nm_nodes.count()):
                    nm = (nm_nodes.nth(ni).get_attribute("data-nm") or "").strip()
                    nm = _norm_space(nm)
                    if not nm or nm in seen:
                        continue
                    seen.add(nm)
                    opt_parts.append(nm)
            data["car_opt"] = " | ".join(opt_parts)
        except Exception:
            data["car_opt"] = ""

        # ── 히스토리: tit/txt join ──────────────────────────────────────
        hist_section = root.locator(".car_view_content .section.car_history").first
        hist_items = hist_section.locator(".list li.item")
        try:
            parts: list[str] = []
            for i in range(hist_items.count()):
                it = hist_items.nth(i)
                tit = _safe_text(it.locator(".tit"))
                txt = _safe_text(it.locator(".txt"))
                if tit and txt:
                    parts.append(f"{tit} : {txt}")
                elif tit:
                    parts.append(tit)
                elif txt:
                    parts.append(txt)
            data["car_history"] = " | ".join(parts)
        except Exception:
            data["car_history"] = ""

        # ── 성능점검: (span1 : span2) join ──────────────────────────────
        insp_section = root.locator(".car_view_content .section.car_inspect").first
        boxes = insp_section.locator(".inspect_wrap .inspect_box .inspect_img .txt")
        try:
            parts: list[str] = []
            for i in range(boxes.count()):
                box = boxes.nth(i)
                spans = box.locator("span")
                if spans.count() < 2:
                    continue
                k = _norm_space(spans.nth(0).inner_text() or "")
                v = _norm_space(spans.nth(1).inner_text() or "")
                if k and v:
                    parts.append(f"{k} : {v}")
            data["car_inspect"] = " | ".join(parts)
        except Exception:
            data["car_inspect"] = ""

        # ── 상세 이미지 저장 ─────────────────────────────────────────────
        try:
            img_root = root.locator(
                ".car_view_content .section.car_img_wrap .img_section.on "
                ".main_slide .swiper-wrapper .swiper-slide.cmn_slide img"
            )
            if img_root.count() == 0:
                img_root = root.locator(
                    ".car_view_content .section.car_img_wrap .main_slide img"
                )
            seen: set[str] = set()
            page_url = page.url or "https://www.autoinside.co.kr/"
            for i in range(img_root.count()):
                img = img_root.nth(i)
                src = (img.get_attribute("data-src") or img.get_attribute("src") or "").strip()
                if not src:
                    continue
                if src.startswith("//"):
                    src = "https:" + src
                elif src.startswith("/"):
                    src = urljoin(page_url, src)
                elif not src.startswith("http"):
                    src = urljoin(page_url, src)
                if src in seen:
                    continue
                seen.add(src)
                out = detail_img_dir / f"{product_id}_{len(seen)}.png"
                _download_image(page, src, out)
        except Exception as e:
            logging.debug("[갤러리 이미지] %s : %s", product_id, e)

    except Exception as e:
        logging.error("파싱 전체 오류: %s - %s", product_id, e)
        return None

    # 최소 핵심값이 비어있으면 실패로 처리(품질 방어)
    core_cols = ("car_name", "year", "km")
    filled_core = sum(1 for c in core_cols if str(data.get(c) or "").strip())
    if filled_core == 0:
        return None

    return data

