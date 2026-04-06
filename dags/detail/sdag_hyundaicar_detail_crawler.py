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

SOURCE_LIST_TABLE = "ods.ods_car_list_hyundaicar"
TARGET_DETAIL_TABLE = "ods.ods_car_detail_hyundaicar"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"  # 예: /home/limhayoung/data/img/ # 예: /mnt/d/data/img
SITE_NAME = "현대차"

DETAIL_CSV_FIELDS = [
    "model_sn",
    "product_id",
    "car_name",
    "year",
    "km",
    "car_pay",
    "installment",
    "operation_period",
    "manufacturer_guarantee",
    "inspection",
    "accident_history",
    "initial_registration",
    "mileage",
    "car_fuel",
    "engine",
    "car_ext_color",
    "car_int_color",
    "car_type",
    "drive_sys",
    "car_num",
    "model_year",
    "transmission",
    "car_history_1",
    "car_history_2",
    "car_report_1",
    "car_report_2",
    "notice",
    "guarantee_1",
    "guarantee_2",
    "options",
    "car_imgs",
    "date_crtr_pnttm",
    "create_dt",
]


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의
# ═══════════════════════════════════════════════════════════════════


@dag(
    dag_id="sdag_hyundaicar_detail_crawl",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "hyundaicar", "detail", "crawler"],
)
def hyundaicar_detail_crawl():
    """현대차 상세: register_flag='A'(최신 스냅샷)만 수집 → 행마다 List complete_yn Y/N, Detail 적재·동기화. register_flag 변경은 list 동기화 전용."""

    @task
    def fetch_target_urls() -> list[dict[str, str]]:
        """
        ods.ods_car_list_hyundaicar 에서 register_flag=A 이고
        date_crtr_pnttm 이 테이블 내 최신 적재일과 같은 행만 조회.
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
          AND l."date_crtr_pnttm" IS NOT NULL
          AND l."date_crtr_pnttm" = (
              SELECT MAX(m."date_crtr_pnttm")
              FROM {SOURCE_LIST_TABLE} m
              WHERE m."date_crtr_pnttm" IS NOT NULL
          )
        ORDER BY l.model_sn
        """
        logging.info("select_target_urls_stmt ::: %s", sql)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        conn = hook.get_conn()
        rows: list[dict[str, str]] = []
        latest_pnttm = None
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT MAX(m."date_crtr_pnttm")
                    FROM {SOURCE_LIST_TABLE} m
                    WHERE m."date_crtr_pnttm" IS NOT NULL
                    """
                )
                max_row = cur.fetchone()
                latest_pnttm = max_row[0] if max_row else None
                logging.info(
                    "현대차 detail 수집 기준 date_crtr_pnttm(최신): %s",
                    latest_pnttm,
                )
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall() or []:
                    rows.append(dict(zip(cols, row)))

                if not rows and latest_pnttm is not None:
                    cur.execute(
                        f"""
                        SELECT
                            COUNT(*) AS total_latest,
                            COUNT(*) FILTER (
                                WHERE TRIM(COALESCE(register_flag, '')) = 'A'
                            ) AS cnt_a,
                            COUNT(*) FILTER (
                                WHERE detail_url IS NOT NULL AND TRIM(detail_url) <> ''
                            ) AS cnt_url
                        FROM {SOURCE_LIST_TABLE}
                        WHERE "date_crtr_pnttm" = %s
                        """,
                        (latest_pnttm,),
                    )
                    diag = cur.fetchone()
                    if diag:
                        total_l, cnt_a, cnt_url = int(diag[0] or 0), int(diag[1] or 0), int(diag[2] or 0)
                        if cnt_a == 0:
                            logging.info(
                                "현대차 상세: 최신 적재일(%s) 기준 신규(register_flag=A) 0건 → "
                                "상세 크롤 생략 후 DAG 정상 완료로 진행. "
                                "(최신일 전체 행=%s, detail_url 보유 행=%s)",
                                latest_pnttm,
                                total_l,
                                cnt_url,
                            )
                        else:
                            logging.warning(
                                "상세 대상 0건: 최신일(%s)에 신규(A)=%s건 있으나 "
                                "detail_url이 있는 신규만 수집하므로 매칭 0건 (detail_url 있음=%s).",
                                latest_pnttm,
                                cnt_a,
                                cnt_url,
                            )
        finally:
            try:
                conn.close()
            except Exception:
                pass

        logging.info("수집 대상: %d건", len(rows))
        if not rows:
            logging.info("수집할 데이터가 없습니다.")
        if not rows and latest_pnttm is None:
            logging.warning(
                "수집 대상 없음: %s 에 date_crtr_pnttm 최신값이 없습니다.",
                SOURCE_LIST_TABLE,
            )
        return rows

    @task
    def summarize_targets(target_rows: list[dict[str, str]]) -> list[dict[str, str]]:
        n = len(target_rows)
        with_url = sum(1 for r in target_rows if str(r.get("detail_url") or "").strip())
        logging.info("상세 크롤 준비: 총 %d건, detail_url 있음 %d건", n, with_url)
        if not target_rows:
            logging.info("상세 수집 대상 0건 — 다음 태스크에서 헤더만 CSV 생성 후 정상 완료합니다.")
        return target_rows

    @task
    def crawl_and_save_csv(target_rows: list[dict[str, str]]) -> str:
        # Airflow DAG 파싱 단계에서 playwright 미설치/무거운 import로 불안정해지는 문제 방지
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"hyundaicar_detail_{run_ts}.csv"
        logging.info("출력 파일: %s", csv_path)

        detail_base = _get_detail_img_dir()
        detail_base.mkdir(parents=True, exist_ok=True)
        logging.info(
            "상세 이미지 루트(삭제 없음): …/detail/{product_id}/{product_id}_N.png → %s",
            detail_base.resolve(),
        )

        total = len(target_rows)
        collected = 0
        failed = 0
        skipped = 0
        recycle_every = 200
        pg_hook = PostgresHook(postgres_conn_id="car_db_conn")

        if total == 0:
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=DETAIL_CSV_FIELDS)
                w.writeheader()
            logging.info(
                "수집할 데이터가 없습니다. Playwright 생략, 헤더만 기록: %s",
                csv_path,
            )
            return str(csv_path)

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

                if not product_id:
                    skipped += 1
                    logging.warning(
                        "[상세수집실패] [%d/%d] product_id=(비어 있음) detail_url=%s "
                        "reason=product_id 없음 → complete_yn 갱신 불가, 스킵",
                        idx,
                        total,
                        (detail_url[:160] + "…") if len(detail_url) > 160 else detail_url or "(없음)",
                    )
                    continue

                if not detail_url:
                    skipped += 1
                    try:
                        CommonUtil.update_list_complete_yn_for_product_id(
                            pg_hook,
                            list_table=SOURCE_LIST_TABLE,
                            product_id=product_id,
                            value="N",
                            list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT,
                            register_flag_a_only=True,
                        )
                    except Exception:
                        logging.exception(
                            "[%d/%d] complete_yn=N DB 갱신 실패 (detail_url 없음) product_id=%s",
                            idx,
                            total,
                            product_id,
                        )
                    logging.warning(
                        "[상세수집실패] [%d/%d] product_id=%s detail_url=(없음) reason=detail_url 비어 있음 → complete_yn=N",
                        idx,
                        total,
                        product_id or "(빈값)",
                    )
                    continue

                if idx == 1 or idx % 50 == 0 or idx == total:
                    logging.info(
                        "[%d/%d] 호출 대상 - product_id=%s, detail_url=%s",
                        idx,
                        total,
                        product_id,
                        detail_url,
                    )

                per_detail_dir = detail_base / product_id
                per_detail_dir.mkdir(parents=True, exist_ok=True)

                success = False
                fail_reason: str | None = None
                try:
                    detail_data = _crawl_one(page, idx, product_id, detail_url, per_detail_dir)
                    if detail_data:
                        _save_to_csv_append(csv_path, DETAIL_CSV_FIELDS, detail_data)
                        success = True
                        collected += 1
                    else:
                        fail_reason = (
                            "상세 수집 결과 없음(접속 실패·파싱 오류·필수 필드 미충족)"
                        )
                        failed += 1
                except Exception as e:
                    fail_reason = f"{type(e).__name__}: {e}"
                    failed += 1
                    logging.exception(
                        "[상세수집실패] [%d/%d] product_id=%s detail_url=%s 예외 발생",
                        idx,
                        total,
                        product_id or "(빈값)",
                        detail_url,
                    )
                finally:
                    try:
                        yn = "Y" if success else "N"
                        CommonUtil.update_list_complete_yn_for_product_id(
                            pg_hook,
                            list_table=SOURCE_LIST_TABLE,
                            product_id=product_id,
                            value=yn,
                            list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT,
                            register_flag_a_only=True,
                        )
                    except Exception:
                        logging.exception(
                            "[%d/%d] complete_yn=%s DB 갱신 실패 product_id=%s",
                            idx,
                            total,
                            yn,
                            product_id,
                        )
                    if not success and fail_reason:
                        logging.error(
                            "[상세수집실패] [%d/%d] product_id=%s detail_url=%s reason=%s → complete_yn=N",
                            idx,
                            total,
                            product_id or "(빈값)",
                            detail_url,
                            fail_reason,
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

                time.sleep(0.25)

            browser.close()

        logging.info(
            "✅ 완료: collected=%d, failed=%d, skipped=%d, total=%d → %s",
            collected,
            failed,
            skipped,
            total,
            csv_path,
        )
        # 헤더를 미리 썼기 때문에, 여기서 파일이 없으면 진짜 경로/권한 문제다.
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV 생성 실패(경로/권한 확인): {csv_path}")
        return str(csv_path)

    @task
    def load_detail_csv_to_ods(csv_path: str) -> dict[str, Any]:
        """
        crawl_and_save_csv 결과 CSV를 ods.ods_car_detail_hyundaicar로 적재.
        - 테이블 컬럼 기준으로 CSV 컬럼을 자동 필터링, truncate 없이 append insert
        - 원천 Car List vs Detail 로 complete_yn Y/N (크롤 중 단건 갱신 후 최종 동기화, register_flag 미변경)
        """
        p = Path(str(csv_path or ""))
        if not p.is_file():
            raise FileNotFoundError(f"적재 대상 CSV가 없습니다: {p}")

        rows = _read_csv_rows(p)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        if not rows:
            logging.info(
                "현대차 상세 DAG 정상 완료: 신규(register_flag=A) 차량 없음 → detail INSERT 생략, "
                "원천 List complete_yn(Y/N)만 최신 스냅샷 기준으로 동기화합니다. csv=%s",
                p,
            )
            CommonUtil.refresh_car_list_complete_flag_vs_detail_ods(
                hook,
                list_table=SOURCE_LIST_TABLE,
                detail_table=TARGET_DETAIL_TABLE,
                list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT,
            )
            table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
            return {
                "done": True,
                "status": "completed_no_new_cars",
                "message": "신규 중고차 없음, 상세 수집 생략, complete_yn 동기화만 수행",
                "target_table": TARGET_DETAIL_TABLE,
                "row_count": 0,
                "table_count": table_count,
                "csv_path": str(p),
                "skipped_insert": True,
            }

        CommonUtil.bulk_insert_detail_ods_rows(hook, TARGET_DETAIL_TABLE, rows, truncate=False, allow_only_table_cols=True)
        CommonUtil.refresh_car_list_complete_flag_vs_detail_ods(
            hook,
            list_table=SOURCE_LIST_TABLE,
            detail_table=TARGET_DETAIL_TABLE,
            list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT,
        )
        table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
        logging.info(
            "현대차 detail CSV 적재 완료: table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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
            "skipped_insert": False,
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


dag_object = hyundaicar_detail_crawl()


if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    dag_object.test(
        execution_date=datetime(2026, 3, 31, 8, 0),
        conn_file_path=conn_path,
    )


# ═══════════════════════════════════════════════════════════════════
#  경로 유틸
# ═══════════════════════════════════════════════════════════════════


def _get_output_dir() -> Path:
    """
    Airflow Variable: used_car_final_file_path 기준 오늘 날짜 경로 반환.
    예) /home/limhayoung/data/crawl/2026년/현대차/20260331
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
    {img_root}/YYYY년/현대차/detail/{product_id}/{product_id}_N.png (폴더 비우지 않음).
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


def _join_kv_pairs(pairs: list[tuple[str, str]]) -> str:
    parts: list[str] = []
    for k, v in pairs:
        k2 = _norm_space(k)
        v2 = _norm_space(v)
        if not (k2 or v2):
            continue
        if k2 and v2:
            parts.append(f"{k2} : {v2}")
        elif k2:
            parts.append(k2)
        else:
            parts.append(v2)
    return " | ".join(parts)


def _download_image(page, image_url: str, save_path: Path) -> bool:
    if not images_enabled():
        return False
    try:
        headers = {
            "Referer": (page.url or "https://certified.hyundai.com/").split("#")[0],
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


def _read_dt_dd_pairs(dl_locator) -> str:
    """
    <dl><dt>주요 고지 항목</dt><dd><em>12</em>건</dd> ... </dl>
    -> "주요 고지 항목 : 12건 | ..."
    """
    try:
        dts = dl_locator.locator("dt")
        dds = dl_locator.locator("dd")
        n = min(dts.count(), dds.count())
        pairs: list[tuple[str, str]] = []
        for i in range(n):
            pairs.append((_safe_text(dts.nth(i)), _safe_text(dds.nth(i))))
        return _join_kv_pairs(pairs)
    except Exception:
        return ""


def _read_ol_tit_txt_pairs(ol_locator, *, remove_key_spaces: bool = False) -> str:
    """
    <ol><li><span class="tit">압류</span><span class="txt">없음</span></li>...</ol>
    -> "압류 : 없음 | ..."
    """
    try:
        lis = ol_locator.locator("li")
        pairs: list[tuple[str, str]] = []
        for i in range(lis.count()):
            li = lis.nth(i)
            k = _safe_text(li.locator(".tit"))
            if remove_key_spaces:
                k = re.sub(r"\s+", "", k)
            v = _safe_text(li.locator(".txt"))
            pairs.append((k, v))
        return _join_kv_pairs(pairs)
    except Exception:
        return ""


def _read_report_inner(inner_locator) -> str:
    """
    <div class="inner"><div class="box"><div class="cont"><p class="name">...</p><p class="result">...</p></div>...</div></div>
    -> "name : result | ..."
    """
    try:
        conts = inner_locator.locator(".box .cont")
        pairs: list[tuple[str, str]] = []
        for i in range(conts.count()):
            c = conts.nth(i)
            pairs.append((_safe_text(c.locator(".name")), _safe_text(c.locator(".result"))))
        return _join_kv_pairs(pairs)
    except Exception:
        return ""


def _format_installment(text: str) -> str:
    """
    "월 35만원" 형태를 최대한 맞춘다.
    """
    t = _norm_space(text)
    t = t.replace(" 만원", "만원")
    t = t.replace("km ", "km")
    t = t.replace("월", "월 ").replace("월  ", "월 ")
    return t.strip()


def _compose_guarantee(li_locator) -> str:
    """
    보증 li 하나를:
    - "1년 2개월 남음 (2027년 6월 까지) | 60,779km 남음 (100,000km 까지)"
    형태로 반환.
    """
    try:
        groups = li_locator.locator("> .group")
        if groups.count() < 2:
            return ""
        g1 = groups.nth(0)
        g2 = groups.nth(1)

        p_remain = _safe_text(g1.locator(".period").nth(0))
        p_until = _safe_text(g1.locator(".period").nth(1))
        part1 = ""
        if p_remain and p_until:
            part1 = f"{p_remain} ({p_until})"
        else:
            part1 = p_remain or p_until

        d_remain = _safe_text(g2.locator(".distance").nth(0))
        d_until = _safe_text(g2.locator(".distance").nth(1))
        part2 = ""
        if d_remain and d_until:
            part2 = f"{d_remain} ({d_until})"
        else:
            part2 = d_remain or d_until

        if part1 and part2:
            return f"{part1} | {part2}"
        return part1 or part2
    except Exception:
        return ""


def _collect_gallery_image_urls(page, root) -> list[str]:
    """
    이미지 보기 버튼 클릭 후 uspGallery에서
    - 메인 이미지(usp_main_img)
    - 리스트 아이템(.usp_list ... .item) 이미지
    URL을 모아 반환.
    """
    try:
        btn = root.locator(".pdp01_car .inner.fullTarget .btn_img").first
        if btn.count() > 0:
            try:
                btn.click(timeout=3000)
                page.wait_for_timeout(600)
            except Exception:
                pass
    except Exception:
        pass

    gallery = page.locator('[data-ref="uspGallery"]').first
    if gallery.count() == 0:
        # 레이아웃/클래스 분기 대응
        gallery = page.locator(".usp_gallery, .uspGallisOpen").first
    if gallery.count() == 0:
        return []

    seen: set[str] = set()
    urls: list[str] = []
    page_url = page.url or "https://certified.hyundai.com/"

    def _add(raw: str) -> None:
        src = (raw or "").strip()
        if not src:
            return
        if src.startswith("//"):
            src2 = "https:" + src
        elif src.startswith("/"):
            src2 = urljoin(page_url, src)
        elif not src.startswith("http"):
            src2 = urljoin(page_url, src)
        else:
            src2 = src
        if src2 not in seen:
            seen.add(src2)
            urls.append(src2)

    try:
        main_img = gallery.locator(".usp_main .usp_main_img img, .usp_main .usp_main_img").first
        if main_img.count() > 0:
            _add(_safe_attr(main_img, "src") or _safe_attr(main_img, "data-src"))
            if not urls:
                # background-image 케이스
                try:
                    bg = main_img.evaluate(
                        """el => {
                            const cs = window.getComputedStyle(el);
                            const v = cs && cs.backgroundImage ? cs.backgroundImage : '';
                            return v || '';
                        }"""
                    )
                    m = re.search(r'url\\(["\\\']?(.*?)["\\\']?\\)', str(bg or ""))
                    if m:
                        _add(m.group(1))
                except Exception:
                    pass
    except Exception:
        pass

    try:
        item_imgs = gallery.locator(
            '.usp_list [data-ref="uspGalleryItemList"] .item img, '
            '.usp_list [data-ref="uspGalleryItemList"] .item'
        )
        for i in range(item_imgs.count()):
            node = item_imgs.nth(i)
            _add(_safe_attr(node, "src") or _safe_attr(node, "data-src"))
            if len(urls) == 0:
                try:
                    bg = node.evaluate(
                        """el => {
                            const cs = window.getComputedStyle(el);
                            const v = cs && cs.backgroundImage ? cs.backgroundImage : '';
                            return v || '';
                        }"""
                    )
                    m = re.search(r'url\\(["\\\']?(.*?)["\\\']?\\)', str(bg or ""))
                    if m:
                        _add(m.group(1))
                except Exception:
                    pass
    except Exception:
        pass

    return urls


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
                    ready_selectors=("#CPOwrap,#CPOcontents,body",),
                    ready_timeout_ms=30_000,
                ),
                logger=logging.getLogger(__name__),
                attempts=1,
            )
            page.wait_for_timeout(800)
            break
        except Exception as e:
            if attempt < 2:
                logging.warning("재시도 (%d/3): %s - %s", attempt + 2, product_id, e)
                time.sleep(2)
            else:
                logging.error("접속 실패: %s - %s", product_id, e)
                return None

    root = page.locator("#CPOwrap #CPOcontents .car_detail_cont").first
    if root.count() == 0:
        root = page.locator("#CPOwrap #CPOcontents").first

    if root.count() == 0:
        return None

    try:
        # ── 가격/차량명/주행정보 ───────────────────────────────────────────
        box1 = root.locator(".car_detail_box .car_detail_box1").first
        data["car_name"] = _safe_text(box1.locator(".pdp02_price .name"))

        drive_spans = box1.locator(".drive span")
        if drive_spans.count() >= 1:
            data["year"] = _norm_space(drive_spans.nth(0).inner_text() or "")
        if drive_spans.count() >= 2:
            data["km"] = _norm_space(drive_spans.nth(1).inner_text() or "")

        data["car_pay"] = _safe_text(
            box1.locator(".price.pricestate .pricetxt .txt.pay")
        )

        installment_btn = box1.locator(
            ".price.pricestate .bottom .installment #btn_cal_estimate"
        )
        data["installment"] = _format_installment(_safe_text(installment_btn))

        # ── chart(인증/보증/비교/이력) ─────────────────────────────────────
        chart = box1.locator(".chart").first
        data["operation_period"] = _safe_text(
            chart.locator("#progress_period01 .text_box .text")
        )
        data["manufacturer_guarantee"] = _safe_text(
            chart.locator("#progress_period02 .text_box #leftWarrantyTxt")
        )
        data["inspection"] = _safe_text(
            chart.locator("#progress_compare .text_box .text")
        )
        data["accident_history"] = _safe_text(
            chart.locator("#progress_history .text_box.img02 .text")
        )

        # ── 기본 정보 (pdp03_tabs first) ──────────────────────────────────
        tabs_first = root.locator(".pdp03_tabs.first").first
        base_lis = tabs_first.locator(".cont_box2 .inner .base_01 > li")
        base_map = [
            "initial_registration",
            "mileage",
            "car_fuel",
            "engine",
            "car_ext_color",
            "car_int_color",
            "car_type",
            "drive_sys",
            "car_num",
            "model_year",
            "transmission",
        ]
        for i, col in enumerate(base_map):
            if base_lis.count() > i:
                data[col] = _safe_text(base_lis.nth(i).locator(".txt"))

        # ── history 2개 ol ────────────────────────────────────────────────
        history = root.locator(".history").first
        hist_boxes = history.locator(".cont_box2")
        if hist_boxes.count() >= 1:
            data["car_history_1"] = _read_ol_tit_txt_pairs(
                hist_boxes.nth(0).locator(".base_01").first,
                remove_key_spaces=True,
            )
        if hist_boxes.count() >= 2:
            data["car_history_2"] = _read_ol_tit_txt_pairs(
                hist_boxes.nth(1).locator(".base_01").first,
                remove_key_spaces=True,
            )

        # ── check_report(리포트 2개 + notice) ─────────────────────────────
        check_report = root.locator(".cont_box.base.check_report").first
        report_inners = check_report.locator(".list_report .inner")
        if report_inners.count() >= 1:
            data["car_report_1"] = _read_report_inner(report_inners.nth(0))
        if report_inners.count() >= 2:
            data["car_report_2"] = _read_report_inner(report_inners.nth(1))

        data["notice"] = _read_dt_dd_pairs(
            check_report.locator(".notice_list dl").first
        )

        # ── 보증 잔여 (warranty-remain-container) ─────────────────────────
        warranty_root = page.locator("#warranty-remain-container").first
        warranty_list = warranty_root.locator(
            ".pdp03_tabs.first.warranty-section.type02 .warranty-container.set_able .list > li"
        )
        if warranty_list.count() >= 1:
            data["guarantee_1"] = _compose_guarantee(warranty_list.nth(0))
        if warranty_list.count() >= 2:
            data["guarantee_2"] = _compose_guarantee(warranty_list.nth(1))

        # ── 옵션 (off 없는 li의 span) ─────────────────────────────────────
        opt_root = root.locator(".pdp03_tabs.option .cont_box.option .option_01").first
        opt_lis = opt_root.locator("li:not(.off)")
        opt_parts: list[str] = []
        seen_opt: set[str] = set()
        for i in range(opt_lis.count()):
            t = _safe_text(opt_lis.nth(i).locator("span").first) or _safe_text(opt_lis.nth(i))
            if t and t not in seen_opt:
                seen_opt.add(t)
                opt_parts.append(t)
        data["options"] = " | ".join(opt_parts)

        # ── 갤러리 이미지 (car_imgs=차량별 폴더 절대경로, {product_id}_1.png부터)
        try:
            urls = _collect_gallery_image_urls(page, root)
            for i, u in enumerate(urls, 1):
                out = detail_img_dir / f"{product_id}_{i}.png"
                _download_image(page, u, out)
        except Exception as e:
            logging.debug("[갤러리 이미지] %s : %s", product_id, e)

    except Exception as e:
        logging.error("파싱 전체 오류: %s - %s", product_id, e)
        return None

    core_cols = ("car_name", "year", "km", "car_pay")
    if sum(1 for c in core_cols if str(data.get(c) or "").strip()) == 0:
        return None

    return data
