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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from util.common_util import CommonUtil
from util.playwright_util import GotoSpec, goto_with_retry, images_enabled, install_route_blocking


# ═══════════════════════════════════════════════════════════════════
#  상수
# ═══════════════════════════════════════════════════════════════════

SOURCE_LIST_TABLE = "ods.ods_car_list_kiacar"
TARGET_DETAIL_TABLE = "ods.ods_car_detail_kiacar"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"  # 예: /home/limhayoung/data/img
SITE_NAME = "기아차"

# 상세 KV 갤러리: .buy-car-detail__kv-area … .tabs.tabs--car-img 의 3번째 탭 클릭 후
# .thumb-flat.is-show … .swiper-slide[data-swiper-slide-index] 하위(또는 중첩 .swiper-slide) img
KIACAR_DETAIL_GALLERY_TAB_ROOT = (
    "#__next .wrap #content .buy-car-detail .buy-car-detail__kv-area "
    ".buy-car-detail__tabs-slides .tabs-slides .tabs.tabs--car-img"
)
KIACAR_GALLERY_TAB_ITEMS = f"{KIACAR_DETAIL_GALLERY_TAB_ROOT} .tabs__item"
KIACAR_GALLERY_THUMB_SHOW = ".thumb-flat.is-show"

DETAIL_CSV_FIELDS = [
    "model_sn",
    "product_id",
    "line_up",
    "car_name",
    "car_price",
    "car_installment",
    "car_num",
    "car_type",
    "car_seat",
    "car_engine",
    "car_ext_color",
    "car_int_color",
    "car_record_1",
    "car_record_2",
    "car_options",
    "guarantee",
    "improvement",
    "car_imgs",
    "date_crtr_pnttm",
    "create_dt",
]


def _read_csv_rows(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의
# ═══════════════════════════════════════════════════════════════════


@dag(
    dag_id="sdag_kiacar_detail_crawl",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,    # 체인 사이클 중첩 방지
    tags=["used_car", "kiacar", "detail", "crawler"],
)
def kiacar_detail_crawl():
    """기아 상세: register_flag='A'(최신 스냅샷)만 수집 → 행마다 List complete_yn Y/N, Detail 적재·동기화. register_flag 변경은 list 동기화 전용."""

    @task
    def fetch_target_urls() -> list[dict[str, str]]:
        """
        ods.ods_car_list_kiacar 에서 register_flag=A 이고
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
                logging.info("기아 detail 수집 기준 date_crtr_pnttm(최신): %s", latest_pnttm)

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
                                "기아 상세: 최신 적재일(%s) 기준 신규(register_flag=A) 0건 → "
                                "상세 크롤 생략 후 DAG 정상 완료로 진행. "
                                "(최신일 전체 행=%s, detail_url 보유 행=%s)",
                                latest_pnttm,
                                total_l,
                                cnt_url,
                            )
                        else:
                            logging.warning(
                                "기아 상세 대상 0건: 최신일(%s)에 신규(A)=%s건 있으나 "
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
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"kiacar_detail_{run_ts}.csv"
        logging.info("출력 파일: %s", csv_path)

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

        detail_img_dir = _get_detail_img_dir()
        detail_img_dir.mkdir(parents=True, exist_ok=True)
        logging.info(
            "상세 이미지 상위 디렉터리(차량별 …/detail/{product_id}/): %s",
            detail_img_dir.resolve(),
        )

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
            install_route_blocking(context, block_resource_types=("media", "font"))
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
                        product_id,
                    )
                    continue

                if idx == 1 or idx % (10 if total <= 100 else 50) == 0 or idx == total:
                    logging.info(
                        "[%d/%d] 호출 대상 - product_id=%s, detail_url=%s",
                        idx,
                        total,
                        product_id,
                        detail_url,
                    )

                per_detail_dir = detail_img_dir / product_id
                per_detail_dir.mkdir(parents=True, exist_ok=True)
                CommonUtil.clear_image_files(per_detail_dir)

                success = False
                fail_reason: str | None = None
                try:
                    write_sn = collected + 1
                    detail_data, crawl_fail = _crawl_one(
                        page, write_sn, product_id, detail_url, per_detail_dir
                    )
                    if detail_data:
                        _save_to_csv_append(csv_path, DETAIL_CSV_FIELDS, detail_data)
                        success = True
                        collected += 1
                    else:
                        fail_reason = crawl_fail or (
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
                        product_id,
                        detail_url,
                    )
                finally:
                    yn = "Y" if success else "N"
                    try:
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
                            product_id,
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
                    install_route_blocking(context, block_resource_types=("media", "font"))
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
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV 생성 실패(경로/권한 확인): {csv_path}")
        return str(csv_path)

    @task
    def load_detail_csv_to_ods(csv_path: str) -> dict[str, Any]:
        """
        crawl_and_save_csv 결과 CSV를 ods.ods_car_detail_kiacar 로 적재.
        신규 0건이면 INSERT 생략 후 원천 List complete_yn 만 동기화(register_flag 미변경).
        """
        p = Path(str(csv_path or ""))
        if not p.is_file():
            raise FileNotFoundError(f"기아 detail 적재 대상 CSV가 없습니다: {p}")

        rows = _read_csv_rows(p)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        if not rows:
            logging.info(
                "기아 상세 DAG 정상 완료: 신규(register_flag=A) 차량 없음 → detail INSERT 생략, "
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
                "message": "신규 없음, 상세 수집 생략, complete_yn 동기화만 수행",
                "target_table": TARGET_DETAIL_TABLE,
                "row_count": 0,
                "table_count": table_count,
                "csv_path": str(p),
                "skipped_insert": True,
            }

        CommonUtil.bulk_insert_detail_ods_rows(
            hook, TARGET_DETAIL_TABLE, rows, truncate=False, allow_only_table_cols=True
        )
        CommonUtil.refresh_car_list_complete_flag_vs_detail_ods(
            hook,
            list_table=SOURCE_LIST_TABLE,
            detail_table=TARGET_DETAIL_TABLE,
            list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT,
        )
        table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
        logging.info(
            "기아 detail CSV 적재 완료: table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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
    loaded = load_detail_csv_to_ods(csv_path)

    # 정상수집 DAG가 끝난 뒤, 재수집 DAG를 자동 트리거
    trigger_retry = TriggerDagRunOperator(
        task_id="trigger_kiacar_detail_retry",
        trigger_dag_id="sdag_kiacar_detail_retry",
        wait_for_completion=False,
        reset_dag_run=False,
    )

    loaded >> trigger_retry


dag_object = kiacar_detail_crawl()


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
    예) /home/limhayoung/data/crawl/2026년/기아차/20260331
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
    Airflow Variable used_car_image_file_path 기준 상세 상위 폴더.
    실제 파일은 {img_root}/YYYY년/기아차/detail/{product_id}/{product_id}_N.png
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


def _read_car_record_list(record_root) -> str:
    """car-record__item: label + profile-desc -> 파이프 조인."""
    try:
        items = record_root.locator(".car-record__item")
        pairs: list[tuple[str, str]] = []
        for i in range(items.count()):
            li = items.nth(i)
            k = _safe_text(li.locator(".car-record__label"))
            v = _safe_text(li.locator(".car-record__profile-desc"))
            pairs.append((k, v))
        return _join_kv_pairs(pairs)
    except Exception:
        return ""


def _format_improvement_h4(text: str) -> str:
    """'외관 및 내장 4건' -> '외관 및 내장 : 4건'"""
    t = _norm_space(text)
    m = re.match(r"^(.+?)\s*(\d+)\s*건\s*$", t)
    if m:
        return f"{m.group(1).strip()} : {m.group(2)}건"
    return t


def _download_image(page, image_url: str, save_path: Path) -> bool:
    try:
        headers = {
            "Referer": (page.url or "https://cpo.kia.com/").split("#")[0],
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


def _collect_kia_tab_gallery_urls(page) -> list[str]:
    """
    기아 CPO 상세 이미지 수집:
    1) '외장 360°' 탭 → data-image-list-x 에서 첫 이미지 1장
    2) '내장 360°' 탭 → data-image-list-x 에서 첫 이미지 1장
    3) '차량 사진' 탭 클릭 → swiper/thumb 이미지 전부
    """
    urls: list[str] = []
    seen: set[str] = set()

    def _add(src: str) -> None:
        u = (src or "").strip()
        if u and u not in seen:
            seen.add(u)
            urls.append(u)

    # ── 1) 외장/내장 360° : data-image-list-x 에서 첫 장만 ──
    try:
        tab_btns = page.locator(".tabs.tabs--car-img .tabs__item")
        n_tabs = tab_btns.count()

        for tab_idx in range(min(n_tabs, 2)):  # 0=외장360, 1=내장360
            try:
                tab_btns.nth(tab_idx).click(timeout=5000)
                page.wait_for_timeout(800)
                first_url = page.evaluate(
                    r"""() => {
                        const el = document.querySelector('[data-image-list-x]');
                        if (!el) return '';
                        try {
                            const list = JSON.parse(el.getAttribute('data-image-list-x') || '[]');
                            return list.length > 0 ? list[0] : '';
                        } catch(e) { return ''; }
                    }"""
                )
                if first_url:
                    _add(first_url)
                    tab_label = "외장360" if tab_idx == 0 else "내장360"
                    logging.info("기아 이미지 [%s]: 첫 장 수집 (누적 %d건)", tab_label, len(urls))
            except Exception:
                pass
    except Exception as e:
        logging.debug("[기아 360 탭] 수집 실패: %s", e)

    # ── 2) '차량 사진' 탭 클릭 → 전체 이미지 수집 ──
    try:
        tab_btns = page.locator(".tabs.tabs--car-img .tabs__item")
        n_tabs = tab_btns.count()
        if n_tabs >= 3:
            tab_btns.nth(2).click(timeout=8000)
            page.wait_for_timeout(1500)

            photo_urls = page.evaluate(
                r"""() => {
                    const seen = new Set();
                    const urls = [];

                    // swiper-slide img (thumb-flat 갤러리)
                    const slides = document.querySelectorAll(
                        '.thumb-flat.is-show .swiper-slide:not(.swiper-slide-duplicate) img'
                    );
                    for (const img of slides) {
                        const src = img.src || img.getAttribute('data-src') || '';
                        if (src && !src.startsWith('data:') && !seen.has(src)) {
                            seen.add(src);
                            urls.push(src);
                        }
                    }

                    // fallback: cpo-cdn 도메인 img 전체
                    if (urls.length === 0) {
                        document.querySelectorAll('img').forEach(img => {
                            const src = img.src || '';
                            if (src && src.includes('cpo-cdn.kia.com') && !seen.has(src)) {
                                seen.add(src);
                                urls.push(src);
                            }
                        });
                    }

                    return urls;
                }"""
            )
            for u in (photo_urls or []):
                _add(u)
            logging.info("기아 이미지 [차량 사진]: +%d건 (누적 %d건)", len(photo_urls or []), len(urls))
    except Exception as e:
        logging.debug("[기아 차량사진 탭] 수집 실패: %s", e)

    logging.info("기아 상세 이미지 URL 최종 수집: %d건", len(urls))
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
) -> tuple[dict[str, Any] | None, str]:
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
                    ready_selectors=("#__next .buy-car-detail,body",),
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
                return None, f"페이지_로드_3회_실패:{e!s}"[:500]

    root = page.locator("#__next .buy-car-detail").first
    if root.count() == 0:
        root = page.locator(".buy-car-detail").first
    if root.count() == 0:
        logging.warning(
            "상세 DOM 없음: product_id=%s (.buy-car-detail 미표시·판매종료·리다이렉트 가능)",
            product_id,
        )
        return None, "DOM_없음_.buy-car-detail_미표시_또는_빈페이지"

    try:
        # ── 가격 영역: 라인업 / 차명 / 가격 / 할부 ───────────────────────
        price_block = root.locator(".buy-car-detail__total-price").first
        data["line_up"] = _safe_text(price_block.locator(".total-price__tags span"))
        data["car_name"] = _safe_text(price_block.locator(".total-price__tit"))
        data["car_price"] = _safe_text(price_block.locator(".purchase-price__money span"))
        # car_installment: "월 36만원 / 48개월" 형태로 추출
        # 동적 css-* 클래스 대응 → 텍스트 패턴 기반 JS 탐색
        _installment = _safe_text(
            price_block.locator(".calc-payment .calc-payment__detail")
        )
        if not _installment:
            try:
                _installment = page.evaluate(r"""
                    () => {
                        const norm = s => (s || '').replace(/\s+/g, ' ').trim();

                        // 전략 1: css-* 요소에서 "월 N만원" + "N개월" 조합 추출
                        const allEls = document.querySelectorAll('[class*="css-"]');
                        for (const el of allEls) {
                            if (el.offsetParent === null) continue;
                            const full = norm(el.textContent);
                            const m = full.match(/월\s*[\d,]+\s*만?\s*원/);
                            const p = full.match(/\d+\s*개월/);
                            if (m && p) {
                                if (el.querySelectorAll('[class*="css-"]').length <= 5) {
                                    return m[0].replace(/\s+/g, ' ').trim()
                                        + ' / '
                                        + p[0].replace(/\s+/g, ' ').trim();
                                }
                            }
                        }

                        // 전략 2: buy-car-detail 영역 내 leaf 요소 수집 후 조합
                        const priceArea = document.querySelector('.buy-car-detail__total-price')
                                       || document.querySelector('.buy-car-detail');
                        if (priceArea) {
                            let monthly = '';
                            let period = '';
                            const leaves = priceArea.querySelectorAll('span, p, div');
                            for (const el of leaves) {
                                if (el.children.length > 0) continue;
                                const t = norm(el.textContent);
                                if (!monthly && /월\s*[\d,]+\s*만?\s*원/.test(t)) monthly = t;
                                if (!period && /^\d+\s*개월$/.test(t)) period = t;
                            }
                            if (monthly) return period ? (monthly + ' / ' + period) : monthly;
                        }

                        // 전략 3: 페이지 전체에서 "월 N원" leaf 요소
                        const body = document.querySelectorAll('span, p, div');
                        for (const el of body) {
                            if (el.offsetParent === null || el.children.length > 0) continue;
                            const t = norm(el.textContent);
                            if (/^월\s*[\d,]+\s*만?\s*원/.test(t)) return t;
                        }
                        return '';
                    }
                """) or ""
            except Exception:
                _installment = ""
        data["car_installment"] = _installment

        # ── car-spec (6 li) ───────────────────────────────────────────
        spec_items = root.locator(
            ".buy-car-detail__car-spec .car-spec__list--info .car-spec__item"
        )
        spec_keys = [
            "car_num",
            "car_type",
            "car_seat",
            "car_engine",
            "car_ext_color",
            "car_int_color",
        ]
        for i, col in enumerate(spec_keys):
            if spec_items.count() <= i:
                break
            li = spec_items.nth(i)
            if i < 4:
                data[col] = _safe_text(li.locator(".car-spec__txt"))
            else:
                data[col] = _safe_text(li.locator(".car-spec__color-label"))

        # ── car-record (2블록) ─────────────────────────────────────────
        record_roots = root.locator(
            ".buy-car-detail__certified .certified--car-record .certified__car-record .car-record"
        )
        if record_roots.count() >= 1:
            data["car_record_1"] = _read_car_record_list(record_roots.nth(0))
        if record_roots.count() >= 2:
            data["car_record_2"] = _read_car_record_list(record_roots.nth(1))

        # ── 옵션 ────────────────────────────────────────────────────────
        opt_descs = root.locator(".buy-car-detail__car-option .car-option__item .car-option__desc")
        opt_parts: list[str] = []
        seen_opt: set[str] = set()
        for i in range(opt_descs.count()):
            t = _safe_text(opt_descs.nth(i))
            if t and t not in seen_opt:
                seen_opt.add(t)
                opt_parts.append(t)
        data["car_options"] = " | ".join(opt_parts)

        # ── 보증 (car-warranty) ─────────────────────────────────────────
        w_items = root.locator(".certified__car-warranty .car-warranty__item")
        w_pairs: list[tuple[str, str]] = []
        for i in range(w_items.count()):
            wi = w_items.nth(i)
            label = _safe_text(wi.locator(".car-warranty__label"))
            bold = _safe_text(wi.locator(".car-warranty__txt-bold"))
            w_pairs.append((label, bold))
        data["guarantee"] = _join_kv_pairs(w_pairs)

        # ── 개선(점검) 건수: benefits 영역 spec-tit ─────────────────────
        imp_specs = root.locator(
            ".buy-car-detail__benefits .car-spec__group-spec01 .car-spec__spec"
        )
        imp_parts: list[str] = []
        for i in range(imp_specs.count()):
            h4 = imp_specs.nth(i).locator("h4.car-spec__spec-tit").first
            if h4.count() == 0:
                h4 = imp_specs.nth(i).locator(".car-spec__spec-tit").first
            if h4.count() > 0:
                imp_parts.append(_format_improvement_h4(_safe_text(h4)))
        data["improvement"] = " | ".join([p for p in imp_parts if p])

        # ── 외장360(1장) + 내장360(1장) + 차량사진(전부) 이미지 저장 ──
        try:
            img_urls = _collect_kia_tab_gallery_urls(page)
            saved = 0
            for i, u in enumerate(img_urls, 1):
                out = detail_img_dir / f"{product_id}_{i}.png"
                if _download_image(page, u, out):
                    saved += 1
            logging.info(
                "기아 이미지 저장: product_id=%s, 추출=%d건, 저장=%d건",
                product_id, len(img_urls), saved,
            )
        except Exception as e:
            logging.warning("기아 이미지 저장 실패 product_id=%s: %s", product_id, e)

    except Exception as e:
        logging.error("파싱 전체 오류: %s - %s", product_id, e)
        return None, f"파싱_예외:{e!s}"[:500]

    core_cols = ("car_name", "car_price", "line_up")
    if sum(1 for c in core_cols if str(data.get(c) or "").strip()) == 0:
        logging.warning(
            "핵심 필드 없음: product_id=%s (car_name·car_price·line_up 모두 비어 있음)",
            product_id,
        )
        return None, "핵심필드_없음_car_name_car_price_line_up_모두_빈값"

    return data, ""
