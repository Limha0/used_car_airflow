from __future__ import annotations

import csv
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

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
#  상수 (현대차 상세 재수집)
# ═══════════════════════════════════════════════════════════════════

SOURCE_LIST_TABLE = "ods.ods_car_list_hyundaicar"
TARGET_DETAIL_TABLE = "ods.ods_car_detail_hyundaicar"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"
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
    "car_seat",
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
#  DAG 정의 (재수집)
# ═══════════════════════════════════════════════════════════════════


@dag(
    dag_id="sdag_hyundaicar_detail_retry",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "hyundaicar", "detail", "retry"],
)
def hyundaicar_detail_retry():
    """
    현대차 상세 재수집:
    - complete_yn != 'Y' (NULL 포함)
    - register_flag != 'N' (NULL 포함)
    - detail_url 존재(널/공백 제외)
    대상 행을 다시 상세 수집하여 CSV를 생성하고, 수집 성공/실패에 따라 list complete_yn을 단건 갱신한다.
    """

    @task
    def fetch_retry_targets() -> list[dict[str, str]]:
        sql = f"""
        SELECT
            l.product_id,
            l.detail_url,
            l.register_flag,
            l.complete_yn
        FROM {SOURCE_LIST_TABLE} l
        WHERE (l.complete_yn IS NULL OR TRIM(COALESCE(l.complete_yn::text, '')) <> 'Y')
          AND (l.register_flag IS NULL OR TRIM(COALESCE(l.register_flag::text, '')) <> 'N')
          AND l.detail_url IS NOT NULL
          AND TRIM(COALESCE(l.detail_url::text, '')) <> ''
        ORDER BY l.model_sn
        """
        logging.info("hyundaicar detail retry select_stmt ::: %s", sql)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        conn = hook.get_conn()
        rows: list[dict[str, str]] = []
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall() or []:
                    rows.append(dict(zip(cols, row)))
        finally:
            try:
                conn.close()
            except Exception:
                pass

        logging.info("hyundaicar detail retry 재수집 대상: %d건", len(rows))
        if not rows:
            logging.info("재수집 대상 0건 — 정상 종료로 진행합니다.")
        return rows

    @task
    def summarize_targets(target_rows: list[dict[str, str]]) -> list[dict[str, str]]:
        n = len(target_rows)
        with_url = sum(1 for r in target_rows if str(r.get("detail_url") or "").strip())
        logging.info("재수집 대상 총 건수: %d", n)
        logging.info("재수집 준비: 대상=%d건(재수집 필요), detail_url 있음 %d건", n, with_url)
        return target_rows

    @task
    def crawl_and_save_csv(target_rows: list[dict[str, str]]) -> str:
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"hyundaicar_detail_retry_{run_ts}.csv"
        logging.info("hyundaicar detail retry 출력 CSV: %s", csv_path.resolve())

        detail_base = _get_detail_img_dir()
        detail_base.mkdir(parents=True, exist_ok=True)

        total = len(target_rows)
        # 0건이든, total>0 이라도 전부 실패할 수 있으니 루프 진입 전에 헤더 CSV 를 미리 만들어둔다.
        # 그래야 수집 성공 0건이어도 후속 load 태스크가 빈 CSV 를 정상적으로 처리함.
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=DETAIL_CSV_FIELDS).writeheader()
        if total == 0:
            return str(csv_path)

        # 중간 진행 로그: 1건째 반드시 출력 + 이후 N건 간격(누적 수집 성공 건수 확인용)
        if total >= 200:
            log_every = 40
        elif total >= 100:
            log_every = 20
        elif total > 50:
            log_every = 10
        elif total > 10:
            log_every = 5
        else:
            log_every = 1

        collected = 0
        failed = 0
        skipped = 0
        recycle_every = 300
        pg_hook = PostgresHook(postgres_conn_id="car_db_conn")
        list_cols_for_complete_yn = set(
            CommonUtil.get_ods_table_columns(pg_hook, SOURCE_LIST_TABLE)
        )
        logging.info("hyundaicar detail retry 재수집 처리 시작 — 총 건수: %d", total)

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
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            for idx, row in enumerate(target_rows, 1):
                product_id = str(row.get("product_id") or "").strip()
                detail_url = str(row.get("detail_url") or "").strip()

                if not product_id or not detail_url:
                    skipped += 1
                    continue

                per_detail_dir = detail_base / product_id
                per_detail_dir.mkdir(parents=True, exist_ok=True)
                CommonUtil.clear_image_files(per_detail_dir)

                success = False
                fail_reason: str | None = None
                try:
                    detail_data = _crawl_one(page, idx, product_id, detail_url, per_detail_dir)
                    if detail_data:
                        _save_to_csv_append(csv_path, DETAIL_CSV_FIELDS, detail_data)
                        success = True
                        collected += 1
                    else:
                        failed += 1
                        fail_reason = "상세 수집 결과 없음(접속 실패·파싱 오류·필수 필드 미충족)"
                except Exception as e:
                    failed += 1
                    fail_reason = f"{type(e).__name__}: {e}"
                    logging.exception(
                        "[재수집실패] [%d/%d] product_id=%s detail_url=%s 예외 발생",
                        idx,
                        total,
                        product_id,
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
                            list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_NON_N_WITH_DETAIL_URL,
                            register_flag_a_only=False,
                            list_cols=list_cols_for_complete_yn,
                        )
                        # logging.info(
                        #     "hyundaicar detail retry update_result ::: product_id=%s complete_yn=%s updated_rows=%d",
                        #     product_id,
                        #     yn,
                        #     int(n or 0),
                        # )
                    except Exception:
                        logging.exception(
                            "[재수집] complete_yn=%s DB 갱신 실패 product_id=%s",
                            "Y" if success else "N",
                            product_id,
                        )
                    if not success and fail_reason:
                        logging.error(
                            "[재수집실패] [%d/%d] product_id=%s reason=%s → complete_yn=N",
                            idx,
                            total,
                            product_id,
                            fail_reason,
                        )

                if idx == 1 or idx % log_every == 0 or idx == total:
                    logging.info(
                        "재수집 중간 진행: %d/%d건 처리 | 누적 수집 성공 %d건 | 실패 %d | 스킵 %d",
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
                    page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                    logging.info("브라우저 컨텍스트 재생성 완료: processed=%d/%d", idx, total)

                time.sleep(0.2)

            try:
                browser.close()
            except Exception:
                pass

        if not Path(csv_path).exists():
            raise FileNotFoundError(f"CSV 생성 실패: {csv_path}")
        logging.info(
            "✅ hyundaicar detail retry 완료: collected=%d failed=%d skipped=%d total=%d csv=%s",
            collected,
            failed,
            skipped,
            total,
            csv_path,
        )
        return str(csv_path)

    @task
    def load_retry_csv_to_ods(csv_path: str) -> dict[str, Any]:
        """재수집 CSV → Detail ODS append, List complete_yn 동기화."""
        p = Path(str(csv_path or ""))
        if not p.is_file():
            raise FileNotFoundError(f"재수집 CSV 적재 대상이 없습니다: {p}")

        rows = _read_csv_rows(p)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        refresh_policy = CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_NON_N_WITH_DETAIL_URL
        if not rows:
            logging.info(
                "hyundaicar detail retry: CSV 데이터 행 없음 → Detail INSERT 생략, List complete_yn 동기화만. csv=%s",
                p,
            )
            CommonUtil.refresh_car_list_complete_flag_vs_detail_ods(
                hook,
                list_table=SOURCE_LIST_TABLE,
                detail_table=TARGET_DETAIL_TABLE,
                list_where_policy=refresh_policy,
            )
            table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
            return {
                "done": True,
                "target_table": TARGET_DETAIL_TABLE,
                "row_count": 0,
                "table_count": table_count,
                "csv_path": str(p),
                "skipped_insert": True,
            }

        CommonUtil.bulk_insert_detail_ods_rows(
            hook,
            TARGET_DETAIL_TABLE,
            rows,
            truncate=False,
            allow_only_table_cols=True,
        )
        CommonUtil.refresh_car_list_complete_flag_vs_detail_ods(
            hook,
            list_table=SOURCE_LIST_TABLE,
            detail_table=TARGET_DETAIL_TABLE,
            list_where_policy=refresh_policy,
        )
        table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
        logging.info(
            "hyundaicar detail retry CSV → ODS 적재 완료: table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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

    @task_group(group_id="prepare_retry")
    def prepare_retry():
        rows = fetch_retry_targets()
        return summarize_targets(rows)

    @task_group(group_id="retry_crawl")
    def retry_crawl(target_rows: list[dict[str, str]]):
        return crawl_and_save_csv(target_rows)

    prepared = prepare_retry()
    csv_path = retry_crawl(prepared)
    load_retry_csv_to_ods(csv_path)


dag_object = hyundaicar_detail_retry()


# ═══════════════════════════════════════════════════════════════════
#  경로/CSV 유틸
# ═══════════════════════════════════════════════════════════════════


def _get_output_dir() -> Path:
    try:
        base = Path(str(Variable.get(FINAL_FILE_PATH_VAR)).strip())
    except Exception:
        base = Path("/home/limhayoung/data/crawl")
        logging.warning(
            "Airflow Variable '%s' 조회 실패 → 기본 경로 사용: %s",
            FINAL_FILE_PATH_VAR,
            base,
        )
    return CommonUtil.build_dated_site_path(base, SITE_NAME, datetime.now())


def _get_detail_img_dir() -> Path:
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
    row = {k: _csv_cell_excel_text(v) if (isinstance(v, str) or v is None) else v for k, v in data.items()}
    with open(file_path, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        w.writerow(row)


def _read_csv_rows(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _safe_text(locator) -> str:
    try:
        return re.sub(r"\s+", " ", (locator.first.inner_text() or "")).strip()
    except Exception:
        return ""


def _download_image(page, image_url: str, save_path: Path) -> bool:
    if not image_url:
        return False
    try:
        headers = {
            "Referer": (page.url or "https://certified.hyundai.com/").split("#")[0],
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
        resp = page.request.get(image_url, timeout=30_000, headers=headers)
        if not resp or not resp.ok:
            return False
        body = resp.body()
        if not body:
            return False
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(body)
        return True
    except Exception:
        return False


def _crawl_one(page, idx: int, product_id: str, detail_url: str, detail_img_dir: Path) -> dict[str, Any] | None:
    """
    현대차 단일 상세페이지 크롤링(재수집용).
    - 원본 DAG 로직의 핵심 필드만 유지하며, 실패 시 None 반환.
    """
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
                    ready_timeout_ms=20_000,
                ),
                logger=logging.getLogger(__name__),
                attempts=1,
            )
            page.wait_for_timeout(250)
            break
        except Exception as e:
            if attempt < 2:
                logging.warning("재수집 접속 재시도 (%d/3): %s - %s", attempt + 2, product_id, e)
                time.sleep(2)
            else:
                logging.error("재수집 접속 실패: %s - %s", product_id, e)
                return None

    try:
        root = page.locator("#CPOwrap #CPOcontents .car_detail_cont").first
        if root.count() == 0:
            root = page.locator("#CPOwrap #CPOcontents").first
        if root.count() == 0:
            root = page.locator("#CPOwrap").first
        if root.count() == 0:
            root = page.locator("body").first

        # 차량명
        if root.count() > 0:
            try:
                name = _safe_text(page.locator(".car_top_tit, .detail_top_tit, h1").first)
                if name:
                    data["car_name"] = name
            except Exception:
                pass

        # ── 기본 정보 (pdp03_tabs first) : tit/txt 기반 매핑 ────────────────
        try:
            tabs_first = root.locator(".pdp03_tabs.first").first
            base_lis = tabs_first.locator(".cont_box2 .inner .base_01 > li")
            title_to_col = {
                "최초등록": "initial_registration",
                "주행거리": "mileage",
                "연료": "car_fuel",
                "배기량": "engine",
                "외관컬러": "car_ext_color",
                "내장컬러": "car_int_color",
                "차종": "car_type",
                "승차인원": "car_seat",
                "구동방식": "drive_sys",
                "차량번호": "car_num",
                "연식": "model_year",
                "변속기": "transmission",
            }
            for i in range(base_lis.count()):
                li = base_lis.nth(i)
                tit = _safe_text(li.locator(".tit"))
                txt = _safe_text(li.locator(".txt"))
                if not tit:
                    continue
                col = title_to_col.get(tit)
                if col and txt:
                    data[col] = txt
        except Exception:
            pass

        # 간단히 텍스트 기반으로 year/km 추출(fallback)
        try:
            body = page.locator("body").inner_text() or ""
            t = re.sub(r"\s+", " ", body)
            if not data.get("year"):
                m = re.search(r"(\d{4})\s*년", t)
                if m:
                    data["year"] = m.group(1) + "년"
            if not data.get("km"):
                m = re.search(r"(\d[\d,]*)\s*km", t, re.IGNORECASE)
                if m:
                    data["km"] = m.group(1) + "km"
        except Exception:
            pass

        # 이미지(있으면)
        try:
            imgs = page.locator("img")
            saved = 0
            for i in range(min(imgs.count(), 20)):
                src = (imgs.nth(i).get_attribute("data-src") or imgs.nth(i).get_attribute("src") or "").strip()
                if not src or src.startswith("data:"):
                    continue
                out = detail_img_dir / f"{product_id}_{saved+1}.png"
                if _download_image(page, src, out):
                    saved += 1
                if saved >= 5:
                    break
        except Exception:
            pass

    except Exception as e:
        logging.error("재수집 파싱 전체 오류: %s - %s", product_id, e)
        return None

    core_cols = ("car_name", "year", "km")
    filled_core = sum(1 for c in core_cols if str(data.get(c) or "").strip())
    if filled_core == 0:
        return None

    return data
