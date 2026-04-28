from __future__ import annotations

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
#  상수 (케이카 상세 재수집)
# ═══════════════════════════════════════════════════════════════════

SOURCE_LIST_TABLE = "ods.ods_car_list_kcar"
TARGET_DETAIL_TABLE = "ods.ods_car_detail_kcar"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"
SITE_NAME = "케이카"

DETAIL_CSV_FIELDS = [
    "model_sn",
    "product_id",
    "car_name",
    "car_num",
    "accident",
    "year",
    "km",
    "fuel",
    "car_color",
    "trans",
    "car_price",
    "user_name",
    "call_guide",
    "price_lists",
    "car_select_option",
    "main_option",
    "ext_panel",
    "frame",
    "diagnosis_pass",
    "notice",
    "detail_history",
    "owner_detail",
    "detail_warranty",
    "car_imgs",
    "date_crtr_pnttm",
    "create_dt",
]


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의 (재수집)
# ═══════════════════════════════════════════════════════════════════


@dag(
    dag_id="sdag_kcar_detail_retry",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "kcar", "detail", "retry"],
)
def kcar_detail_retry():
    """
    K Car 상세 재수집:
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
        logging.info("kcar detail retry select_stmt ::: %s", sql)
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

        logging.info("kcar detail retry 재수집 대상: %d건", len(rows))
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
        csv_path = output_dir / f"kcar_detail_retry_{run_ts}.csv"
        logging.info("kcar detail retry 출력 CSV: %s", csv_path.resolve())

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
        logging.info("kcar detail retry 재수집 처리 시작 — 총 건수: %d", total)

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
                        #     "kcar detail retry update_result ::: product_id=%s complete_yn=%s updated_rows=%d",
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
                    # 원본 kcar는 컨텍스트 재생성 로직이 별도 함수인데, retry는 간단히 재생성
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
                    logging.info("kcar detail retry browser context 재생성 완료: processed=%d/%d", idx, total)

                time.sleep(0.2)

            try:
                browser.close()
            except Exception:
                pass

        if not Path(csv_path).exists():
            raise FileNotFoundError(f"CSV 생성 실패: {csv_path}")
        logging.info(
            "✅ kcar detail retry 완료: collected=%d failed=%d skipped=%d total=%d csv=%s",
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
                "kcar detail retry: CSV 데이터 행 없음 → Detail INSERT 생략, List complete_yn 동기화만. csv=%s",
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
            "kcar detail retry CSV → ODS 적재 완료: table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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


dag_object = kcar_detail_retry()


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
    row = {
        k: _csv_cell_excel_text(v) if (isinstance(v, str) or v is None) else v
        for k, v in data.items()
    }
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


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _safe_text(locator) -> str:
    try:
        return _norm_space(locator.first.inner_text() or "")
    except Exception:
        return ""


def _download_image(page, image_url: str, save_path: Path) -> bool:
    if not image_url:
        return False
    try:
        headers = {
            "Referer": (page.url or "https://www.kcar.com/").split("#")[0],
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


def _to_abs_url(page_url: str, src: str) -> str:
    raw = (src or "").strip()
    if not raw or raw.startswith("data:"):
        return ""
    if raw.startswith("//"):
        return "https:" + raw
    if raw.startswith("http"):
        return raw
    return urljoin(page_url, raw)


def _crawl_one(page, idx: int, product_id: str, detail_url: str, detail_img_dir: Path) -> dict[str, Any] | None:
    """
    kcar 단일 상세페이지 크롤링(재수집용 간소화 버전).
    - 원본 DAG의 핵심 부분(상단 key area + 옵션 일부 + 이미지)을 우선 채우고, 실패 시 None.
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
                    ready_selectors=("#carContent", ".carInfoKeyArea", "body"),
                    ready_timeout_ms=25_000,
                ),
                logger=logging.getLogger(__name__),
                attempts=1,
            )
            page.wait_for_timeout(800)
            break
        except Exception as e:
            if attempt < 2:
                logging.warning("kcar detail retry 재시도 (%d/3): %s - %s", attempt + 2, product_id, e)
                time.sleep(2)
            else:
                logging.error("kcar detail retry 접속 실패: %s - %s", product_id, e)
                return None

    try:
        key_area = page.locator(".carInfoKeyArea").first
        if key_area.count() == 0:
            return None

        data["car_name"] = _safe_text(key_area.locator(".carNameWrap.drct .carName").first) or _safe_text(
            key_area.locator(".carName").first
        )
        data["car_num"] = _safe_text(key_area.locator(".carNameWrap.drct .left .carNum").first) or _safe_text(
            key_area.locator(".carNum").first
        )

        dot_lists = key_area.locator(".dotLists li")
        dot_cols = ["accident", "year", "km", "fuel", "car_color", "trans", "car_price"]
        for i, col in enumerate(dot_cols):
            if dot_lists.count() >= i + 1:
                data[col] = _norm_space(dot_lists.nth(i).inner_text() or "")

        # 옵션 텍스트 fallback (간소화)
        try:
            data["main_option"] = _norm_space(page.locator(".option-list-wrap, .option-detail-box").first.inner_text() or "")
        except Exception:
            pass

        # 이미지 저장: 썸네일 background-image / 캐러셀 img src / fallback
        try:
            raw_urls = page.evaluate(
                r"""() => {
                    const seen = new Set();
                    const urls = [];
                    // 1) 썸네일 button background-image
                    const thumbBtns = document.querySelectorAll(
                        '.el-carousel-thumbnail-item button[data-slide-index]'
                    );
                    for (const btn of thumbBtns) {
                        const bg = btn.style.backgroundImage || '';
                        const m = bg.match(/url\(["']?(.*?)["']?\)/);
                        if (m && m[1] && !seen.has(m[1])) {
                            seen.add(m[1]);
                            urls.push(m[1]);
                        }
                    }
                    // 2) 메인 캐러셀 img src
                    if (urls.length === 0) {
                        const carouselImgs = document.querySelectorAll(
                            '.el-carousel__item .image-wrap img'
                        );
                        for (const img of carouselImgs) {
                            const src = img.src || img.getAttribute('data-src') || '';
                            if (src && !src.startsWith('data:') && !seen.has(src)) {
                                seen.add(src);
                                urls.push(src);
                            }
                        }
                    }
                    // 3) fallback: img.kcar.com 도메인
                    if (urls.length === 0) {
                        document.querySelectorAll('img').forEach(img => {
                            const src = img.src || '';
                            if (src && src.includes('img.kcar.com') &&
                                !src.includes('icon') && !src.includes('logo') &&
                                !seen.has(src)) {
                                seen.add(src);
                                urls.push(src);
                            }
                        });
                    }
                    return urls;
                }"""
            )
            image_urls = [u for u in (raw_urls or []) if u]
            logging.info("kcar 이미지 URL 수집: %d건", len(image_urls))
            saved = 0
            for img_idx, u in enumerate(image_urls, 1):
                out = detail_img_dir / f"{product_id}_{img_idx}.png"
                if _download_image(page, u, out):
                    saved += 1
            logging.info(
                "kcar 이미지 저장: product_id=%s, 추출=%d건, 저장=%d건",
                product_id, len(image_urls), saved,
            )
        except Exception as e:
            logging.warning("kcar 이미지 저장 실패 product_id=%s: %s", product_id, e)

    except Exception as e:
        logging.error("kcar detail retry 파싱 전체 오류: %s - %s", product_id, e)
        return None

    core_cols = ("car_name", "year", "km")
    filled_core = sum(1 for c in core_cols if str(data.get(c) or "").strip())
    if filled_core == 0:
        return None

    return data
