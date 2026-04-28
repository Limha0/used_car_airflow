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
#  상수 (헤이딜러 상세 재수집)
# ═══════════════════════════════════════════════════════════════════

SOURCE_LIST_TABLE = "ods.ods_car_list_heydealer"
TARGET_DETAIL_TABLE = "ods.ods_car_detail_heydealer"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"
SITE_NAME = "헤이딜러"

DETAIL_CSV_FIELDS = [
    "model_sn",
    "product_id",
    "car_name",
    "year",
    "km",
    "accident",
    "insurance",
    "guarantee",
    "refund",
    "inner_car_wash",
    "exterior_description",
    "interior_description",
    "main_option",
    "delivery_information",
    "recommendation_comment",
    "tire",
    "tinting",
    "car_key",
    "inspection_records_1",
    "inspection_records_2",
    "car_imgs",
    "date_crtr_pnttm",
    "create_dt",
]


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의 (재수집)
# ═══════════════════════════════════════════════════════════════════


@dag(
    dag_id="sdag_heydealer_detail_retry",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "heydealer", "detail", "retry"],
)
def heydealer_detail_retry():
    """
    헤이딜러 상세 재수집:
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
        logging.info("heydealer detail retry select_stmt ::: %s", sql)
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

        logging.info("heydealer detail retry 재수집 대상: %d건", len(rows))
        if not rows:
            logging.info("재수집 대상 0건 — 정상 종료로 진행합니다.")
        return rows

    @task
    def summarize_targets(target_rows: list[dict[str, str]]) -> list[dict[str, str]]:
        n = len(target_rows)
        with_url = sum(1 for r in target_rows if str(r.get("detail_url") or "").strip())
        logging.info("재수집 준비: 대상=%d건(재수집 필요), detail_url 있음 %d건", n, with_url)
        return target_rows

    @task
    def crawl_and_save_csv(target_rows: list[dict[str, str]]) -> str:
        # DAG 파싱 단계에서 playwright import로 DAGFileProcessor가 죽는 문제 방지
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"heydealer_detail_retry_{run_ts}.csv"
        logging.info("heydealer detail retry 출력 CSV: %s", csv_path.resolve())

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
        logging.info("heydealer detail retry 재수집 처리 시작 — 총 건수: %d", total)

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
                        #     "heydealer detail retry update_result ::: product_id=%s complete_yn=%s updated_rows=%d",
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
            "✅ heydealer detail retry 완료: collected=%d failed=%d skipped=%d total=%d csv=%s",
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
                "heydealer detail retry: CSV 데이터 행 없음 → Detail INSERT 생략, List complete_yn 동기화만. csv=%s",
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
            "heydealer detail retry CSV → ODS 적재 완료: table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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


dag_object = heydealer_detail_retry()


# ═══════════════════════════════════════════════════════════════════
#  경로/CSV/파싱 유틸 (헤이딜러 상세 크롤에 필요한 최소)
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


def _safe_texts_join(locator, sep: str = " | ") -> str:
    try:
        parts: list[str] = []
        for i in range(locator.count()):
            t = _norm_space(locator.nth(i).inner_text() or "")
            if t:
                parts.append(t)
        return sep.join(parts)
    except Exception:
        return ""


def _to_abs_url(page_url: str, src: str) -> str:
    raw = (src or "").strip()
    if not raw or raw.startswith("data:"):
        return ""
    if raw.startswith("//"):
        return "https:" + raw
    if raw.startswith("http"):
        return raw
    return urljoin(page_url, raw)


def _download_detail_gallery_image(page, image_url: str, save_path: Path) -> bool:
    if not image_url:
        return False
    try:
        headers = {
            "Referer": (page.url or "https://heydealer.co.kr/").split("#")[0],
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


def _collect_heydealer_detail_gallery_urls(page) -> list[str]:
    """
    헤이딜러 상세 이미지 수집:
    1) 관리 상태 섹션까지 스크롤
    2) 관리 상태 섹션의 이미지 클릭 → 갤러리 모달 열림
    3) 모달 안에서 실내/외부/하부/스크래치 탭 순회 → 각 탭의 이미지만 수집
    """
    seen: set[str] = set()
    urls: list[str] = []

    def _collect_modal_imgs():
        """갤러리 모달(css-1sglv04 포함 오버레이) 안의 img만 수집."""
        try:
            new_urls = page.evaluate(
                r"""() => {
                    const out = [];
                    // 모달/오버레이: 탭 버튼(실내/외부/하부/스크래치)이 있는 컨테이너의 부모
                    // 풀스크린 갤러리는 보통 position:fixed 오버레이
                    const allImgs = document.querySelectorAll('img');
                    for (const img of allImgs) {
                        const src = img.src || img.getAttribute('data-src') || '';
                        if (!src || src.startsWith('data:') || src.includes('svg')) continue;
                        if (!src.includes('heydealer') && !src.includes('prnd-car')) continue;
                        // 모달 안의 이미지인지 판별: fixed/absolute 위치의 부모 안에 있는지
                        let el = img;
                        let inModal = false;
                        for (let i = 0; i < 15 && el; i++) {
                            const style = window.getComputedStyle(el);
                            if (style.position === 'fixed' || style.position === 'absolute') {
                                // z-index가 높거나 전체화면급 크기
                                const rect = el.getBoundingClientRect();
                                if (rect.width > window.innerWidth * 0.5 && rect.height > window.innerHeight * 0.5) {
                                    inModal = true;
                                    break;
                                }
                            }
                            el = el.parentElement;
                        }
                        if (inModal) out.push(src);
                    }
                    return out;
                }"""
            )
            for u in (new_urls or []):
                if u and u not in seen:
                    seen.add(u)
                    urls.append(u)
        except Exception:
            pass

    def _click_tab(tab_name):
        """모달 안의 탭 버튼(실내/외부/하부/스크래치) 클릭."""
        try:
            return page.evaluate(
                r"""(name) => {
                    const buttons = document.querySelectorAll('button');
                    for (const btn of buttons) {
                        const span = btn.querySelector('span');
                        const text = span ? span.textContent.trim() : btn.textContent.trim();
                        if (text === name && btn.offsetParent !== null) {
                            btn.click();
                            return true;
                        }
                    }
                    return false;
                }""",
                tab_name,
            )
        except Exception:
            return False

    # ── 1) 관리 상태 섹션까지 스크롤 ──
    try:
        for _ in range(15):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            page.wait_for_timeout(300)
    except Exception:
        pass

    # ── 2) 관리 상태 섹션의 이미지 클릭 → 갤러리 모달 열기 ──
    modal_opened = False
    try:
        modal_opened = page.evaluate(
            r"""() => {
                // 관리 상태 근처의 이미지를 찾아 클릭
                // "관리 상태" 텍스트를 포함하는 요소 탐색
                const allEls = document.querySelectorAll('*');
                let conditionSection = null;
                for (const el of allEls) {
                    if (el.children.length === 0 && (el.textContent || '').trim() === '관리 상태') {
                        conditionSection = el;
                        break;
                    }
                }
                if (!conditionSection) return false;
                // 관리 상태 요소의 부모를 올라가며 img를 찾아 클릭
                let parent = conditionSection.parentElement;
                for (let i = 0; i < 10 && parent; i++) {
                    const imgs = parent.querySelectorAll('img');
                    for (const img of imgs) {
                        const src = img.src || '';
                        if (src && !src.includes('svg') && img.offsetParent !== null) {
                            img.click();
                            return true;
                        }
                    }
                    parent = parent.parentElement;
                }
                return false;
            }"""
        )
    except Exception:
        pass

    if not modal_opened:
        logging.info("헤이딜러 이미지 [관리상태 이미지 클릭 실패]")
        return urls

    page.wait_for_timeout(2000)
    logging.info("헤이딜러 이미지 [갤러리 모달 열림]")

    # ── 3) 실내/외부/하부/스크래치 탭 순회 → 모달 안 이미지만 수집 ──
    for tab_name in ["실내", "외부", "하부", "스크래치"]:
        try:
            before = len(urls)
            if _click_tab(tab_name):
                page.wait_for_timeout(1500)
                _collect_modal_imgs()
                logging.info(
                    "헤이딜러 이미지 [%s 탭]: +%d건 (누적 %d건)",
                    tab_name, len(urls) - before, len(urls),
                )
            else:
                logging.debug("헤이딜러 [%s] 탭 버튼 못 찾음", tab_name)
        except Exception:
            pass

    # 모달 닫기 (Escape)
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)
    except Exception:
        pass

    logging.info("헤이딜러 상세 이미지 URL 최종 수집: %d건", len(urls))
    return urls


def _parse_kv_rows(
    box,
    *,
    row_class: str,
    key_class: str,
    key_map: dict[str, str],
) -> dict[str, str]:
    out: dict[str, str] = {v: "" for v in key_map.values()}
    try:
        rows = box.locator(f".{row_class}")
        for i in range(rows.count()):
            r = rows.nth(i)
            try:
                k = r.locator(f".{key_class}").first.inner_text().strip()
            except Exception:
                continue
            if k not in key_map:
                continue
            # 값은 key_class가 아닌 첫 번째 div 텍스트로 잡는다.
            v = ""
            try:
                divs = r.locator("> div")
                for di in range(divs.count()):
                    d = divs.nth(di)
                    cls = d.get_attribute("class") or ""
                    if key_class not in cls:
                        v = _norm_space(d.inner_text() or "")
                        break
            except Exception:
                v = ""
            out[key_map[k]] = v
    except Exception:
        pass
    return out


def _apply_text_fallback(page, data: dict[str, Any]) -> None:
    try:
        body = page.locator("body").inner_text() or ""
        t = _norm_space(body)
        if not str(data.get("car_name") or "").strip():
            m = re.search(r"(\d{2,4}\s*년식)", t)
            if m:
                data["car_name"] = _norm_space((data.get("car_name") or "") + " " + m.group(1)).strip()
    except Exception:
        return


def _apply_detail_extras_fallback(page, data: dict[str, Any]) -> None:
    # 재수집용: 원본 대비 최소만 유지(추가 보완은 필요 시 확장)
    _ = (page, data)


def _crawl_one(page, idx: int, product_id: str, detail_url: str, detail_img_dir: Path) -> dict[str, Any] | None:
    """헤이딜러 단일 상세페이지 크롤링(원본 로직의 핵심만 유지)."""
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
                    ready_selectors=(".css-1uus6sd,.css-12qft46,body",),
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
        wrap = page.locator(".css-1uus6sd .css-12qft46")
        sections = wrap.first.locator(".css-ltrevz") if wrap.count() > 0 else page.locator(".css-ltrevz")

        # 섹션1: car_name + 기본 스펙
        try:
            sec1 = sections.nth(0)
            try:
                name_main = _norm_space(sec1.locator(".css-1ugrlhy").first.inner_text() or "")
                sub_spans = sec1.locator(".css-pjgjzs span")
                subs: list[str] = []
                for si in range(sub_spans.count()):
                    t = _norm_space(sub_spans.nth(si).inner_text() or "")
                    if t:
                        subs.append(t)
                data["car_name"] = " ".join(filter(None, [name_main] + subs))
            except Exception:
                pass
            data.update(
                _parse_kv_rows(
                    sec1.locator(".css-c9qil9"),
                    row_class="css-113wzqa",
                    key_class="css-1b7o1k1",
                    key_map={
                        "연식": "year",
                        "주행거리": "km",
                        "사고": "accident",
                        "자차 보험처리": "insurance",
                        "헤이딜러 보증": "guarantee",
                        "환불": "refund",
                        "실내 세차": "inner_car_wash",
                    },
                )
            )
        except Exception as e:
            logging.debug("[섹션1] %s : %s", product_id, e)

        # 섹션2: 외부/실내 설명
        try:
            sec2 = sections.nth(1)
            desc_rows = sec2.locator(".css-1i3qy3r .css-1cfq7ri .css-113wzqa")
            for di in range(desc_rows.count()):
                dr = desc_rows.nth(di)
                try:
                    label = dr.locator(".css-1b7o1k1").first.inner_text().strip()
                    val = ""
                    divs = dr.locator("> div")
                    for vi in range(divs.count()):
                        vd = divs.nth(vi)
                        cls = vd.get_attribute("class") or ""
                        if "css-1b7o1k1" not in cls:
                            val = _norm_space(vd.inner_text() or "")
                            break
                    if label == "외부":
                        data["exterior_description"] = val
                    elif label == "실내":
                        data["interior_description"] = val
                except Exception:
                    continue
        except Exception:
            pass

        # 섹션3: 옵션/배송/추천 코멘트
        try:
            sec3 = sections.nth(2)
            data["main_option"] = _safe_texts_join(sec3.locator(".css-vsdo2k .css-g5wwb2 .css-13wylk3"), " | ")

            deliv_items = sec3.locator(".css-1i3qy3r .css-1cfq7ri")
            deliv_parts: list[str] = []
            for di in range(deliv_items.count()):
                ds = deliv_items.nth(di)
                try:
                    title = _norm_space(ds.locator(".css-1njdrvn").first.inner_text() or "")
                    content = _norm_space(ds.locator(".css-1n3oo4w").first.inner_text() or "")
                    if title or content:
                        deliv_parts.append(f"{title} : {content}" if title else content)
                except Exception:
                    continue
            data["delivery_information"] = " | ".join(deliv_parts)

            data["recommendation_comment"] = _safe_texts_join(sec3.locator(".css-isc2b5 .css-yfldxx"), " | ")
        except Exception:
            pass

        # 섹션4: tire/tinting/car_key
        try:
            sec4 = sections.nth(3)
            data.update(
                _parse_kv_rows(
                    sec4.locator(".css-1i3qy3r .css-1cfq7ri"),
                    row_class="css-113wzqa",
                    key_class="css-1b7o1k1",
                    key_map={"타이어": "tire", "틴팅": "tinting", "차 키": "car_key"},
                )
            )
        except Exception:
            pass

        # inspection_records는 원본 대비 간소화(텍스트 fallback로만 보완)
        _apply_text_fallback(page, data)
        _apply_detail_extras_fallback(page, data)

        # 색상 + 관리상태(실내/외부/하부/스크래치) 이미지 저장
        try:
            gallery_urls = _collect_heydealer_detail_gallery_urls(page)
            saved = 0
            for gi, gurl in enumerate(gallery_urls, start=1):
                out = detail_img_dir / f"{product_id}_{gi}.png"
                if _download_detail_gallery_image(page, gurl, out):
                    saved += 1
            logging.info(
                "헤이딜러 이미지 저장: product_id=%s, 추출=%d건, 저장=%d건",
                product_id, len(gallery_urls), saved,
            )
        except Exception as e:
            logging.warning("헤이딜러 이미지 저장 실패 product_id=%s: %s", product_id, e)

    except Exception as e:
        logging.error("재수집 파싱 전체 오류: %s - %s", product_id, e)
        return None

    core_cols = ("car_name", "year", "km")
    filled_core = sum(1 for c in core_cols if str(data.get(c) or "").strip())
    if filled_core == 0:
        return None

    return data
