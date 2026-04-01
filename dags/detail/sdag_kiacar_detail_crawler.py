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

SOURCE_LIST_TABLE = "ods.ods_car_list_kiacar"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"  # 예: /home/limhayoung/data/img
SITE_NAME = "기아차"

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


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의
# ═══════════════════════════════════════════════════════════════════


@dag(
    dag_id="sdag_kiacar_detail_crawl",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "kiacar", "detail", "crawler"],
)
def kiacar_detail_crawl():
    """기아 인증중고차 상세페이지 크롤링 DAG (register_flag != 'N' 전체)."""

    @task
    def fetch_target_urls() -> list[dict[str, str]]:
        sql = f"""
        SELECT
            product_id,
            detail_url,
            register_flag
        FROM {SOURCE_LIST_TABLE}
        WHERE (register_flag IS NULL OR TRIM(register_flag) != 'N')
          AND detail_url IS NOT NULL
          AND TRIM(detail_url) != ''
        ORDER BY model_sn
        """
        logging.info("select_target_urls_stmt ::: %s", sql)
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
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"kiacar_detail_{run_ts}.csv"
        logging.info("출력 파일: %s", csv_path)
        if not csv_path.exists():
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=DETAIL_CSV_FIELDS)
                w.writeheader()

        detail_img_dir = _get_detail_img_dir()
        detail_img_dir.mkdir(parents=True, exist_ok=True)
        CommonUtil.clear_image_files(detail_img_dir, recursive=False)
        logging.info("상세 이미지 저장 디렉터리: %s", detail_img_dir.resolve())

        total = len(target_rows)
        collected = 0
        failed = 0
        skipped = 0
        recycle_every = 200

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

    @task_group(group_id="prepare_detail_crawl")
    def prepare_detail_crawl():
        rows = fetch_target_urls()
        return summarize_targets(rows)

    @task_group(group_id="crawl_and_persist")
    def crawl_and_persist(target_rows: list[dict[str, str]]):
        return crawl_and_save_csv(target_rows)

    prepared = prepare_detail_crawl()
    crawl_and_persist(prepared)


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
    Airflow Variable used_car_image_file_path 기준
    {img_root}/YYYY년/기아차/detail
    예) /home/limhayoung/data/img/2026년/기아차/detail
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
    if not images_enabled():
        return False
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
    세 번째 탭(인덱스 2) 클릭 후 .thumb-flat.is-show 영역의 swiper-slide img URL 수집.
    """
    seen: set[str] = set()
    urls: list[str] = []
    page_url = page.url or "https://cpo.kia.com/"

    def _add(raw: str) -> None:
        src = (raw or "").strip()
        if not src or src.startswith("data:"):
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
        tab_btns = page.locator(".tabs.tabs--car-img .tabs__item")
        if tab_btns.count() >= 3:
            tab_btns.nth(2).click(timeout=8000)
            page.wait_for_timeout(600)
            try:
                page.locator(".thumb-flat.is-show").first.wait_for(state="visible", timeout=12000)
            except Exception:
                pass
    except Exception as e:
        logging.debug("[기아 갤러리 탭] 클릭 실패: %s", e)

    selectors = [
        ".thumb-flat.is-show .swiper-slide img",
        ".thumb-flat.is-show .thumb-slide .swiper-slide img",
        ".buy-car-detail__kv-area .thumb-flat.is-show img",
    ]
    for sel in selectors:
        try:
            imgs = page.locator(sel)
            for i in range(imgs.count()):
                node = imgs.nth(i)
                _add(_safe_attr(node, "src") or _safe_attr(node, "data-src"))
        except Exception:
            continue
        if urls:
            break

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
                return None

    root = page.locator("#__next .buy-car-detail").first
    if root.count() == 0:
        root = page.locator(".buy-car-detail").first
    if root.count() == 0:
        return None

    try:
        # ── 가격 영역: 라인업 / 차명 / 가격 / 할부 ───────────────────────
        price_block = root.locator(".buy-car-detail__total-price").first
        data["line_up"] = _safe_text(price_block.locator(".total-price__tags span"))
        data["car_name"] = _safe_text(price_block.locator(".total-price__tit"))
        data["car_price"] = _safe_text(price_block.locator(".purchase-price__money span"))
        data["car_installment"] = _safe_text(
            price_block.locator(".calc-payment .calc-payment__detail")
        )

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

        # ── 갤러리(세 번째 탭) 이미지 저장 ─────────────────────────────
        try:
            urls = _collect_kia_tab_gallery_urls(page)
            for i, u in enumerate(urls, 1):
                out = detail_img_dir / f"{product_id}_{i}.png"
                _download_image(page, u, out)
        except Exception as e:
            logging.debug("[기아 갤러리 이미지] %s : %s", product_id, e)

    except Exception as e:
        logging.error("파싱 전체 오류: %s - %s", product_id, e)
        return None

    core_cols = ("car_name", "car_price", "line_up")
    if sum(1 for c in core_cols if str(data.get(c) or "").strip()) == 0:
        return None

    return data
