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
from airflow.decorators import dag, task
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

SOURCE_LIST_TABLE = "ods.ods_car_list_lotterentacar"

FINAL_FILE_PATH_VAR = "used_car_final_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"
SITE_NAME = "롯데렌터카"

DETAIL_CSV_FIELDS = [
    "model_sn",
    "product_id",
    "detail_url",
    "car_name",
    "new_car_price",
    "opt_wrap",
    "prc_wrap",
    "car_num",
    "km",
    "car_type",
    "engine",
    "year",
    "fuel",
    "car_ext_color",
    "car_int_color",
    "transmission",
    "car_seat",
    "drive_sys",
    "detail_options",
    "options_explain",
    "maintenance",
    "engine_oil",
    "tire",
    "battery",
    "indoor_cleaner",
    "car_tire_check_info",
    "car_tire_left_front",
    "car_tire_right_front",
    "car_tire_left_back",
    "car_tire_right_back",
    "ext_panel",
    "main_frame",
    "car_insurance",
    "seller_name",
    "seller_tel",
    "car_imgs",
    "date_crtr_pnttm",
    "create_dt",
]

# 상세 DOM 루트(요청한 구조)
LOTTE_DETAIL_ROOT_SEL = (
    ".body-ctn-car-detail #wrap #container #reactMainPage "
    ".layout--container.ctn-car-detail .search-detail-wrapper "
    ".search-detail-container .detail-element"
)


# ═══════════════════════════════════════════════════════════════════
#  경로 유틸
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
    """
    Airflow Variable used_car_image_file_path 기준 상세 상위 폴더.
    실제 파일은 {img_root}/YYYY년/롯데렌터카/detail/{product_id}/{product_id}_N.png
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
    try:
        with open(file_path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if not file_exists:
                w.writeheader()
            w.writerow(row)
    except PermissionError as e:
        logging.error(
            "CSV 쓰기 Permission denied: %s (디렉터리 소유자·퍼미션 확인, Airflow worker 유저에게 쓰기 권한 필요)",
            file_path,
        )
        raise PermissionError(
            f"CSV 경로에 쓸 수 없습니다: {file_path}. "
            f"/mnt/d/data/crawl 등은 Airflow 실행 유저에게 chown/chmod 로 쓰기 권한을 주세요."
        ) from e


def _resolve_writable_csv_path(preferred: Path) -> Path:
    """
    preferred 의 부모 디렉터리에 쓰기 가능한지 probe 파일로 검사.
    불가하면 /tmp 하위로 폴백(로그에 원 경로 안내).
    """
    preferred.parent.mkdir(parents=True, exist_ok=True)
    probe = preferred.parent / f".airflow_write_probe_{time.time_ns()}"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return preferred
    except PermissionError:
        try:
            probe.unlink(missing_ok=True)
        except OSError:
            pass
        fb_dir = Path("/tmp") / "lotterentacar_detail_csv"
        fb_dir.mkdir(parents=True, exist_ok=True)
        fallback = fb_dir / preferred.name
        logging.error(
            "CSV 원 경로 쓰기 불가(Permission denied) → 폴백 저장: %s (원래: %s). "
            "서버에서 원 경로에 Airflow worker 유저 쓰기 권한을 부여하세요.",
            fallback.resolve(),
            preferred.resolve(),
        )
        return fallback


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


def _safe_attr(locator, name: str) -> str:
    try:
        return (locator.first.get_attribute(name) or "").strip()
    except Exception:
        return ""


def _extract_price_only(s: str) -> str:
    """
    '신차가 3,754만원' → '3,754만원'
    """
    t = _norm_space(s)
    m = re.search(r"(\d[\d,]*\s*만원)", t)
    return _norm_space(m.group(1)) if m else t


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
    if not image_url:
        return False
    try:
        headers = {
            "Referer": (page.url or "https://www.lotterentacar.net/").split("#")[0],
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


def _collect_lotte_detail_image_urls(page, root) -> list[str]:
    """
    좌측 이미지 리스트:
    .section-left ... .car-image-list ul.image-list li[data-link-category="차량상세"] 내부 이미지 src 수집
    """
    page_url = page.url or "https://www.lotterentacar.net/"
    seen: set[str] = set()
    urls: list[str] = []

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

    selectors = [
        ".section-left .car-image-list ul.image-list li[data-link-category] img",
        ".car-image-list ul.image-list li[data-link-category] img",
        "ul.image-list li[data-link-category] img",
    ]
    for sel in selectors:
        try:
            imgs = root.locator(sel)
            if imgs.count() == 0:
                imgs = page.locator(sel)
            for i in range(imgs.count()):
                img = imgs.nth(i)
                _add(_safe_attr(img, "data-src") or _safe_attr(img, "src"))
        except Exception:
            pass
        if urls:
            break
    return urls


def _parse_right_main_info(root) -> dict[str, str]:
    """
    우측 상단 main info: car_name / new_car_price / opt_wrap / prc_wrap
    """
    out: dict[str, str] = {"car_name": "", "new_car_price": "", "opt_wrap": "", "prc_wrap": ""}

    right = root.locator(".section-right").first
    wrap = right.locator(".wrap").first if right.count() > 0 else root.locator(".wrap").first
    main = wrap.locator(".car-main-info-wrap").first if wrap.count() > 0 else root.locator(".car-main-info-wrap").first
    left = main.locator(".left").first if main.count() > 0 else root.locator(".car-main-info-wrap .left").first

    out["car_name"] = _safe_text(left.locator(".car-model").first) or _safe_text(root.locator(".car-model").first)

    # 기본: .car-price .wrap .car → "신차가 3,754만원" 중 가격만
    new_car_raw = ""
    sale_price = root.locator(".car-price.sale-price").first
    if sale_price.count() > 0:
        # 차선책: sale-price 가 있으면 prc_wrap은 sale-price 하위 prc-wrap.prc 사용,
        # new_car_price 는 discount-rate .car에서 가격만
        out["prc_wrap"] = _safe_text(sale_price.locator(".prc-wrap .prc").first) or _safe_text(
            root.locator(".prc-wrap .prc").first
        )
        new_car_raw = _safe_text(root.locator(".discount-rate .car").first) or _safe_text(
            sale_price.locator(".discount-rate .car").first
        )
    else:
        new_car_raw = _safe_text(root.locator(".car-price .wrap .car").first)
        out["prc_wrap"] = _safe_text(root.locator(".prc-wrap .prc").first)

    out["new_car_price"] = _extract_price_only(new_car_raw)
    out["opt_wrap"] = _safe_text(root.locator(".opt-wrap").first)

    return out


def _parse_car_options_kv(root) -> dict[str, str]:
    """
    car-detail-info > car-options ul li span.t / span.c 매핑
    """
    mapping = {
        "차량번호": "car_num",
        "주행거리": "km",
        "차종": "car_type",
        "배기량": "engine",
        "연식": "year",
        "연료": "fuel",
        "외장색상": "car_ext_color",
        "내장색상": "car_int_color",
        "변속기": "transmission",
        "인승": "car_seat",
        "구동방식": "drive_sys",
    }
    out = {v: "" for v in mapping.values()}
    lis = root.locator(".car-detail-info .car-options ul li")
    for i in range(lis.count()):
        li = lis.nth(i)
        k = _safe_text(li.locator("span.t").first)
        v = _safe_text(li.locator("span.c").first)
        if k in mapping and v:
            out[mapping[k]] = v
    return out


def _parse_detail_option_min(root) -> str:
    """
    `.car-options-detail .options #detailOptionMin li` 중 class 토큰에 `on`이 있는 항목만
    (예: `opt-rearcamera on`) — `p` 텍스트를 ` | ` 로 조인. 옵션 전체보기 버튼 행은 제외.
    """
    ul = root.locator(".car-options-detail.area .options #detailOptionMin").first
    if ul.count() == 0:
        ul = root.locator("#detailOptionMin").first
    if ul.count() == 0:
        return ""
    lis = ul.locator("li")
    parts: list[str] = []
    for i in range(lis.count()):
        li = lis.nth(i)
        try:
            if li.locator("button.bt-option-all").count() > 0:
                continue
        except Exception:
            pass
        cls = (li.get_attribute("class") or "").strip()
        tokens = cls.split()
        if "on" not in tokens:
            continue
        p = li.locator("p").first
        t = _safe_text(p)
        if t:
            parts.append(_norm_space(t.replace("\n", " ").replace("\r", " ")))
    return " | ".join(parts)


def _parse_options_explain(root) -> str:
    """
    .options_explain ul li: p + span → "p (span)" , 다건은 | 조인
    """
    sec = root.locator(".options_explain").first
    if sec.count() == 0:
        return ""
    lis = sec.locator("ul li")
    parts: list[str] = []
    for i in range(lis.count()):
        li = lis.nth(i)
        p = _safe_text(li.locator("p").first)
        s = _safe_text(li.locator("span").first)
        if p and s:
            parts.append(f"{p} ({s})")
        elif p:
            parts.append(p)
        elif s:
            parts.append(s)
    return " | ".join([_norm_space(x) for x in parts if _norm_space(x)])


def _parse_checkup_boxes(root) -> dict[str, str]:
    """
    .checkup-box li: .t-label 라벨별로 .t-val 값을 매핑
    """
    mapping = {
        "정기관리": "maintenance",
        "엔진오일": "engine_oil",
        "타이어": "tire",
        "배터리": "battery",
        "실내클리너": "indoor_cleaner",
    }
    out = {v: "" for v in mapping.values()}
    lis = root.locator(".checkup-box-wrap .checkup-box li")
    for i in range(lis.count()):
        li = lis.nth(i)
        k = _safe_text(li.locator(".t-label").first)
        v = _safe_text(li.locator(".t-val").first)
        if k in mapping and v:
            out[mapping[k]] = v
    return out


def _parse_tire_check(root) -> dict[str, str]:
    out: dict[str, str] = {
        "car_tire_check_info": "",
        "car_tire_left_front": "",
        "car_tire_right_front": "",
        "car_tire_left_back": "",
        "car_tire_right_back": "",
    }

    top_lis = root.locator(".car-tire-check-wrap .car-tire-check-info.top ul li")
    pairs: list[tuple[str, str]] = []
    for i in range(top_lis.count()):
        li = top_lis.nth(i)
        spans = li.locator("span")
        if spans.count() >= 2:
            pairs.append((_safe_text(spans.nth(0)), _safe_text(spans.nth(1))))
    out["car_tire_check_info"] = _join_kv_pairs(pairs)

    img = root.locator(".car-tire-check-img").first
    if img.count() == 0:
        return out

    out["car_tire_left_front"] = _safe_text(
        img.locator(".car-tire-item-group").nth(0).locator(".car-tire-item.left.front .car-tire-parts").first
    )
    out["car_tire_right_front"] = _safe_text(
        img.locator(".car-tire-item-group").nth(0).locator(".car-tire-item.right.front .car-tire-parts").first
    )
    out["car_tire_left_back"] = _safe_text(
        img.locator(".car-tire-item-group").nth(1).locator(".car-tire-item.left.back .car-tire-parts").first
    )
    out["car_tire_right_back"] = _safe_text(
        img.locator(".car-tire-item-group").nth(1).locator(".car-tire-item.right.back .car-tire-parts").first
    )
    return out


def _parse_carcheck_table(root) -> dict[str, str]:
    """
    #carCheckTable .carcheck-table-item(1,2) → ext_panel / main_frame
    """
    out = {"ext_panel": "", "main_frame": ""}
    items = root.locator("#carCheckTable .carcheck-table-item")
    for idx, key in ((0, "ext_panel"), (1, "main_frame")):
        if items.count() <= idx:
            continue
        it = items.nth(idx)
        lis = it.locator(".carcheck-info ul li")
        pairs: list[tuple[str, str]] = []
        for i in range(lis.count()):
            li = lis.nth(i)
            label = _norm_space(_safe_text(li.locator("span").first))
            cnt = _safe_text(li.locator("p b").first)
            if label and cnt:
                pairs.append((label, f"{cnt}건"))
            elif label:
                pairs.append((label, ""))
        out[key] = _join_kv_pairs(pairs)
    return out


def _parse_insurance(root) -> str:
    lis = root.locator(".car-insurance.area .isr-box ul li")
    pairs: list[tuple[str, str]] = []
    for i in range(lis.count()):
        li = lis.nth(i)
        k = _safe_text(li.locator(".t-label").first)
        v = _safe_text(li.locator(".t-val").first)
        if k or v:
            pairs.append((k, v))
    return _join_kv_pairs(pairs)


def _parse_seller(root) -> tuple[str, str]:
    sec = root.locator(".car-seller.area").first
    if sec.count() == 0:
        return "", ""
    name_p = sec.locator(".seller-profile .info .name").first
    strong = _safe_text(name_p.locator("strong").first)
    shop = _safe_text(name_p.locator("span.shop").first)
    seller_name = _norm_space(" ".join([strong, shop]).strip())
    seller_tel = _safe_text(sec.locator(".bt-tel").first)
    return seller_name, seller_tel


def _crawl_one(page, idx: int, product_id: str, detail_url: str, detail_img_dir: Path) -> tuple[dict[str, Any] | None, str]:
    d_pnttm, c_dt = _get_now_times()
    data: dict[str, Any] = {f: "" for f in DETAIL_CSV_FIELDS}
    data["model_sn"] = idx
    data["product_id"] = product_id
    data["detail_url"] = detail_url
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
                    ready_selectors=(LOTTE_DETAIL_ROOT_SEL, "body"),
                    ready_timeout_ms=30_000,
                ),
                logger=logging.getLogger(__name__),
                attempts=1,
            )
            page.wait_for_timeout(900)
            break
        except Exception as e:
            if attempt < 2:
                logging.warning("롯데카 상세 재시도(%d/3): product_id=%s err=%s", attempt + 2, product_id, e)
                time.sleep(2)
            else:
                return None, f"페이지_로드_실패:{e!s}"[:500]

    root = page.locator(LOTTE_DETAIL_ROOT_SEL).first
    if root.count() == 0:
        root = page.locator(".detail-element").first
    if root.count() == 0:
        return None, "DOM_없음_detail-element"

    try:
        main = _parse_right_main_info(root)
        data.update(main)

        data.update(_parse_car_options_kv(root))
        data["detail_options"] = _parse_detail_option_min(root)
        data["options_explain"] = _parse_options_explain(root)

        data.update(_parse_checkup_boxes(root))
        data.update(_parse_tire_check(root))
        data.update(_parse_carcheck_table(root))
        data["car_insurance"] = _parse_insurance(root)
        seller_name, seller_tel = _parse_seller(root)
        data["seller_name"] = seller_name
        data["seller_tel"] = seller_tel

        # 상세 이미지 저장 (좌측 리스트)
        try:
            urls = _collect_lotte_detail_image_urls(page, root)
            for i, u in enumerate(urls, 1):
                out = detail_img_dir / f"{product_id}_{i}.png"
                _download_image(page, u, out)
        except Exception as e:
            logging.debug("롯데카 상세 이미지 저장 실패 product_id=%s: %s", product_id, e)

    except Exception as e:
        return None, f"파싱_예외:{e!s}"[:500]

    # 최소 방어: car_name만이라도 있어야 성공 처리
    if not str(data.get("car_name") or "").strip():
        return None, "필수필드_없음_car_name"
    return data, ""


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의 (1회성)
# ═══════════════════════════════════════════════════════════════════
@dag(
    dag_id="sdag_lotterentacar_detail_only1",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "lotterentacar", "detail", "only1"],
)
def lotterentacar_detail_only1():
    """롯데카 상세페이지 1회성 크롤링 DAG (register_flag != 'N' 전체, complete_yn 단건 갱신)."""

    @task
    def fetch_target_urls() -> list[dict[str, str]]:
        sql = f"""
        SELECT
            product_id,
            detail_url,
            register_flag
        FROM {SOURCE_LIST_TABLE}
        WHERE (register_flag IS NULL OR TRIM(register_flag) <> 'N')
          AND detail_url IS NOT NULL
          AND TRIM(detail_url) <> ''
        ORDER BY model_sn
        """
        logging.info("lotterentacar detail select_target_urls_stmt ::: %s", sql)
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
        logging.info("lotterentacar detail 수집 대상: %d건", len(rows))
        if not rows:
            raise ValueError("수집 대상 URL이 없습니다. 테이블/조건(register_flag != 'N', detail_url) 확인 필요.")
        return rows

    @task
    def crawl_and_save_csv(target_rows: list[dict[str, str]]) -> str:
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = _resolve_writable_csv_path(output_dir / f"lotterentacar_detail_{run_ts}.csv")
        logging.info("lotterentacar detail 출력 CSV: %s", csv_path.resolve())

        detail_img_root = _get_detail_img_dir()
        detail_img_root.mkdir(parents=True, exist_ok=True)
        logging.info(
            "lotterentacar detail 이미지 상위 디렉터리(차량별 …/detail/{product_id}/): %s",
            detail_img_root.resolve(),
        )

        pg_hook = PostgresHook(postgres_conn_id="car_db_conn")
        policy = CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_NON_N_WITH_DETAIL_URL

        total = len(target_rows)
        collected = 0
        failed = 0
        skipped = 0
        logging.info(
            "롯데렌터카 상세 수집 시작: DB 조회 대상 전체 %d건 (이 중 CSV 행으로 적재되면 성공 건수로 집계)",
            total,
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
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )
            # 이미지 다운로드는 page.request로 하므로, 페이지 리소스는 기본 차단(속도)
            install_route_blocking(ctx)
            page = ctx.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            for idx, row in enumerate(target_rows, 1):
                product_id = str(row.get("product_id") or "").strip()
                detail_url = str(row.get("detail_url") or "").strip()

                if not product_id or not detail_url:
                    skipped += 1
                    continue

                per_dir = detail_img_root / product_id
                per_dir.mkdir(parents=True, exist_ok=True)

                success = False
                fail_reason = ""
                try:
                    detail_data, fail_reason = _crawl_one(page, idx, product_id, detail_url, per_dir)
                    if detail_data:
                        _save_to_csv_append(csv_path, DETAIL_CSV_FIELDS, detail_data)
                        collected += 1
                        success = True
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    fail_reason = f"예외:{e!s}"[:500]
                    logging.exception("롯데카 상세 수집 예외: product_id=%s detail_url=%s", product_id, detail_url)
                finally:
                    yn = "Y" if success else "N"
                    try:
                        CommonUtil.update_list_complete_yn_for_product_id(
                            pg_hook,
                            list_table=SOURCE_LIST_TABLE,
                            product_id=product_id,
                            value=yn,
                            list_where_policy=policy,
                            register_flag_a_only=False,
                        )
                    except Exception:
                        logging.exception(
                            "롯데카 complete_yn 갱신 실패: product_id=%s value=%s",
                            product_id,
                            yn,
                        )
                    if not success:
                        logging.error(
                            "[상세수집실패] [%d/%d] product_id=%s reason=%s → complete_yn=N",
                            idx,
                            total,
                            product_id,
                            fail_reason or "상세 수집 결과 없음",
                        )

                if idx % 50 == 0 or idx == total:
                    logging.info(
                        "롯데렌터카 상세 진행 [%d/%d] 성공=%d 실패=%d 스킵=%d",
                        idx,
                        total,
                        collected,
                        failed,
                        skipped,
                    )
                time.sleep(0.15)

            try:
                browser.close()
            except Exception:
                pass

        if not csv_path.exists():
            raise FileNotFoundError(f"lotterentacar detail CSV 생성 실패: {csv_path}")

        logging.info(
            "롯데렌터카 상세 수집 ✅ 완료 — 전체 %d건 중 CSV 적재 성공 %d건, 실패 %d건, 스킵(product_id/detail_url 없음) %d건 | CSV=%s",
            total,
            collected,
            failed,
            skipped,
            csv_path.resolve(),
        )
        return str(csv_path)

    crawl_and_save_csv(fetch_target_urls())


dag_object = lotterentacar_detail_only1()

