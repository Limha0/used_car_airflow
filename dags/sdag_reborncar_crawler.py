import csv
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Airflow Variable (heydealer와 동일)
USED_CAR_SITE_NAMES_VAR = "used_car_site_names"
CRAWL_BASE_PATH_VAR = "crawl_base_path"

# DB `std.tn_data_bsc_info`: 리본카 제공사이트 코드 (실제 DB 값과 맞출 것)
REBORNCAR_PVSN_SITE_CD = "ps00007"
# 브랜드 / 차종 / 목록 메타 행의 datst_cd 
REBORNCAR_DATST_BRAND = "data19"
REBORNCAR_DATST_CAR_TYPE = "data20"
REBORNCAR_DATST_LIST = "data21"


def _get_crawl_base_path() -> Path:
    try:
        v = Variable.get(CRAWL_BASE_PATH_VAR, default_var=None)
        if v:
            return Path(str(v).strip())
    except Exception:
        pass
    return Path(Variable.get("HEYDEALER_BASE_PATH", "/home/limhayoung/data"))


def get_site_name_by_datst(datst_cd: str) -> str:
    """Variable USED_CAR_SITE_NAMES(JSON)에서 datst_cd에 해당하는 한글 폴더명 조회."""
    key = (datst_cd or "").lower().strip()
    mapping: dict[str, Any] = {}
    try:
        raw = Variable.get(
            USED_CAR_SITE_NAMES_VAR,
            default_var="{}",
            deserialize_json=True,
        )
        if isinstance(raw, dict):
            mapping = {str(k).lower(): v for k, v in raw.items()}
    except Exception:
        mapping = {}
    name = mapping.get(key)
    if name is not None and str(name).strip():
        return str(name).strip()
    return "리본카"


@dataclass(frozen=True)
class BscInfo:
    pvsn_site_cd: str
    datst_cd: str
    dtst_nm: str
    link_url: str
    clct_yn: str
    link_yn: str
    incr_yn: str
    clct_mthd: str
    target_table: str | None = None

    @staticmethod
    def from_row(row: dict[str, Any]) -> "BscInfo":
        def _s(k: str) -> str:
            v = row.get(k)
            return "" if v is None else str(v)

        return BscInfo(
            pvsn_site_cd=_s("pvsn_site_cd"),
            datst_cd=_s("datst_cd"),
            dtst_nm=_s("dtst_nm"),
            link_url=_s("link_url") or _s("link_data_clct_url"),
            clct_yn=_s("clct_yn"),
            link_yn=_s("link_yn"),
            incr_yn=_s("incr_yn"),
            clct_mthd=_s("clct_mthd"),
            target_table=(row.get("target_table") or row.get("trgt_tbl_nm") or row.get("target_tbl"))
            and str(row.get("target_table") or row.get("trgt_tbl_nm") or row.get("target_tbl")),
        )


def activate_paths_for_datst(datst_cd: str) -> None:
    """datst_cd 기준 crawl / log / img 루트 경로 설정 (각 Task 시작 시 호출)."""
    global RESULT_DIR, LOG_DIR, IMG_BASE, YEAR_STR, DATE_STR, RUN_TS

    base = _get_crawl_base_path()
    site = get_site_name_by_datst(datst_cd)
    now = datetime.now()
    YEAR_STR = now.strftime("%Y년")
    DATE_STR = now.strftime("%Y%m%d")
    RUN_TS = now.strftime("%Y%m%d%H%M")

    RESULT_DIR = base / "crawl" / YEAR_STR / site / DATE_STR
    LOG_DIR = base / "log" / YEAR_STR / site / DATE_STR
    IMG_BASE = base / "img" / YEAR_STR / site / DATE_STR


# 경로는 activate_paths_for_datst()에서 설정 (DAG import 시 기본값)
YEAR_STR = ""
DATE_STR = ""
RUN_TS = ""
RESULT_DIR = Path("/tmp")
LOG_DIR = Path("/tmp")
IMG_BASE = Path("/tmp")

try:
    activate_paths_for_datst(REBORNCAR_DATST_LIST)
except Exception:
    pass

# 수집 제어
HEADLESS_MODE = True

def get_logger():
    """heydealer와 동일하게 Airflow 기본 로깅(포맷/핸들러)을 사용."""
    return logging.getLogger("reborncar")


def _get_file_logger(run_ts: str) -> logging.Logger:
    """
    Airflow 태스크 로그(UI)와 별개로, LOG_DIR에도 파일 로그를 남기기 위한 로거.
    - 파일: LOG_DIR/reborncar_crawler_{run_ts}.log
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("reborncar_crawler")
    logger.setLevel(logging.INFO)
    log_path = str(LOG_DIR / f"reborncar_crawler_{run_ts}.log")
    # 동일 run_ts에 대해 핸들러 중복 추가 방지
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == log_path:
            return logger
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    return logger

def _normalize_composite_key(*parts):
    """공백 정리 후 한 문자열로 합침 (brand의 model_list+model_list_1+model_list_2, list의 lp_car_name+lp_car_trim 비교용)."""
    return " ".join((p or "").strip() for p in parts).strip()


def load_brand_model_map(result_dir, brand_path: Path | None = None):
    """reborncar_brand_list.csv 또는 reborncar_brand.csv에서
    1) model_list(| 앞부분) -> brand_list, car_list 매핑
    2) (model_list + model_list_1 + model_list_2) full 키 -> (brand_list, car_list, model_list, model_list_1, model_list_2) 5-tuple
    3) (model_list + model_list_1) 짧은 키 -> 동일 5-tuple
    4) model_list 단독 -> 5-tuple (car_name에 model_list 포함 시 보완 매칭, 예: 더 뉴봉고Ⅲ화물)
    을 로드."""
    model_to_brand = {}
    model_to_car_list = {}
    composite_to_model = {}  # full key -> (brand_list, car_list, model_list, model_list_1, model_list_2)
    composite_short_to_model = {}  # short key -> 5-tuple
    model_list_to_row = {}  # model_list -> 5-tuple
    if brand_path is None:
        brand_path = result_dir / "reborncar_brand_list.csv"
        if not brand_path.exists():
            brand_path = result_dir / "reborncar_brand.csv"
    if not brand_path.exists():
        return model_to_brand, model_to_car_list, composite_to_model, composite_short_to_model, model_list_to_row
    try:
        with open(brand_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                model_list_raw = (row.get("model_list") or "").strip()
                brand_list = (row.get("brand_list") or "").strip()
                car_list = (row.get("car_list") or "").strip()
                model_list_val = model_list_raw.split("|")[0].strip() if model_list_raw else ""
                model_list_1 = (row.get("model_list_1") or "").strip()
                model_list_2 = (row.get("model_list_2") or "").strip()
                if not brand_list:
                    continue
                if model_list_val and model_list_val not in model_to_brand:
                    model_to_brand[model_list_val] = brand_list
                    model_to_car_list[model_list_val] = car_list if car_list else "-"
                row_5 = (brand_list or "-", car_list or "-", model_list_val or "-", model_list_1 or "-", model_list_2 or "-")
                comp_key = _normalize_composite_key(model_list_val, model_list_1, model_list_2)
                if comp_key and comp_key not in composite_to_model:
                    composite_to_model[comp_key] = row_5
                short_key = _normalize_composite_key(model_list_val, model_list_1)
                if short_key and short_key not in composite_short_to_model:
                    composite_short_to_model[short_key] = row_5
                if model_list_val and model_list_val not in model_list_to_row:
                    model_list_to_row[model_list_val] = row_5
    except Exception:
        pass
    return model_to_brand, model_to_car_list, composite_to_model, composite_short_to_model, model_list_to_row

def _get_model_key_for_lp_car_name(lp_car_name, model_keys):
    """lp_car_name으로 model_keys 중 매칭되는 키 반환. 없으면 None."""
    name = (lp_car_name or "").strip()
    if not name or not model_keys:
        return None
    if name in model_keys:
        return name
    parts = name.rsplit(maxsplit=1)
    if len(parts) >= 2:
        last_part = parts[-1].strip()
        if last_part in model_keys:
            return last_part
    return None

def get_brand_for_lp_car_name(lp_car_name, model_to_brand):
    """lp_car_name과 brand의 model_list(| 앞) 매칭. 실패 시 lp_car_name 뒤에서 띄어쓰기 기준 마지막 부분으로 재매칭."""
    key = _get_model_key_for_lp_car_name(lp_car_name, model_to_brand)
    return model_to_brand[key] if key else "-"

def get_car_list_for_lp_car_name(lp_car_name, model_to_car_list):
    """lp_car_name으로 brand의 model_list(| 앞) 매칭 후 해당 car_list 반환."""
    key = _get_model_key_for_lp_car_name(lp_car_name, model_to_car_list)
    return model_to_car_list.get(key, "-") if key else "-"


def get_brand_car_model_for_list_row(lp_car_name, lp_car_trim, composite_to_model, composite_short_to_model=None, model_list_to_row=None):
    """list의 (lp_car_name + lp_car_trim) = car_name으로 brand 행 매칭 후 (brand_list, car_list, model_list, model_list_1, model_list_2) 5-tuple 반환.
    1) full 키 / 짧은 키 composite 매칭 (브랜드 첫 단어 제거한 car_name도 시도)
    2) 실패 시 car_name에 model_list가 포함된 행으로 보완 (예: '더 뉴봉고Ⅲ화물 1.2톤 LPG ...' -> model_list '더 뉴봉고Ⅲ화물')
    """
    if composite_short_to_model is None:
        composite_short_to_model = {}
    if model_list_to_row is None:
        model_list_to_row = {}
    name = (lp_car_name or "").strip()
    trim = (lp_car_trim or "").strip()
    car_name_full = _normalize_composite_key(name, trim)
    parts = name.split(None, 1)
    car_name_no_first = _normalize_composite_key(parts[1] if len(parts) >= 2 else "", trim)

    for key in (car_name_full, car_name_no_first):
        if not key:
            continue
        if composite_to_model and key in composite_to_model:
            return composite_to_model[key]
        if composite_short_to_model and key in composite_short_to_model:
            return composite_short_to_model[key]

    # 보완: car_name이 brand의 model_list로 시작하는 행 사용 (가장 긴 model_list 우선, 예: 더 뉴봉고Ⅲ화물)
    for model_list_key in sorted(model_list_to_row.keys(), key=len, reverse=True):
        if not model_list_key:
            continue
        if car_name_full.startswith(model_list_key) or car_name_no_first.startswith(model_list_key):
            return model_list_to_row[model_list_key]

    return "-", "-", "-", "-", "-"

def split_boname_by_last_paren(text):
    """뒤에서부터 첫 번째 ()를 기준으로 나누어 '앞부분|(괄호내용)' 형태로 반환 (crawl_reborncar_brand.py와 동일)."""
    if not text or "(" not in text:
        return text
    last_open = text.rfind("(")
    prefix = text[:last_open].strip()
    suffix = text[last_open:].strip()
    if not prefix:
        return text
    return f"{prefix}|{suffix}"


def split_model_list_and_period(combined):
    """'앞부분|(괄호내용)' 형태에서 (model_list, production_period) 반환 (crawl_reborncar_brand.py와 동일)."""
    if not combined or "|" not in combined:
        return combined.strip(), ""
    parts = combined.split("|", 1)
    return parts[0].strip(), (parts[1].strip() if len(parts) > 1 else "")


def normalize_production_period(text):
    """'(21~24년)' → '21~24', '(23년~현재)' → '23~현재' 형태로 변환."""
    if not text:
        return ""
    s = text.strip()
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1].strip()
    s = s.replace("년", "")
    return s


def run_reborncar_brand(page, result_dir, logger, csv_path: Path | None = None, pnttm: str | None = None, create_dt: str | None = None):
    """브랜드·차종·모델·트림·옵션 계층 수집 → reborncar_brand_list.csv (crawl_reborncar_brand.py와 동일 방식)."""
    if pnttm is None:
        pnttm = DATE_STR
    if create_dt is None:
        create_dt = datetime.now().strftime("%Y%m%d%H%M")
    if csv_path is None:
        csv_path = result_dir / "reborncar_brand_list.csv"
    headers = ["model_sn", "brand_list", "car_list", "model_list", "model_list_1", "model_list_2", "production_period", "date_crtr_pnttm", "create_dt"]
    model_sn = 1
    row_count = 0
    try:
        logger.info("리본카 브랜드 계층 수집 시작...")
        page.goto("https://www.reborncar.co.kr/smartbuy/SB1001.rb", wait_until="networkidle", timeout=60000)
        page.wait_for_selector(".filter-brand .brand-list", timeout=30000)
        page.wait_for_timeout(1500)
        brand_selectors = page.locator(".filter-brand .brand-list")
        brand_count = brand_selectors.count()

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            for i in range(brand_count):
                brand_box = brand_selectors.nth(i)
                brand_list = brand_box.locator(".brand-name label span").inner_text().strip()
                logger.info(f"[{brand_list}] 처리 중...")
                brand_box.locator(".brand-name label").click()
                page.wait_for_timeout(400)

                car_items = brand_box.locator(".car-list .check-box[class*='car-']")
                car_count = car_items.count()

                for j in range(car_count):
                    car_box = car_items.nth(j)
                    car_list = car_box.locator("label span").first.inner_text().strip()
                    car_box.locator("label").first.click()
                    page.wait_for_timeout(300)

                    # 모델만 선택 (트림/옵션 check-box 제외: class에 model- 포함된 것만)
                    detail_boxes = car_box.locator(".model-list .check-box[class*='model-']")
                    detail_count = detail_boxes.count()

                    if detail_count > 0:
                        for k in range(detail_count):
                            model_box = detail_boxes.nth(k)
                            full_boname = model_box.locator("> label span").inner_text().strip()
                            model_list_val, production_period_val = split_model_list_and_period(
                                split_boname_by_last_paren(full_boname)
                            )
                            production_period_val = normalize_production_period(production_period_val)
                            model_box.locator("label").first.click()
                            page.wait_for_timeout(200)

                            trim_boxes = model_box.locator(".trim-list.depth04 .check-box[class*='trim-']")
                            trim_count = trim_boxes.count()

                            # 트림이 전혀 없는 모델(예: 더 뉴레이, 레이(11~17년))도 1행으로 수집
                            if trim_count == 0:
                                row = {
                                    "model_sn": model_sn,
                                    "brand_list": brand_list,
                                    "car_list": car_list,
                                    "model_list": model_list_val,
                                    "model_list_1": "",
                                    "model_list_2": "",
                                    "production_period": production_period_val,
                                    "date_crtr_pnttm": pnttm,
                                    "create_dt": create_dt
                                }
                                writer.writerow(row)
                                row_count += 1
                                model_sn += 1
                                continue

                            for t in range(trim_count):
                                trim_el = trim_boxes.nth(t)
                                try:
                                    trim_name = trim_el.locator("label span").inner_text().strip()
                                except Exception:
                                    trim_name = ""
                                trim_el.locator("label").click()
                                page.wait_for_timeout(250)
                                # 현재 트림 요소 안에서만 옵션 조회 (트렌디/프레스티지/노블레스 등 해당 트림 옵션만 수집)
                                option_boxes = trim_el.locator(".option-list.depth05 .check-box[class*='option-']")
                                opt_count = option_boxes.count()
                                if opt_count > 0:
                                    for o in range(opt_count):
                                        try:
                                            option_name = option_boxes.nth(o).locator("label span").inner_text().strip()
                                        except Exception:
                                            option_name = ""
                                        row = {
                                            "model_sn": model_sn,
                                            "brand_list": brand_list,
                                            "car_list": car_list,
                                            "model_list": model_list_val,
                                            "model_list_1": trim_name,
                                            "model_list_2": option_name,
                                            "production_period": production_period_val,
                                            "date_crtr_pnttm": pnttm,
                                            "create_dt": create_dt
                                        }
                                        writer.writerow(row)
                                        row_count += 1
                                        model_sn += 1
                                else:
                                    row = {
                                        "model_sn": model_sn,
                                        "brand_list": brand_list,
                                        "car_list": car_list,
                                        "model_list": model_list_val,
                                        "model_list_1": trim_name,
                                        "model_list_2": "",
                                        "production_period": production_period_val,
                                        "date_crtr_pnttm": pnttm,
                                        "create_dt": create_dt
                                    }
                                    writer.writerow(row)
                                    row_count += 1
                                    model_sn += 1
                    else:
                        car_model_list, car_production_period = split_model_list_and_period(
                            split_boname_by_last_paren(car_list)
                        )
                        car_production_period = normalize_production_period(car_production_period)
                        row = {
                            "model_sn": model_sn,
                            "brand_list": brand_list,
                            "car_list": car_list,
                            "model_list": car_model_list,
                            "model_list_1": "",
                            "model_list_2": "",
                            "production_period": car_production_period,
                            "date_crtr_pnttm": pnttm,
                            "create_dt": create_dt
                        }
                        writer.writerow(row)
                        row_count += 1
                        model_sn += 1

        if row_count > 0:
            logger.info(f"브랜드 CSV 저장 완료: {csv_path} ({row_count}행)")
        else:
            logger.warning("브랜드 수집 데이터 없음.")
    except Exception as e:
        logger.error(f"브랜드 수집 오류: {e}")

def run_reborncar_car_type(page, result_dir, logger):
    """차종(car_type) 수집 → reborncar_car_type_list.csv (cate_cb 제거, 날짜 컬럼 추가)."""
    result_path = result_dir / "reborncar_car_type_list.csv"
    return run_reborncar_car_type_to_path(page, result_path, logger)


def run_reborncar_car_type_to_path(page, result_path: Path, logger, pnttm: str | None = None, create_dt: str | None = None):
    """차종(car_type) 수집 → 지정 path로 저장."""
    if result_path.exists():
        result_path.unlink()
    if pnttm is None:
        pnttm = DATE_STR
    if create_dt is None:
        create_dt = datetime.now().strftime("%Y%m%d%H%M")
    # cate_cb 제거, 날짜 컬럼 추가
    headers = ["car_type_sn", "car_type_name", "date_crtr_pnttm", "create_dt"]
    car_type_sn = 1
    try:
        logger.info("리본카 차종(car_type) 수집 시작...")
        page.goto("https://www.reborncar.co.kr/smartbuy/SB1001.rb", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_selector("input.cate-cb[id^='car_type']", state="attached", timeout=30000)
        page.wait_for_timeout(1000)
        car_type_elements = page.locator("input.cate-cb[id^='car_type']").all()
        for el in car_type_elements:
            el_id = el.get_attribute("id")
            label_span = page.locator(f"label[for='{el_id}'] span")
            if label_span.count() > 0:
                car_type_name = label_span.inner_text().strip()
                row = {
                    "car_type_sn": car_type_sn,
                    "car_type_name": car_type_name,
                    "date_crtr_pnttm": pnttm,
                    "create_dt": create_dt,
                }
                with open(result_path, "a", newline="", encoding="utf-8-sig") as f:
                    w = csv.DictWriter(f, fieldnames=headers)
                    if car_type_sn == 1:
                        w.writeheader()
                    w.writerow(row)
                car_type_sn += 1
        if car_type_sn > 1:
            logger.info(f"차종 CSV 저장 완료: {result_path} ({car_type_sn - 1}건)")
        else:
            logger.warning("차종 데이터 없음.")
    except Exception as e:
        logger.error(f"차종 수집 오류: {e}")

def save_list_thumbnail(item_locator, page, product_id, save_dir, list_page_url, logger):
    """목록 아이템의 .lp-thumnail > img 를 product_id_list.png 로 저장. 저장 경로(imgs부터) 반환."""
    if not product_id or not save_dir:
        return ""
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    base_url = list_page_url.rsplit("?", 1)[0] if "?" in list_page_url else list_page_url
    img_el = item_locator.locator(".lp-thumnail img").first
    if img_el.count() == 0:
        return ""
    src = img_el.get_attribute("src")
    if not src:
        return ""
    try:
        full_url = urljoin(base_url, src) if not (src.startswith("http") or src.startswith("//")) else ("https:" + src if src.startswith("//") else src)
        resp = page.request.get(full_url)
        if resp.ok:
            path = save_dir / f"{product_id}_list.png"
            path.write_bytes(resp.body())
            return str(path)
    except Exception as e:
        logger.warning(f"리스트 썸네일 저장 실패 ({product_id}_list): {e}")
    return ""


def save_detail_images(page, product_id, save_dir, detail_url, logger):
    """상세 페이지 vip-visual 영역 이미지를 product_id_1.png, product_id_2.png ... 로 저장."""
    if not product_id or not save_dir:
        return
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    base_url = detail_url.rsplit("?", 1)[0] if "?" in detail_url else detail_url
    urls = []
    # 1) #wrap .vip-section .vip-visual .vip-visual-detail .visual-detail .detail-img 내 이미지
    detail_imgs = page.locator("#wrap .vip-section .vip-visual .vip-visual-detail .visual-detail .detail-img img")
    for i in range(detail_imgs.count()):
        src = detail_imgs.nth(i).get_attribute("src")
        if src:
            urls.append(src)
    if not urls:
        single = page.locator("#wrap .vip-section .vip-visual .vip-visual-detail .visual-detail img.detail-img").first
        if single.count() > 0:
            src = single.get_attribute("src")
            if src:
                urls.append(src)
    # 2) .vip-visual-list .visual-box .visual-con 내 이미지
    list_imgs = page.locator("#wrap .vip-section .vip-visual .vip-visual-list .visual-box .visual-con img")
    for i in range(list_imgs.count()):
        src = list_imgs.nth(i).get_attribute("src")
        if src:
            urls.append(src)
    saved_count = 0
    for idx, src in enumerate(urls, start=1):
        try:
            full_url = urljoin(base_url, src) if not (src.startswith("http") or src.startswith("//")) else ("https:" + src if src.startswith("//") else src)
            resp = page.request.get(full_url)
            if resp.ok:
                path = save_dir / f"{product_id}_{idx}.png"
                path.write_bytes(resp.body())
                # logger.info(f"이미지 저장: {path}")
                saved_count += 1
        except Exception as e:
            logger.warning(f"이미지 저장 실패 ({product_id}_{idx}): {e}")
    if saved_count > 0:
        logger.info(f"{product_id} 이미지 수집 완료")

def _open_reborncar_list_page(page, list_url: str, logger) -> None:
    """
    스마트바이 목록(SB1001) 로드. 리본카 사이트는 LCP/스와이퍼 등으로
    `visible` 판정이 늦거나 DOM만 먼저 붙는 경우가 있어 attached 우선 대기 + 재시도.
    """
    list_sel = "ul.lp-box.smartbuy-lp"
    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            page.goto(
                list_url,
                wait_until="domcontentloaded",
                timeout=120_000,
            )
            try:
                page.wait_for_load_state("load", timeout=45_000)
            except Exception:
                pass
            page.wait_for_timeout(800)
            # visible 대신 attached: 스와이퍼/레이아웃으로 보이기 전 타임아웃 방지
            page.wait_for_selector(list_sel, state="attached", timeout=90_000)
            page.wait_for_timeout(1200)
            return
        except Exception as e:
            last_err = e
            logger.warning(f"목록 페이지 로드 재시도 ({attempt}/3): {e}")
            page.wait_for_timeout(2000 * attempt)
    raise RuntimeError(f"목록 페이지 로드 실패 ({list_url}): {last_err}") from last_err


def fetch_detail_images_only(page, product_id, img_save_dir, logger):
    """상세 페이지 접속 후 vip-visual 이미지만 저장 (detail CSV 없음)."""
    detail_url = f"https://www.reborncar.co.kr/smartbuy/SB1002.rb?productId={product_id}"
    try:
        page.goto(detail_url, wait_until="domcontentloaded")
        page.wait_for_selector(".vip-section .vip-visual", state="visible", timeout=10000)
        save_detail_images(page, product_id, img_save_dir, detail_url, logger)
    except Exception as e:
        logger.warning(f"이미지 저장 스킵 ({product_id}): {e}")


def _run_reborncar_brand_csv(datst_cd: str) -> str:
    from playwright.sync_api import sync_playwright

    activate_paths_for_datst((datst_cd or REBORNCAR_DATST_BRAND).lower() or REBORNCAR_DATST_BRAND)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    brand_csv = RESULT_DIR / f"reborncar_brand_list_{run_ts}.csv"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS_MODE)
        context = browser.new_context(user_agent="Mozilla/5.0...", viewport={"width": 1900, "height": 1000})
        page = context.new_page()
        try:
            run_reborncar_brand(
                page,
                RESULT_DIR,
                logger,
                csv_path=brand_csv,
                pnttm=DATE_STR,
                create_dt=run_ts,
            )
        finally:
            browser.close()
    return str(brand_csv)


def _run_reborncar_car_type_csv(datst_cd: str) -> str:
    from playwright.sync_api import sync_playwright

    activate_paths_for_datst((datst_cd or REBORNCAR_DATST_CAR_TYPE).lower() or REBORNCAR_DATST_CAR_TYPE)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    car_type_csv = RESULT_DIR / f"reborncar_car_type_list_{run_ts}.csv"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS_MODE)
        context = browser.new_context(user_agent="Mozilla/5.0...", viewport={"width": 1900, "height": 1000})
        page = context.new_page()
        try:
            run_reborncar_car_type_to_path(
                page,
                car_type_csv,
                logger,
                pnttm=DATE_STR,
                create_dt=run_ts,
            )
        finally:
            browser.close()
    return str(car_type_csv)


def run_reborncar_list_job(
    bsc: BscInfo,
    *,
    brand_list_csv_path: str,
    car_type_csv_path: str | None = None,
) -> dict[str, Any]:
    """
    DB 메타 1건(목록 datst) 기준 리본카 목록·이미지 수집.
    브랜드 CSV는 앞단 Task에서 생성한 절대경로를 사용한다 (RUN_TS 불일치 방지).
    """
    from playwright.sync_api import sync_playwright

    activate_paths_for_datst((bsc.datst_cd or REBORNCAR_DATST_LIST).lower() or REBORNCAR_DATST_LIST)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (IMG_BASE / "list").mkdir(parents=True, exist_ok=True)
    (IMG_BASE / "detail").mkdir(parents=True, exist_ok=True)

    logger = _get_file_logger(run_ts)
    brand_path = Path(brand_list_csv_path)
    if not brand_path.is_file():
        raise FileNotFoundError(
            "브랜드 CSV가 없습니다. run_brand_csv 태스크를 먼저 성공시키고 brand_list_csv_path를 확인하세요: "
            f"{brand_path}"
        )
    if car_type_csv_path and not Path(car_type_csv_path).is_file():
        logger.warning(f"차종 CSV 경로 없음(무시하고 진행): {car_type_csv_path}")

    logger.info("🏁 리본카 목록 수집 시작")
    logger.info(f"- pvsn_site_cd={bsc.pvsn_site_cd}, datst_cd={bsc.datst_cd}, dtst_nm={bsc.dtst_nm}")
    logger.info(f"- link_url={bsc.link_url}")
    logger.info(f"- 브랜드 CSV(이전 태스크): {brand_path}")

    pnttm, create_dt_full = DATE_STR, run_ts

    brand_model_map, model_to_car_list, composite_to_model, composite_short_to_model, model_list_to_row = load_brand_model_map(
        RESULT_DIR, brand_path=brand_path
    )

    list_headers = [
        "model_sn", "product_id", "car_type_name", "brand_list", "car_list", "model_list", "model_list_1", "model_list_2", "car_name",
        "release_dt", "car_navi", "car_seat",
        "car_main_pay", "amtsel", "status", "copytext", "endtimedeal", "detail_url", "car_imgs", "date_crtr_pnttm", "create_dt",
    ]

    list_path = RESULT_DIR / f"reborncar_list_{run_ts}.csv"
    if list_path.exists():
        list_path.unlink()

    list_img_save_dir = IMG_BASE / "list"
    detail_img_save_dir = IMG_BASE / "detail"

    TEST_PAGE_LIMIT = None
    list_url = (bsc.link_url or "").strip() or "https://www.reborncar.co.kr/smartbuy/SB1001.rb"
    car_counter = 1

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS_MODE)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
        )
        page = context.new_page()
        detail_page = context.new_page()
        for pg in (page, detail_page):
            pg.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
        try:
            logger.info("리본카 목록 페이지 접속...")
            _open_reborncar_list_page(page, list_url, logger)

            car_type_filter_box = page.locator("#wrap .lp-section .lp-filter .lp-filter-form .lp-filter-box .lp-filter-con .check-btn-box.car-type-filter")
            car_type_buttons = car_type_filter_box.locator(".check-btn.check-btn-s.filter-chk") if car_type_filter_box.count() > 0 else page.locator(".check-btn-box.car-type-filter .check-btn.check-btn-s.filter-chk")
            n_car_types = car_type_buttons.count()
            if n_car_types == 0:
                n_car_types = 1
                car_type_labels = [""]
            else:
                car_type_labels = []
                for i in range(n_car_types):
                    try:
                        lbl = car_type_buttons.nth(i).inner_text().strip()
                        car_type_labels.append(lbl or f"타입{i+1}")
                    except Exception:
                        car_type_labels.append(f"타입{i+1}")

            for car_type_idx in range(n_car_types):
                current_car_type = car_type_labels[car_type_idx]
                if n_car_types > 1:
                    try:
                        car_type_buttons.nth(car_type_idx).click()
                        page.wait_for_timeout(2500)
                        page.wait_for_selector('.lp-filter-list .lp-filter-choice span[data-cls="cate-cb"]', timeout=8000)
                        page.wait_for_timeout(800)
                        logger.info(f"차종 필터 선택 (칩 생성): {current_car_type}")
                    except Exception as e:
                        logger.warning(f"차종 필터 클릭 실패 ({current_car_type}): {e}")
                        continue

                while True:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(3000)

                    active_el = page.locator("li.pagination-con.page-num.active")
                    try:
                        current_page = int(active_el.inner_text() or "1")
                    except Exception:
                        current_page = 1

                    items = page.locator("ul.lp-box.smartbuy-lp > li.lp-con.swiper-slide:not(.lp-banner):not(.swiper-slide-duplicate)").all()
                    logger.info(f"[{current_car_type}] 현재 {current_page}페이지 수집 중... (매물 {len(items)}개)")

                    for item in items:
                        try:
                            v_product_id = ""
                            href_value = item.locator("a.lp-thumnail").get_attribute("href")
                            if href_value:
                                match = re.search(r"fnDetailMove\('([^']+)'", href_value)
                                if match:
                                    v_product_id = match.group(1)

                            v_status = item.locator(".lp-status").inner_text().strip() or "판매중"

                            v_finamt, v_amtsel = "-", "-"
                            if v_status in ["판매중", "계약중", "상담중"]:
                                pay_b = item.locator(".car-pay .pay b")
                                if pay_b.count() > 0:
                                    v_finamt = pay_b.inner_text().strip() + "만원"
                                discount_el = item.locator(".car-pay .discount")
                                v_amtsel = discount_el.inner_text().strip() if discount_el.count() > 0 else "0"
                            elif v_status == "준비중":
                                v_finamt, v_amtsel = "0만원", "-"
                            elif v_status == "판매완료":
                                v_finamt, v_amtsel = "판매완료", "-"

                            summary_lis = item.locator(".lp-summery li").all()
                            v_year = summary_lis[0].inner_text().strip() if len(summary_lis) > 0 else ""
                            v_navi = summary_lis[1].inner_text().strip() if len(summary_lis) > 1 else ""
                            v_seat = summary_lis[2].inner_text().strip() if len(summary_lis) > 2 else ""

                            is_td = item.locator(".lp-timedeal").count() > 0
                            v_copy = "타임딜" if is_td else ""
                            v_endtd = item.locator(".lp-timedeal-count").inner_text().strip() if is_td else ""

                            v_lp_car_name = item.locator(".lp-car-name").inner_text().strip()
                            v_lp_car_trim = item.locator(".lp-car-trim").inner_text().strip()
                            v_brand, v_car, v_model_list, v_model_list_1, v_model_list_2 = get_brand_car_model_for_list_row(
                                v_lp_car_name, v_lp_car_trim, composite_to_model, composite_short_to_model, model_list_to_row
                            )
                            if v_brand == "-" and v_car == "-":
                                v_brand = get_brand_for_lp_car_name(v_lp_car_name, brand_model_map)
                                v_car = get_car_list_for_lp_car_name(v_lp_car_name, model_to_car_list)

                            detail_url_val = f"https://www.reborncar.co.kr/smartbuy/SB1002.rb?productId={v_product_id}" if v_product_id else ""
                            list_page_url = page.url

                            car_imgs_val = ""
                            if v_product_id:
                                img_el = item.locator(".lp-thumnail img").first
                                if img_el.count() > 0:
                                    src = img_el.get_attribute("src")
                                    if src:
                                        base_url = list_page_url.rsplit("?", 1)[0] if "?" in list_page_url else list_page_url
                                        full_url = urljoin(base_url, src) if not (src.startswith("http") or src.startswith("//")) else ("https:" + src if src.startswith("//") else src)
                                        resp = page.request.get(full_url)
                                        if resp.ok:
                                            path = list_img_save_dir / f"{v_product_id}_list.png"
                                            path.write_bytes(resp.body())
                                            car_imgs_val = str(path)

                            car_name_val = _normalize_composite_key(v_lp_car_name, v_lp_car_trim)
                            list_row = {
                                "model_sn": car_counter,
                                "product_id": v_product_id,
                                "car_type_name": current_car_type,
                                "brand_list": v_brand,
                                "car_list": v_car,
                                "model_list": v_model_list,
                                "model_list_1": v_model_list_1,
                                "model_list_2": v_model_list_2,
                                "car_name": car_name_val,
                                "release_dt": v_year,
                                "car_navi": v_navi,
                                "car_seat": v_seat,
                                "car_main_pay": v_finamt,
                                "amtsel": v_amtsel,
                                "status": v_status,
                                "copytext": v_copy,
                                "endtimedeal": v_endtd,
                                "detail_url": detail_url_val,
                                "car_imgs": car_imgs_val,
                                "date_crtr_pnttm": pnttm,
                                "create_dt": create_dt_full,
                            }
                            with open(list_path, "a", newline="", encoding="utf-8-sig") as fl:
                                wl = csv.DictWriter(fl, fieldnames=list_headers)
                                if car_counter == 1:
                                    wl.writeheader()
                                wl.writerow(list_row)

                            if v_product_id and v_status not in ["준비중", "판매완료"]:
                                fetch_detail_images_only(detail_page, v_product_id, detail_img_save_dir, logger)

                            car_counter += 1
                        except Exception as e:
                            logger.error(f"항목 수집 실패: {e}")

                    logger.info(f"목록 {current_page}페이지 수집 완료 → CSV 저장 (이번 페이지 {len(items)}건)")

                    if TEST_PAGE_LIMIT is not None and current_page >= TEST_PAGE_LIMIT:
                        break

                    prev_page = current_page
                    next_page_link = page.locator("li.pagination-con.page-num.active + li.pagination-con.page-num:not(.next):not(.prev) a").first
                    if next_page_link.count() > 0:
                        next_page_link.evaluate("el => el.click()")
                        page.wait_for_timeout(3000)
                    else:
                        next_grp = page.locator("li.pagination-con.next:not(.disabled) a").first
                        if next_grp.count() > 0:
                            next_grp.evaluate("el => el.click()")
                            page.wait_for_timeout(4000)
                            page.wait_for_selector("li.pagination-con.page-num.active", timeout=8000)
                        else:
                            break
                    page.wait_for_timeout(1500)

                    try:
                        new_active = page.locator("li.pagination-con.page-num.active")
                        new_page = int(new_active.inner_text() or "0")
                        if new_page == prev_page:
                            page.wait_for_timeout(1500)
                            break
                    except Exception:
                        break

                if n_car_types > 1:
                    try:
                        choice_delete = page.locator(
                            '.lp-filter-list .lp-filter-choice:has(span[data-cls="cate-cb"])'
                        ).filter(has_text=current_car_type).locator(".lp-filter-choice-delete").first
                        if choice_delete.count() > 0:
                            choice_delete.click()
                            page.wait_for_timeout(2000)
                            logger.info(f"차종 필터 칩 제거: {current_car_type}")
                    except Exception as e:
                        logger.warning(f"차종 칩 제거 실패 ({current_car_type}): {e}")
        finally:
            browser.close()

    return {
        "brand_csv": str(brand_path),
        "car_type_csv": car_type_csv_path or "",
        "list_csv": str(list_path),
        "count": car_counter - 1,
        "log_dir": str(LOG_DIR),
        "img_base": str(IMG_BASE),
    }


@dag(
    dag_id="sdag_reborncar_crawler",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["used_car", "reborncar", "crawler", "day"],
)
def reborncar_crawler_dag():
    """
    리본카 브랜드/차종/목록 수집 DAG.

    - DB 메타(std.tn_data_bsc_info, 리본카 ps00003 · data19/20/21) 조회
    - 브랜드 CSV → 차종 CSV → 목록/이미지 순, 태스크별 파일 생성 (헤이딜러 DAG와 동일 패턴)
    """

    @task
    def insert_collect_data_info() -> dict[str, dict[str, Any]]:
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        site = REBORNCAR_PVSN_SITE_CD.lower().strip()
        sql = f"""
       select * from std.tn_data_bsc_info
       WHERE 1=1
         AND LOWER(clct_yn) = 'y'
         AND LOWER(link_yn) = 'y'
         AND LOWER(pvsn_site_cd) = '{site}'
         AND LOWER(datst_cd) IN ('data19','data20','data21')
       ORDER by data_sn
        """
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                out: dict[str, dict[str, Any]] = {}
                for row in cur.fetchall() or []:
                    raw = dict(zip(cols, row))
                    d: dict[str, Any] = {}
                    for k, v in raw.items():
                        if isinstance(v, (datetime, date, dt_time)):
                            d[k] = v.isoformat()
                        else:
                            d[k] = v
                    k = str(d.get("datst_cd") or "").lower().strip()
                    if k and k not in out:
                        out[k] = d
        finally:
            try:
                conn.close()
            except Exception:
                pass

        missing = [k for k in ("data19", "data20", "data21") if k not in out]
        if missing:
            raise ValueError(
                f"std.tn_data_bsc_info 조회 결과 누락: {missing} (pvsn_site_cd={REBORNCAR_PVSN_SITE_CD})"
            )
        return out

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]]) -> str:
        dc = str((infos.get(REBORNCAR_DATST_BRAND) or {}).get("datst_cd") or REBORNCAR_DATST_BRAND).lower()
        return _run_reborncar_brand_csv(dc)

    @task
    def run_car_type_csv(infos: dict[str, dict[str, Any]]) -> str:
        dc = str((infos.get(REBORNCAR_DATST_CAR_TYPE) or {}).get("datst_cd") or REBORNCAR_DATST_CAR_TYPE).lower()
        return _run_reborncar_car_type_csv(dc)

    @task
    def run_list_csv(
        bsc_infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        car_type_csv_path: str,
    ) -> dict[str, Any]:
        b_list = BscInfo.from_row(bsc_infos[REBORNCAR_DATST_LIST])
        return run_reborncar_list_job(
            b_list,
            brand_list_csv_path=brand_csv_path,
            car_type_csv_path=car_type_csv_path,
        )

    infos = insert_collect_data_info()
    brand_path = run_brand_csv(infos)
    car_type_path = run_car_type_csv(infos)
    run_list_csv(infos, brand_path, car_type_path)


reborncar_dag = reborncar_crawler_dag()