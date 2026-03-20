import csv
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

try:
    from pendulum import datetime as pd_datetime
except Exception:
    pd_datetime = None

from airflow.decorators import dag, task

_now = datetime.now()
YEAR_STR = _now.strftime("%Y년")
DATE_STR = _now.strftime("%Y%m%d")

BASE_DATA_PATH = Path("/home/limhayoung/data")
RESULT_DIR = BASE_DATA_PATH / "crawl" / YEAR_STR / "현대인증중고" / DATE_STR
LOG_DIR = BASE_DATA_PATH / "log" / YEAR_STR / "현대인증중고" / DATE_STR
IMG_BASE = BASE_DATA_PATH / "img" / YEAR_STR / "현대인증중고" / DATE_STR

import requests
from playwright.sync_api import sync_playwright


def _hyundaicar_list_img_dir(dt=None):
    now = dt or datetime.now()
    return _root / "imgs" / "hyundaicar" / "list" / f"{now.year}년" / now.strftime("%Y%m%d")

URL = "https://certified.hyundai.com/p/search/vehicle"
DETAIL_URL_TEMPLATE = "https://certified.hyundai.com/p/goods/goodsDetail.do?goodsNo={}"
# 차종별 목록: 0 = 전체 수집(더보기 끝까지), 양수면 해당 건수만 수집
LIST_PER_CAR_TYPE = 0


def _norm(s: str) -> str:
    return " ".join((s or "").strip().split())


def setup_logger():
    log_dir = Path(__file__).resolve().parent.parent / "logs" / "hyundaicar"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "hyundaicar_type_to_list.log"

    logger = logging.getLogger("HyundaiCarTypeList")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger


def _click_if_visible(page, selector: str, logger, timeout_ms: int = 800):
    """짧은 시간 내 보이면 클릭. 실패해도 예외를 올리지 않음."""
    try:
        el = page.locator(selector).first
        if el.count() > 0 and el.is_visible(timeout=timeout_ms):
            el.click()
            page.wait_for_timeout(400)
            # logger.info("팝업/버튼 클릭: %s", selector)
            return True
    except Exception:
        return False
    return False


def dismiss_common_popups(page, logger):
    """
    페이지 진입 시 자주 뜨는 팝업(동의/확인/닫기/제네시스 이동 여부 등)을 최대한 무해하게 처리.
    - 버튼 텍스트 기반으로 시도하되, 사이트/시점에 따라 없을 수 있으니 best-effort.
    """
    for _ in range(5):
        clicked = False
        # 1) 개인정보/약관 동의
        clicked |= _click_if_visible(page, "button:has-text('동의하기')", logger)
        clicked |= _click_if_visible(page, "a:has-text('동의하기')", logger)

        # 2) 제네시스 브랜드 이동 유도 팝업: '닫기'를 눌러 현재 페이지 유지
        clicked |= _click_if_visible(page, "button:has-text('닫기')", logger)
        clicked |= _click_if_visible(page, "a:has-text('닫기')", logger)

        # 3) 확인 팝업
        clicked |= _click_if_visible(page, "button:has-text('확인')", logger)
        clicked |= _click_if_visible(page, "a:has-text('확인')", logger)

        if not clicked:
            break


def _normalize_production_period(s: str) -> str:
    """'23년~현재' → '23~현재', '21~24년' → '21~24' (년 제거)."""
    if not s:
        return ""
    t = _norm(s).replace("년", "").strip()
    return t


def _label_txt(locator, selector: str = "label[for^='filter'] .txt"):
    """
    locator 범위 내 selector 텍스트 수집. 없으면 label .txt 시도.
    - 개별 요소에서 텍스트를 읽다가 Timeout/에러가 나도 전체 수집이 중단되지 않도록 보호.
    """
    els = locator.locator(selector)
    n = els.count()
    if n == 0:
        els = locator.locator("label .txt")
        n = els.count()
    if n == 0:
        return []
    results: list[str] = []
    for i in range(n):
        try:
            txt = els.nth(i).inner_text(timeout=1000)
            txt = _norm(txt)
            if txt:
                results.append(txt)
        except Exception:
            # 특정 li 안의 label 텍스트를 못 읽어도 그냥 스킵하고 계속 진행
            continue
    return results


def _label_txt_first(locator, selector: str = "label[for^='filter'] .txt"):
    """첫 번째만 반환."""
    vals = _label_txt(locator, selector)
    return vals[0] if vals else ""


def _label_txt_exclude_small(locator):
    """label 내 .txt 텍스트에서 small 텍스트를 제외한 값 반환 (model_list용)."""
    txt_el = locator.locator("label[for^='filter'] .txt").first
    if txt_el.count() == 0:
        txt_el = locator.locator("label .txt").first
    if txt_el.count() == 0:
        return ""
    txt_text = _norm(txt_el.inner_text())
    small_el = locator.locator("small").first
    if small_el.count() > 0:
        small_text = _norm(small_el.inner_text())
        if small_text and small_text in txt_text:
            txt_text = txt_text.replace(small_text, "", 1).strip()
    return txt_text


def run_hyundaicar_brand_list(page, result_dir: Path, logger, csv_path: Path | None = None):
    """
    #saleVehicleFilter 두번째 li에서 브랜드(현대/제네시스) 및 .brandalllist 트리 수집.
    - brand_list: .form_filter.brand 내 radio ga-tx '현대'/'제네시스'
    - .brandalllist 내 li[data-ref=toggleBox] .btn_toggle 클릭 후
      ul > li(label.txt)=car_list → ul > li=model_list, small=production_period,
      ul > li=model_list_1, ul > li=model_list_2
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    if csv_path is None:
        csv_path = result_dir / "hyundaicar_brand_list.csv"
    if csv_path.exists():
        csv_path.unlink()

    headers = [
        "model_sn",
        "brand_list",
        "car_list",
        "model_list",
        "model_list_1",
        "model_list_2",
        "production_period",
        "date_crtr_pnttm",
        "create_dt",
    ]
    now = datetime.now()
    date_crtr_pnttm = now.strftime("%Y%m%d")
    create_dt = now.strftime("%Y%m%d%H%M")
    model_sn = 0

    def write_row(f, w, brand_list, car_list, model_list, production_period, model_list_1, model_list_2):
        nonlocal model_sn
        model_sn += 1
        w.writerow({
            "model_sn": model_sn,
            "brand_list": brand_list or "",
            "car_list": car_list or "",
            "model_list": model_list or "",
            "model_list_1": model_list_1 or "",
            "model_list_2": model_list_2 or "",
            "production_period": production_period or "",
            "date_crtr_pnttm": date_crtr_pnttm,
            "create_dt": create_dt,
        })
        f.flush()

    try:
        logger.info("============================================================")
        logger.info("현대 인증중고차 브랜드/차종/모델 목록 수집 시작")
        logger.info("============================================================")

        if URL not in (page.url or ""):
            page.goto(URL, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(2000)
            dismiss_common_popups(page, logger)
        page.wait_for_selector("#saleVehicleFilter", timeout=15000)
        page.wait_for_timeout(800)

        # 두번째 li (index 1): 브랜드/모델 영역
        all_toggle = page.locator("#saleVehicleFilter li[data-ref='toggleBox']")
        if all_toggle.count() < 2:
            logger.warning("saleVehicleFilter toggleBox li가 2개 미만입니다.")
            return
        second_li = all_toggle.nth(1)

        # 브랜드명: .form_filter.brand 내 radio(ga-tx 또는 인접 label)에서 "현대"/"제네시스"
        brand_names = []
        for ga_part in ("현대", "제네시스"):
            try:
                radio = second_li.locator(f".form_filter.brand input[type='radio'][ga-tx*='{ga_part}']").first
                if radio.count() > 0 and radio.is_visible(timeout=300):
                    brand_names.append(ga_part)
            except Exception:
                pass
        if not brand_names:
            for node in second_li.locator(".form_filter.brand label").all():
                try:
                    t = _norm(node.inner_text())
                    if t and t in ("현대", "제네시스"):
                        brand_names.append(t)
                except Exception:
                    pass
        if not brand_names:
            brand_names = ["현대", "제네시스"]

        # .brandalllist 내 펼침 li들
        brandalllist = second_li.locator(".brandalllist")
        if brandalllist.count() == 0:
            logger.warning(".brandalllist를 찾지 못했습니다.")
            return

        toggle_items = brandalllist.locator("li[data-ref='toggleBox']")
        n_car_blocks = toggle_items.count()
        logger.info("브랜드: %s, .brandalllist 내 펼침 블록 %d개", brand_names, n_car_blocks)

        with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            f.flush()

            for brand_name in brand_names:
                # 해당 브랜드 선택: radio 클릭
                try:
                    r = second_li.locator(f".form_filter.brand input[type='radio'][ga-tx*='{brand_name}']").first
                    if r.count() > 0 and r.is_visible(timeout=500):
                        r.click()
                        page.wait_for_timeout(600)
                except Exception:
                    pass

                # 다시 .brandalllist (선택에 따라 바뀔 수 있음)
                list_scope = second_li.locator(".brandalllist")
                items = list_scope.locator("li[data-ref='toggleBox']")
                count = items.count()
                if count == 0:
                    write_row(f, w, brand_name, "", "", "", "", "")
                    continue

                for idx in range(count):
                    item = items.nth(idx)
                    # 클래스명으로 현대/제네시스 구분 (예: brandlist brandhmclist ...)
                    cls = item.get_attribute("class") or ""
                    if brand_name == "현대" and "hmclist" not in cls:
                        continue
                    if brand_name == "제네시스" and "hmclist" in cls:
                        continue
                    try:
                        item.scroll_into_view_if_needed(timeout=3000)
                        page.wait_for_timeout(200)
                    except Exception:
                        pass
                    # 펼치기: .btn_toggle 클릭 → open 클래스 추가
                    try:
                        btn = item.locator(".btn_toggle").first
                        if btn.count() > 0 and btn.is_visible(timeout=500):
                            # 이미 open이면 스킵
                            cls = item.get_attribute("class") or ""
                            if "open" not in cls:
                                btn.click()
                                page.wait_for_timeout(500)
                    except Exception as e:
                        logger.debug("btn_toggle 클릭 실패 idx=%s: %s", idx, e)
                        continue

                    # 첫 번째 ul > li → car_list (각 li가 하나의 차종)
                    first_ul = item.locator("> ul").first
                    if first_ul.count() == 0:
                        continue
                    car_lis = first_ul.locator("> li")
                    num_car = car_lis.count()
                    if num_car == 0:
                        continue

                    for c in range(num_car):
                        car_li = car_lis.nth(c)
                        car_list_val = _label_txt_first(car_li)
                        if not car_list_val:
                            continue

                        # car_li 직계 첫 ul > li → model_list, small → production_period, 그 하위 ul → model_list_1, model_list_2
                        model_ul = car_li.locator("> ul").first
                        if model_ul.count() == 0:
                            write_row(f, w, brand_name, car_list_val, "", "", "", "")
                            continue
                        model_lis = model_ul.locator("> li")
                        for m in range(model_lis.count()):
                            model_li = model_lis.nth(m)
                            model_list_val = _label_txt_exclude_small(model_li)
                            small_el = model_li.locator("small").first
                            production_period_val = ""
                            if small_el.count() > 0:
                                production_period_val = _normalize_production_period(small_el.inner_text())

                            m1_uls = model_li.locator("> ul")
                            m1_count = m1_uls.count()
                            if m1_count == 0:
                                write_row(f, w, brand_name, car_list_val, model_list_val, production_period_val, "", "")
                                continue
                            for m1u in range(m1_count):
                                m1_ul = m1_uls.nth(m1u)
                                m1_lis = m1_ul.locator("> li")
                                for m1 in range(m1_lis.count()):
                                    m1_li = m1_lis.nth(m1)
                                    model_list_1_val = _label_txt_first(m1_li)
                                    m2_uls = m1_li.locator("> ul")
                                    m2_count = m2_uls.count()
                                    if m2_count == 0:
                                        write_row(f, w, brand_name, car_list_val, model_list_val, production_period_val, model_list_1_val, "")
                                        continue
                                    for m2u in range(m2_count):
                                        m2_ul = m2_uls.nth(m2u)
                                        for m2 in range(m2_ul.locator("> li").count()):
                                            m2_li = m2_ul.locator("> li").nth(m2)
                                            model_list_2_val = _label_txt_first(m2_li)
                                            write_row(f, w, brand_name, car_list_val, model_list_val, production_period_val, model_list_1_val, model_list_2_val)

                    logger.info("[%s] 블록 %d/%d 처리 완료", brand_name, idx + 1, count)

        logger.info("============================================================")
        logger.info("✅ 브랜드 목록 저장 완료: %s (총 %d건)", csv_path, model_sn)
        logger.info("============================================================")
    except Exception as e:
        logger.error("브랜드 목록 수집 오류: %s", e, exc_info=True)


def run_hyundaicar_car_type_list(page, result_dir: Path, logger, csv_path: Path | None = None):
    result_dir.mkdir(parents=True, exist_ok=True)
    if csv_path is None:
        csv_path = result_dir / "hyundaicar_car_type_list.csv"
    if csv_path.exists():
        csv_path.unlink()

    headers = ["car_type_sn", "car_type_name", "date_crtr_pnttm", "create_dt"]

    logger.info("============================================================")
    logger.info("현대 인증중고차 차종 목록 수집 시작")
    logger.info("URL: %s", URL)
    logger.info("============================================================")

    try:
        page.goto(URL, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(2000)

        dismiss_common_popups(page, logger)

        # 필터 컨테이너 대기
        page.wait_for_selector("#CPOwrap, #saleVehicleFilter", timeout=30000)
        page.wait_for_timeout(1200)

        dismiss_common_popups(page, logger)

        # 첫번째 toggleBox 선택 (isShow 우선)
        box = page.locator("#saleVehicleFilter li[data-ref='toggleBox'].isShow").first
        if box.count() == 0:
            box = page.locator("#saleVehicleFilter li[data-ref='toggleBox']").first

        if box.count() == 0:
            logger.warning("saleVehicleFilter 내 toggleBox li를 찾지 못했습니다.")
            return

        # 차종 텍스트: label[for*="filter_car_type"] 아래 .txt
        txt_locator = box.locator(".cont .form_filter.model label[for*='filter_car_type'] .txt")
        try:
            txt_locator.first.wait_for(state="visible", timeout=15000)
        except Exception:
            # fallback: form_filter.model 범위만 유지하고 label 조건 완화
            txt_locator = box.locator(".cont .form_filter.model .txt")
            try:
                txt_locator.first.wait_for(state="visible", timeout=8000)
            except Exception:
                pass

        n = txt_locator.count()
        if n == 0:
            logger.warning("차종 텍스트(.txt) 요소를 찾지 못했습니다. (count=0)")
            return

        now = datetime.now()
        date_crtr_pnttm = now.strftime("%Y%m%d")
        create_dt = now.strftime("%Y%m%d%H%M")

        seen = set()
        rows = []
        for i in range(n):
            name = _norm(txt_locator.nth(i).inner_text())
            if not name:
                continue
            if name in seen:
                continue
            seen.add(name)
            rows.append(name)

        if not rows:
            logger.warning("차종 텍스트를 추출했지만 유효 데이터가 없습니다.")
            return

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for idx, name in enumerate(rows, start=1):
                w.writerow(
                    {
                        "car_type_sn": idx,
                        "car_type_name": name,
                        "date_crtr_pnttm": date_crtr_pnttm,
                        "create_dt": create_dt,
                    }
                )
                logger.info("[%d/%d] %s", idx, len(rows), name)

        logger.info("============================================================")
        logger.info("✅ 저장 완료: %s", csv_path)
        logger.info("총 수집 차종 수: %d", len(rows))
        logger.info("============================================================")
    except Exception as e:
        logger.error("차종 수집 오류: %s", e, exc_info=True)


def _extract_product_id_from_href(href: str) -> str:
    """href=\"javascript:common.link.goodsDeatil('GJX251113020892');\" 등에서 product_id 추출."""
    if not href:
        return ""
    m = re.search(r"goodsDeatil\s*\(\s*['\"]([^'\"]+)['\"]", href, re.I)
    if m:
        return (m.group(1) or "").strip()
    m = re.search(r"goodsDetail\s*\(\s*['\"]([^'\"]+)['\"]", href, re.I)
    if m:
        return (m.group(1) or "").strip()
    return ""


def _build_brand_match_key(text: str) -> str:
    """매칭용 키 정규화: 공백 정규화 후 ' (' → '(' 로 통일."""
    if not (text or "").strip():
        return ""
    key = _norm(text)
    return key.replace(" (", "(")


def _load_hyundaicar_brand_map(result_dir: Path, logger, brand_path: Path | None = None):
    """
    hyundaicar_brand_list.csv에서
    - 1순위: model_list + model_list_1 포함
    - 2순위: model_list만 포함
    - 3순위: car_list만 포함
    매칭을 위해, 각 행별로 세 가지 키를 모두 보관한 리스트를 반환.
    """
    csv_path = Path(brand_path) if brand_path else (result_dir / "hyundaicar_brand_list.csv")
    rows = []
    if not csv_path.exists():
        logger.debug("브랜드 CSV 없음: %s", csv_path)
        return rows
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                model_list = (row.get("model_list") or "").strip()
                model_list_1 = (row.get("model_list_1") or "").strip()
                car_list = (row.get("car_list") or "").strip()
                brand_list = (row.get("brand_list") or "").strip()

                # 1순위: model_list + model_list_1
                combined_src = " ".join(p for p in [model_list, model_list_1] if p)
                combined_key = _build_brand_match_key(combined_src) if combined_src else ""
                # 2순위: model_list
                model_key = _build_brand_match_key(model_list) if model_list else ""
                # 3순위: car_list
                car_key = _build_brand_match_key(car_list) if car_list else ""

                if not (combined_key or model_key or car_key):
                    continue

                rows.append(
                    {
                        "combined": combined_key,
                        "model": model_key,
                        "car": car_key,
                        "brand_list": brand_list,
                        "car_list": car_list,
                        "model_list": model_list,
                    }
                )
        logger.info("브랜드 매핑 %d건 로드 (list 매칭용)", len(rows))
    except Exception as e:
        logger.warning("브랜드 CSV 로드 실패: %s", e)
    return rows


def _find_brand_for_name(name: str, brand_rows):
    """
    차량 이름(name)에 대해,
    1순위: model_list+model_list_1(combined)이 포함되는 행 중 가장 긴 키
    2순위: model_list만 포함되는 행 중 가장 긴 키
    3순위: car_list만 포함되는 행 중 가장 긴 키
    를 찾아 (brand_list, car_list, model_list) 반환.
    """
    if not name or not brand_rows:
        return None
    nm_norm = _norm(name).replace(" (", "(")
    if not nm_norm:
        return None
    # 1순위: combined
    best = None
    best_len = -1
    for row in brand_rows:
        key = row.get("combined") or ""
        if not key:
            continue
        if key in nm_norm and len(key) > best_len:
            best = row
            best_len = len(key)
    if best:
        return best["brand_list"], best["car_list"], best.get("model_list", "")

    # 2순위: model
    best = None
    best_len = -1
    for row in brand_rows:
        key = row.get("model") or ""
        if not key:
            continue
        if key in nm_norm and len(key) > best_len:
            best = row
            best_len = len(key)
    if best:
        return best["brand_list"], best["car_list"], best.get("model_list", "")

    # 3순위: car_list
    best = None
    best_len = -1
    for row in brand_rows:
        key = row.get("car") or ""
        if not key:
            continue
        if key in nm_norm and len(key) > best_len:
            best = row
            best_len = len(key)
    if best:
        return best["brand_list"], best["car_list"], best.get("model_list", "")

    return None


def _extract_list_row_from_li(li, date_crtr_pnttm: str, create_dt: str):
    """li.type02 한 개에서 product_id, car_name, release_dt, ... dict 생성."""
    row = {
        "model_sn": 0,
        "product_id": "",
        "car_type": "",
        "car_name": "",
        "release_dt": "",
        "car_navi": "",
        "car_num": "",
        "local_dos": "",
        "pay": "",
        "del": "-",
        "sale": "",
        "flag": "",
        "model_list": "",
        "date_crtr_pnttm": date_crtr_pnttm,
        "create_dt": create_dt,
    }
    try:
        link = li.locator("a[href*='goodsDeatil'], a[href*='goodsDetail']").first
        if link.count() > 0:
            href = link.get_attribute("href") or ""
            row["product_id"] = _extract_product_id_from_href(href)
        name_el = li.locator(".unit_info .name").first
        if name_el.count() > 0:
            row["car_name"] = _norm(name_el.inner_text())
        drive_spans = li.locator(".unit_info .drive span")
        for s in range(min(4, drive_spans.count())):
            val = _norm(drive_spans.nth(s).inner_text())
            if s == 0:
                row["release_dt"] = val
            elif s == 1:
                row["car_navi"] = val
            elif s == 2:
                row["car_num"] = val
            else:
                row["local_dos"] = val
        if li.locator(".price .txt.pay").first.count() > 0:
            row["pay"] = _norm(li.locator(".price .txt.pay").first.inner_text())
        if li.locator(".price .txt.del").first.count() > 0:
            row["del"] = _norm(li.locator(".price .txt.del").first.inner_text()) or "-"
        if li.locator(".price .txt.sale").first.count() > 0:
            row["sale"] = _norm(li.locator(".price .txt.sale").first.inner_text())
        flag_spans = li.locator(".flag span")
        flag_vals = [_norm(flag_spans.nth(j).inner_text()) for j in range(flag_spans.count()) if _norm(flag_spans.nth(j).inner_text())]
        row["flag"] = "|".join(flag_vals)
    except Exception:
        pass
    return row


def _save_list_thumbnail_from_li(page, li, product_id: str, img_dir: Path, logger) -> str:
    """
    목록 li.type02 내 .unit_img 영역에서 썸네일 이미지를 1장 저장하고,
    저장된 경우 상대 경로( imgs/hyundaicar/list/연도년/날짜/{product_id}_list.png )를 반환.
    실패 시 빈 문자열 반환.
    """
    try:
        if not product_id:
            return ""
        img_el = li.locator(".unit_img img, [class*='unit_img'] img").first
        if img_el.count() == 0:
            return ""
        src = img_el.get_attribute("src") or img_el.get_attribute("data-src")
        if not src:
            return ""
        base_url = page.url if getattr(page, "url", None) else URL
        abs_url = urljoin(base_url, src)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": base_url,
        }
        r = requests.get(abs_url, headers=headers, timeout=15)
        r.raise_for_status()
        img_dir.mkdir(parents=True, exist_ok=True)
        out_path = img_dir / f"{product_id}_list.png"
        with open(out_path, "wb") as f:
            f.write(r.content)
        logger.debug("[list-thumb %s] 목록 썸네일 저장: %s", product_id, out_path)
        return out_path
    except Exception as e:
        logger.debug("[list-thumb %s] 목록 썸네일 저장 실패: %s", product_id, e)
        return ""


def run_hyundaicar_list(
    page,
    result_dir: Path,
    logger,
    list_csv_path: Path | None = None,
    brand_csv_path: Path | None = None,
    list_img_dir: Path | None = None,
):
    """
    차종(승용, SUV, 승합, EV)별로 선택 → 더보기(#btnSeeMore) 끝까지 전체 수집 → 해제 후 다음 차종 반복.
    .search_result .resultlist .product.productlist > li.type02 에서
    product_id, car_type, car_name, release_dt, car_navi, car_num, local_dos, pay, del, sale, flag, model_list 수집.
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = list_csv_path if list_csv_path is not None else (result_dir / "hyundaicar_list.csv")
    if csv_path.exists():
        csv_path.unlink()

    headers = [
        "model_sn",
        "product_id",
        "car_type",
        "brand_list",
        "car_list",
        "model_list",
        "car_name",
        "release_dt",
        "car_navi",
        "car_num",
        "local_dos",
        "pay",
        "del",
        "sale",
        "flag",
        "detail_url",  # 상세 페이지 URL (DETAIL_URL_TEMPLATE 기반)
        "car_imgs",
        "date_crtr_pnttm",
        "create_dt",
    ]
    now = datetime.now()
    date_crtr_pnttm = now.strftime("%Y%m%d")
    create_dt = now.strftime("%Y%m%d%H%M")

    year_str = f"{now.year}년"
    date_str = now.strftime("%Y%m%d")
    if list_img_dir is not None:
        img_save_dir = Path(list_img_dir)
        list_img_rel_prefix = None
        use_abs_car_imgs = True
    else:
        img_save_dir = _hyundaicar_list_img_dir(now)
        list_img_rel_prefix = f"imgs/hyundaicar/list/{year_str}/{date_str}"
        use_abs_car_imgs = False
    img_save_dir.mkdir(parents=True, exist_ok=True)

    # type02만 수집, type02.banner(광고) 제외
    list_selector = ".search_result .resultlist .product.productlist li.type02:not(.banner)"
    list_selector_fb = ".resultlist .productlist li.type02:not(.banner)"
    btn_more = "#btnSeeMore"

    try:
        logger.info("============================================================")
        logger.info("현대 인증중고차 검색 결과 목록 수집 (차종별 %s)", "전체" if LIST_PER_CAR_TYPE <= 0 else f"{LIST_PER_CAR_TYPE}건")
        logger.info("============================================================")

        if URL not in (page.url or ""):
            page.goto(URL, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(2000)
            dismiss_common_popups(page, logger)
        page.wait_for_selector("#CPOwrap, #saleVehicleFilter", timeout=20000)
        page.wait_for_timeout(1200)

        box = page.locator("#saleVehicleFilter li[data-ref='toggleBox'].isShow").first
        if box.count() == 0:
            box = page.locator("#saleVehicleFilter li[data-ref='toggleBox']").first
        if box.count() == 0:
            logger.warning("saleVehicleFilter toggleBox를 찾지 못했습니다.")
            return

        txt_locator = box.locator(".cont .form_filter.model label[for*='filter_car_type'] .txt")
        if txt_locator.count() == 0:
            txt_locator = box.locator(".cont .form_filter.model .txt")
        n_types = txt_locator.count()
        if n_types == 0:
            logger.warning("차종(.txt) 요소가 없습니다.")
            return

        car_type_names = []
        for i in range(n_types):
            t = _norm(txt_locator.nth(i).inner_text())
            if t and t not in car_type_names:
                car_type_names.append(t)
        if not car_type_names:
            logger.warning("차종 이름을 읽지 못했습니다.")
            return
        logger.info("차종 순서: %s", car_type_names)

        brand_map = _load_hyundaicar_brand_map(result_dir, logger, brand_path=brand_csv_path)

        total_written = 0
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            f.flush()

            for car_type_name in car_type_names:
                logger.info("---------- 차종 선택: %s ----------", car_type_name)
                label_el = box.locator("label").filter(has_text=car_type_name).first
                if label_el.count() == 0:
                    logger.warning("[%s] 차종 라벨 없음, 스킵", car_type_name)
                    continue
                label_el.click()
                page.wait_for_timeout(1800)

                items = page.locator(list_selector)
                if items.count() == 0:
                    items = page.locator(list_selector_fb)
                need = LIST_PER_CAR_TYPE if LIST_PER_CAR_TYPE > 0 else None  # None = 전체
                prev_count = 0
                no_change_count = 0
                while True:
                    n = items.count()
                    if need is not None and n >= need:
                        break
                    try:
                        btn = page.locator(btn_more).first
                        if btn.count() == 0:
                            break
                        btn.scroll_into_view_if_needed(timeout=3000)
                        if not btn.is_visible(timeout=1000):
                            break
                        btn.click()
                        page.wait_for_timeout(2000)
                        items = page.locator(list_selector)
                        if items.count() == 0:
                            items = page.locator(list_selector_fb)
                        new_count = items.count()
                        if new_count == prev_count:
                            no_change_count += 1
                            if no_change_count >= 2:
                                break  # 연속 2번 개수 동일하면 더 이상 없음
                        else:
                            no_change_count = 0
                        prev_count = new_count
                    except Exception:
                        break
                items = page.locator(list_selector)
                if items.count() == 0:
                    items = page.locator(list_selector_fb)
                n = items.count()
                to_take = min(need, n) if need is not None else n
                logger.info("[%s] 수집 %d건 (전체 %d건)", car_type_name, to_take, n)
                for i in range(to_take):
                    li = items.nth(i)
                    row = _extract_list_row_from_li(li, date_crtr_pnttm, create_dt)
                    total_written += 1
                    row["model_sn"] = total_written
                    row["car_type"] = car_type_name
                    # car_name 기준으로 브랜드/차종/모델 매핑
                    if row.get("car_name") and brand_map:
                        matched = _find_brand_for_name(row["car_name"], brand_map)
                        if matched:
                            row["brand_list"], row["car_list"], row["model_list"] = matched
                    # 상세 페이지 URL: goodsNo=product_id 패턴
                    pid = row.get("product_id") or ""
                    row["detail_url"] = DETAIL_URL_TEMPLATE.format(pid) if pid else ""
                    # 목록 썸네일: unit_img 기준 첫 번째 이미지 저장 후 car_imgs에 경로 기록
                    thumb_path = ""
                    try:
                        thumb_full_path = _save_list_thumbnail_from_li(page, li, row.get("product_id") or "", img_save_dir, logger)
                        if thumb_full_path:
                            if use_abs_car_imgs:
                                thumb_path = str(Path(thumb_full_path))
                            else:
                                thumb_path = f"{list_img_rel_prefix}/{row['product_id']}_list.png"
                    except Exception:
                        thumb_path = ""
                    row["car_imgs"] = thumb_path
                    w.writerow(row)
                    f.flush()
                    logger.info(
                        "[%s] %d/%d %s %s",
                        car_type_name,
                        i + 1,
                        to_take,
                        row["product_id"],
                        (row.get("car_name") or "")[:25],
                    )

                label_el.click()
                page.wait_for_timeout(800)

        logger.info("============================================================")
        logger.info("✅ 목록 저장 완료: %s (총 %d건)", csv_path, total_written)
        logger.info("============================================================")
    except Exception as e:
        logger.error("목록 수집 오류: %s", e, exc_info=True)


def _save_hyundaicar_detail_images(page, product_id: str, save_dir: Path, detail_url: str, logger):
    """
    상세 페이지에서 '이미지 보기' 클릭 후
    data-ref="uspGallery" 아래 .usp_main .usp_main_img, .usp_list [data-ref="uspGalleryItemList"] .item 이미지 수집
    → save_dir/product_id_1.png, product_id_2.png ... 저장.
    """
    if not product_id or not save_dir:
        return 0
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    base_url = detail_url.rsplit("?", 1)[0] if "?" in detail_url else detail_url
    urls = []

    try:
        # '이미지 보기' 버튼이 보일 때까지 스크롤 후 클릭
        btn = page.locator(".btn_img button, .btn_img a, .btn_img").first
        if btn.count() > 0:
            btn.scroll_into_view_if_needed(timeout=5000)
            if btn.is_visible(timeout=3000):
                btn.click()
                page.wait_for_timeout(2000)

        # data-ref="uspGallery" 갤러리 영역 대기 (클릭 시 .pdp01_car.ready → .uspGallisOpen 추가됨)
        gallery = page.locator('[data-ref="uspGallery"]').first
        gallery.wait_for(state="visible", timeout=10000)
        page.wait_for_timeout(800)

        # 1) .usp_main .usp_main_img (메인 이미지)
        main_img = gallery.locator(".usp_main .usp_main_img img").first
        if main_img.count() == 0:
            main_img = gallery.locator(".usp_main .usp_main_img").first
        if main_img.count() > 0:
            src = main_img.get_attribute("src") or main_img.get_attribute("data-src")
            if src:
                urls.append(src)

        # 2) .usp_list [data-ref="uspGalleryItemList"] .item (class에 item 포함) 내 이미지
        items = gallery.locator('.usp_list [data-ref="uspGalleryItemList"] [class*="item"]')
        for i in range(items.count()):
            item = items.nth(i)
            img = item.locator("img").first
            if img.count() > 0:
                src = img.get_attribute("src") or img.get_attribute("data-src")
                if src:
                    urls.append(src)
    except Exception as e:
        logger.warning("이미지 영역 추출 실패 %s: %s", product_id, e)

    saved = 0
    for idx, src in enumerate(urls, start=1):
        try:
            full_url = urljoin(base_url, src) if not (src.startswith("http") or src.startswith("//")) else ("https:" + src if src.startswith("//") else src)
            resp = page.request.get(full_url, timeout=15000)
            if resp.ok:
                path = save_dir / f"{product_id}_{idx}.png"
                path.write_bytes(resp.body())
                saved += 1
        except Exception as e:
            logger.warning("이미지 저장 실패 %s_%s: %s", product_id, idx, e)
    if saved > 0:
        logger.info("%s 이미지 %d장 저장: %s", product_id, saved, save_dir)
    return saved


def _update_hyundaicar_list_car_imgs(result_dir: Path, product_id: str, car_imgs_path: str, logger, list_path: Path | None = None):
    """hyundaicar_list.csv에서 해당 product_id 행의 car_imgs 컬럼을 경로로 갱신."""
    list_path = Path(list_path) if list_path else (result_dir / "hyundaicar_list.csv")
    if not list_path.exists():
        return
    try:
        with open(list_path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            fieldnames = list(r.fieldnames or [])
            rows = list(r)
        if "car_imgs" not in fieldnames:
            if "date_crtr_pnttm" in fieldnames:
                idx = fieldnames.index("date_crtr_pnttm")
                fieldnames.insert(idx, "car_imgs")
            else:
                fieldnames.append("car_imgs")
            for row in rows:
                row.setdefault("car_imgs", "")
        for row in rows:
            if (row.get("product_id") or "").strip() == product_id:
                row["car_imgs"] = car_imgs_path
                break
        with open(list_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
    except Exception as e:
        logger.warning("목록 CSV car_imgs 갱신 실패 %s: %s", product_id, e)


def _collect_hyundaicar_images_for_ids(
    page, product_ids: list, img_dir: Path, car_imgs_path: str, result_dir: Path, logger, max_retries: int = 2, list_csv_path: Path | None = None
):
    """product_id 목록에 대해 상세 페이지 접속 → 이미지 수집 → car_imgs 갱신. 실패 시 최대 max_retries회 재시도."""
    for i, product_id in enumerate(product_ids, start=1):
        detail_url = DETAIL_URL_TEMPLATE.format(product_id)
        saved = 0
        for attempt in range(max_retries + 1):
            try:
                page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(1500)
                page.wait_for_selector("#CPOwrap, .car_detail_cont, .pdp01_car", timeout=15000)
                page.wait_for_timeout(800)
                saved = _save_hyundaicar_detail_images(page, product_id, img_dir, detail_url, logger)
                if saved > 0:
                    _update_hyundaicar_list_car_imgs(result_dir, product_id, car_imgs_path, logger, list_path=list_csv_path)
                    break
                if attempt < max_retries:
                    logger.info("[%s] 이미지 0건, %d회 재시도 예정", product_id, attempt + 1)
            except Exception as e:
                logger.warning("[%d/%d] %s 상세 페이지 실패 (시도 %d/%d): %s", i, len(product_ids), product_id, attempt + 1, max_retries + 1, e)
                if attempt < max_retries:
                    page.wait_for_timeout(1000)
            if attempt < max_retries:
                page.wait_for_timeout(500)
        if saved == 0:
            logger.warning("[%d/%d] %s 이미지 수집 실패 (재시도 %d회 후)", i, len(product_ids), product_id, max_retries)
        if i < len(product_ids):
            page.wait_for_timeout(500)


def run_hyundaicar_detail_images(page, result_dir: Path, logger, list_csv_path: Path | None = None, img_dir: Path | None = None):
    """
    hyundaicar_list.csv의 product_id로 상세 페이지 접속 후
    이미지 보기 클릭 → data-ref="uspGallery" 내 이미지 수집
    저장: imgs/hyundaicar/YYYY년/YYYYMMDD/product_id_1.png, product_id_2.png ...
    저장 시 해당 행의 car_imgs 컬럼에 imgs/hyundaicar/년도/년월일 경로 기록.
    실패 건은 재시도 후, 마지막에 car_imgs 비어 있는 건만 2차 수집.
    """
    list_path = Path(list_csv_path) if list_csv_path else (result_dir / "hyundaicar_list.csv")
    if not list_path.exists():
        logger.warning("목록 파일 없음: %s (이미지 수집 스킵)", list_path)
        return

    now = datetime.now()
    if img_dir is None:
        img_dir = _root / "imgs" / "hyundaicar" / f"{now.year}년" / now.strftime("%Y%m%d")
        car_imgs_path = f"imgs/hyundaicar/{now.year}년/{now.strftime('%Y%m%d')}"
    else:
        img_dir = Path(img_dir)
        car_imgs_path = str(img_dir)
    img_dir.mkdir(parents=True, exist_ok=True)
    logger.info("이미지 저장 경로: %s", img_dir)

    product_ids = []
    try:
        with open(list_path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                pid = (row.get("product_id") or "").strip()
                if pid:
                    product_ids.append(pid)
    except Exception as e:
        logger.error("목록 CSV 읽기 실패: %s", e)
        return

    if not product_ids:
        logger.warning("product_id 없음. 이미지 수집 스킵.")
        return

    logger.info("============================================================")
    logger.info("현대 인증중고차 상세 이미지 수집 시작 (총 %d건, 실패 시 2회 재시도)", len(product_ids))
    logger.info("============================================================")

    _collect_hyundaicar_images_for_ids(
        page, product_ids, img_dir, car_imgs_path, result_dir, logger, max_retries=2, list_csv_path=list_path
    )

    # 2차: car_imgs가 비어 있는 product_id만 재수집
    missing = []
    try:
        with open(list_path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                pid = (row.get("product_id") or "").strip()
                car_imgs = (row.get("car_imgs") or "").strip()
                if pid and not car_imgs:
                    missing.append(pid)
    except Exception as e:
        logger.warning("car_imgs 빈 목록 읽기 실패: %s", e)

    if missing:
        logger.info("============================================================")
        logger.info("car_imgs 미채움 %d건 2차 수집 시작", len(missing))
        logger.info("============================================================")
        _collect_hyundaicar_images_for_ids(
            page, missing, img_dir, car_imgs_path, result_dir, logger, max_retries=2, list_csv_path=list_path
        )
        still_missing = 0
        try:
            with open(list_path, "r", encoding="utf-8-sig", newline="") as f:
                still_missing = sum(1 for row in csv.DictReader(f) if (row.get("product_id") or "").strip() and not (row.get("car_imgs") or "").strip())
        except Exception:
            pass
        if still_missing > 0:
            logger.warning("2차 수집 후에도 car_imgs 미채움 %d건 남음", still_missing)

    logger.info("============================================================")
    logger.info("✅ 상세 이미지 수집 완료")
    logger.info("============================================================")


@dag(
    dag_id="sdag_hyundaicar_crawler",
    schedule="@daily",
    start_date=(pd_datetime(2026, 3, 1, tz="Asia/Seoul") if pd_datetime else datetime(2026, 3, 1)),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["used_car", "hyundaicar", "full_logic"],
)
def hyundaicar_crawler_dag():

    def _get_file_logger(run_ts: str) -> logging.Logger:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger("hyundaicar_crawler")
        logger.setLevel(logging.INFO)
        log_path = str(LOG_DIR / f"hyundaicar_crawler_{run_ts}.log")
        for h in logger.handlers:
            if isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == log_path:
                return logger
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(fh)
        return logger

    @task
    def prepare_environment():
        RESULT_DIR.mkdir(parents=True, exist_ok=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        (IMG_BASE / "list").mkdir(parents=True, exist_ok=True)
        (IMG_BASE / "detail").mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        _get_file_logger(run_ts).info("🏁 hyundaicar crawler start")
        return run_ts

    @task
    def run_meta(run_ts: str):
        logger = _get_file_logger(run_ts)
        car_type_csv = RESULT_DIR / f"hyundaicar_car_type_list_{run_ts}.csv"
        brand_csv = RESULT_DIR / f"hyundaicar_brand_list_{run_ts}.csv"
        use_headed = os.environ.get("HYUNDAICAR_AIRFLOW_HEADED", "0").strip().lower() in ("1", "true", "yes")
        with sync_playwright() as p:
            kw = {"headless": not use_headed}
            if os.environ.get("HYUNDAICAR_CHROME_CHANNEL", "0").strip().lower() in ("1", "true", "yes"):
                kw["channel"] = "chrome"
            browser = p.chromium.launch(**kw)
            try:
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                )
                page = context.new_page()
                run_hyundaicar_car_type_list(page, RESULT_DIR, logger, csv_path=car_type_csv)
                run_hyundaicar_brand_list(page, RESULT_DIR, logger, csv_path=brand_csv)
            finally:
                browser.close()
        return {"run_ts": run_ts}

    @task
    def run_list_task(meta: dict):
        run_ts = meta["run_ts"]
        logger = _get_file_logger(run_ts)
        list_csv = RESULT_DIR / f"hyundaicar_list_{run_ts}.csv"
        brand_csv = RESULT_DIR / f"hyundaicar_brand_list_{run_ts}.csv"
        use_headed = os.environ.get("HYUNDAICAR_AIRFLOW_HEADED", "0").strip().lower() in ("1", "true", "yes")
        with sync_playwright() as p:
            kw = {"headless": not use_headed}
            if os.environ.get("HYUNDAICAR_CHROME_CHANNEL", "0").strip().lower() in ("1", "true", "yes"):
                kw["channel"] = "chrome"
            browser = p.chromium.launch(**kw)
            try:
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                )
                page = context.new_page()
                run_hyundaicar_list(
                    page,
                    RESULT_DIR,
                    logger,
                    list_csv_path=list_csv,
                    brand_csv_path=brand_csv,
                    list_img_dir=IMG_BASE / "list",
                )
            finally:
                browser.close()
        return "SUCCESS"

    run_ts = prepare_environment()
    meta = run_meta(run_ts)
    lst = run_list_task(meta)
    run_ts >> meta >> lst


hyundaicar_dag = hyundaicar_crawler_dag()


def main():
    logger = setup_logger()
    result_dir = Path(__file__).resolve().parent.parent / "result" / "hyundaicar"
    result_dir.mkdir(parents=True, exist_ok=True)

    # 기본값: WSL/서버 환경을 고려해 headless=True. 필요 시 HYUNDAICAR_HEADED=1 로 브라우저 창 표시.
    use_headed = os.environ.get("HYUNDAICAR_HEADED", "0").strip().lower() in ("1", "true", "yes")

    with sync_playwright() as p:
        launch_kwargs = {"headless": (not use_headed)}
        # 로컬 GUI 환경이면 Chrome 채널이 더 안정적인 경우가 있어 옵션 제공
        if os.environ.get("HYUNDAICAR_CHROME_CHANNEL", "0").strip().lower() in ("1", "true", "yes"):
            launch_kwargs["channel"] = "chrome"

        browser = p.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        try:
            # 차종/브랜드 목록이 필요할 때 아래 주석 해제
            # run_hyundaicar_car_type_list(page, result_dir, logger)
            # run_hyundaicar_brand_list(page, result_dir, logger)

            # 검색 결과 목록만 수집 (상세 이미지 수집은 비활성화)
            run_hyundaicar_list(page, result_dir, logger)
            # run_hyundaicar_detail_images(page, result_dir, logger)
        finally:
            browser.close()


if __name__ == "__main__":
    main()
