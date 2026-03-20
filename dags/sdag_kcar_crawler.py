# -*- coding: utf-8 -*-
"""
K Car 검색 페이지(https://www.kcar.com/bc/search)에서
차종·브랜드 목록 수집 및 검색 결과 리스트(차량 목록) 수집.

Airflow DAG: DB 메타(std.tn_data_bsc_info, ps00004, data10/11/12) 조회 후
차종 CSV → 브랜드 CSV → 목록/이미지 순으로 단계별 Task 실행.
"""
import csv
import logging
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

# kcar/ 에서 실행해도 config import 가능하도록 프로젝트 루트 추가
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from playwright.sync_api import sync_playwright

# Airflow Variable (reborncar/heydealer와 동일)
USED_CAR_SITE_NAMES_VAR = "used_car_site_names"
CRAWL_BASE_PATH_VAR = "crawl_base_path"

# DB `std.tn_data_bsc_info`: K Car 제공사이트 코드
KCAR_PVSN_SITE_CD = "ps00004"
KCAR_DATST_BRAND = "data10"
KCAR_DATST_CAR_TYPE = "data11"
KCAR_DATST_LIST = "data12"
KCAR_SITE_NAME = "케이카"


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
    return KCAR_SITE_NAME


def get_kcar_site_name() -> str:
    """
    K Car는 brand/data10, car_type/data11, list/data12가 모두 같은 사이트이므로
    datst_cd와 무관하게 동일한 폴더명을 사용한다.
    """
    for datst_cd in (KCAR_DATST_LIST, KCAR_DATST_BRAND, KCAR_DATST_CAR_TYPE):
        site_name = get_site_name_by_datst(datst_cd)
        if site_name and site_name.strip():
            return site_name
    return KCAR_SITE_NAME


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
    """K Car 공통 crawl / log / img 루트 경로 설정 (각 Task 시작 시 호출)."""
    global RESULT_DIR, LOG_DIR, IMG_BASE, YEAR_STR, DATE_STR, RUN_TS

    base = _get_crawl_base_path()
    site = get_kcar_site_name()
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
    activate_paths_for_datst(KCAR_DATST_LIST)
except Exception:
    pass

# 수집 제어
HEADLESS_MODE = True


def _extract_product_id_from_img_src(src):
    """
    이미지 src에서 product_id 추출.
    - 3dcarpicture/.../61310405_1/main/ 또는 61322118_2/main/ → 숫자만 (61310405, 61322118)
    - kcarM_61320099_045.jpg → 61320099
    """
    if not src:
        return ""
    # 숫자_1, 숫자_2 등 /main/ 앞 숫자만
    m = re.search(r"/(\d+)_\d+/main/", src)
    if m:
        return m.group(1)
    m = re.search(r"kcarM_(\d+)_", src)
    if m:
        return m.group(1)
    return ""


def _extract_product_id_from_href(href):
    """상세 링크 href에서 product_id 추출. 예: /bc/detail/61321299 → 61321299"""
    if not href:
        return ""
    m = re.search(r"/detail/(\d+)(?:\?|$|/)", href)
    if m:
        return m.group(1)
    m = re.search(r"detail[/_]?(\d{5,})", href, re.I)
    if m:
        return m.group(1)
    return ""


def get_logger():
    """Airflow 기본 로깅 사용."""
    return logging.getLogger("kcar")


def _get_file_logger(run_ts: str) -> logging.Logger:
    """
    Airflow 태스크 로그(UI)와 별개로, LOG_DIR에도 파일 로그를 남기기 위한 로거.
    - 파일: LOG_DIR/kcar_crawler_{run_ts}.log
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("kcar_crawler")
    logger.setLevel(logging.INFO)
    log_path = str(LOG_DIR / f"kcar_crawler_{run_ts}.log")
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == log_path:
            return logger
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    return logger


def _launch_browser(playwright, headless: bool):
    """WSL/Airflow 환경에서 Chromium 메모리 사용을 조금 줄여 실행."""
    return playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--no-sandbox",
        ],
    )


def run_kcar_car_type_list(page, result_dir: Path, logger, csv_path: Path | None = None):
    """
    K Car 검색 페이지에서 차종 목록 수집.
    body #__nuxt #__layout .Container .searchWrap ... .kcarSnb 내
    [aria-label="차종"] 하위 role="tabpanel" > .el-collapse-item__content > .menuItemList >
    .el-checkbox-group > .el-checkbox > .el-checkbox__label 텍스트 수집.
    """
    csv_path = csv_path or result_dir / "kcar_car_type_list.csv"
    if csv_path.exists():
        csv_path.unlink()
    headers = ["car_type_sn", "car_type_name", "date_crtr_pnttm", "create_dt"]
    url = "https://www.kcar.com/bc/search"

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(3000)

        # 약관/동의 팝업 확인 버튼 처리
        for _ in range(3):
            try:
                confirm_btn = page.locator("button:has-text('확인')").first
                if confirm_btn.is_visible(timeout=1000):
                    confirm_btn.click()
                    page.wait_for_timeout(800)
            except Exception:
                break

        # 사이드 SNB 로딩 대기 (차종이 첫 번째 아코디언인 경우 많음)
        page.wait_for_selector(".kcarSnb, .kcarSnbWrap, .menuItemList", timeout=20000)
        page.wait_for_timeout(1500)

        # 차종 영역 펼치기: aria-label="차종" 헤더 클릭
        try:
            header = page.locator('[aria-label="차종"]').first
            if header.is_visible(timeout=2000):
                header.click()
                page.wait_for_timeout(1000)
        except Exception:
            pass

        labels = None
        n = 0

        # 1) 차종 탭 패널 내 .el-checkbox__label (aria-label 차종 하위)
        try:
            section = page.locator(
                '.kcarSnb .el-collapse-item:has([aria-label="차종"]) .menuItemList .el-checkbox-group .el-checkbox'
            )
            section.first.wait_for(state="visible", timeout=5000)
            labels = section.locator(".el-checkbox__label")
            n = labels.count()
        except Exception:
            pass

        # 2) 첫 번째 .menuItemList 내 차종 체크박스 (차종이 첫 번째 메뉴인 경우)
        if n == 0:
            try:
                first_menu = page.locator(".kcarSnb .menuItemList").first
                first_menu.wait_for(state="visible", timeout=5000)
                labels = first_menu.locator(".el-checkbox .el-checkbox__label")
                n = labels.count()
            except Exception:
                pass

        # 3) .kcarSnb 내 모든 .el-checkbox__label 중 첫 번째 그룹만 (role=group 첫 번째)
        if n == 0:
            try:
                first_group = page.locator(".kcarSnb .el-checkbox-group.chkGroup").first
                first_group.wait_for(state="visible", timeout=5000)
                labels = first_group.locator(".el-checkbox .el-checkbox__label")
                n = labels.count()
            except Exception:
                pass

        if n == 0:
            logger.warning("차종 라벨 요소를 찾지 못했습니다.")
            return

        logger.info("============================================================")
        logger.info("K Car 차종(car_type) 계층 데이터 수집 시작")
        logger.info("============================================================")
        logger.info("총 %d개 차종 데이터 수집 시작", n)

        car_type_sn = 1
        now = datetime.now()
        base_date_crtr_pnttm = now.strftime("%Y%m%d")
        base_create_dt = now.strftime("%Y%m%d%H%M")
        wrote_any = False
        for i in range(n):
            name = labels.nth(i).inner_text().strip()
            if not name:
                continue

            # 한 종 발견될 때마다 바로 CSV append
            first_write = not csv_path.exists()
            with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=headers)
                if first_write:
                    w.writeheader()
                w.writerow(
                    {
                        "car_type_sn": car_type_sn,
                        "car_type_name": name,
                        "date_crtr_pnttm": base_date_crtr_pnttm,
                        "create_dt": base_create_dt,
                    }
                )

            logger.info("[%d/%d] 차종 처리 중: %s", car_type_sn, n, name)
            for h in logger.handlers:
                h.flush()
            car_type_sn += 1
            wrote_any = True

        if wrote_any:
            logger.info("============================================================")
            logger.info("✅ 수집 완료! 파일: %s", csv_path)
            logger.info("총 수집 차종 수: %s개", f"{car_type_sn - 1:,}")
        else:
            logger.warning("차종 데이터 없음.")
    except Exception as e:
        logger.error("차종 수집 오류: %s", e, exc_info=True)


def _model_list_format(text):
    """model_list 값: 뒤에서 첫 번째 '(' 기준으로 '앞부분|(괄호내용)' 형태로 변환. 예: '더 뉴 i40 (15~19년)' → '더 뉴 i40|(15~19년)'"""
    if not text or "(" not in text:
        return (text or "").strip()
    last_open = text.rfind("(")
    prefix = text[:last_open].strip()
    suffix = text[last_open:].strip()
    if not prefix:
        return text.strip()
    return f"{prefix}|{suffix}"


def _collect_brand_depth1_block(page, depth1_block, depth_1_value, csv_path, headers, logger, model_sn_ref, start_index=0, total_brands=0):
    """한 개의 depth1 블록(국산 또는 수입) 내부 depth2→depth3→depth4→depth5→depth6 수집 후 CSV에 append. model_sn_ref는 [1] 형태 리스트로 전달."""

    def append_row(row):
        """행을 CSV에 쓸 때 공통 전처리."""
        # 1) 일련번호
        row["model_sn"] = model_sn_ref[0]
        model_sn_ref[0] += 1

        # 2) production_period 자동 생성 및 model_list 본문만 남기기
        original_ml = row.get("model_list") or ""
        if not row.get("production_period"):
            production_period = ""
            if "|" in original_ml:
                _, suffix = original_ml.split("|", 1)
                suffix = suffix.strip()
                if suffix.startswith("(") and suffix.endswith(")"):
                    suffix = suffix[1:-1].strip()
                production_period = suffix.replace("년", "").strip()
            if production_period:
                row["production_period"] = production_period

        # model_list 컬럼에는 '|' 앞(모델명)만 넣기
        if original_ml:
            if "|" in original_ml:
                row["model_list"] = original_ml.split("|", 1)[0].strip()
            else:
                row["model_list"] = original_ml.strip()

        # 3) 생성일시/생성일 기본값
        now = datetime.now()
        if not row.get("date_crtr_pnttm"):
            row["date_crtr_pnttm"] = now.strftime("%Y%m%d")
        if not row.get("create_dt"):
            row["create_dt"] = now.strftime("%Y%m%d%H%M")

        # 4) 컬럼 정규화: 비어 있으면 '-'로 채워 컬럼 밀림 방지
        normalized = {}
        for key in headers:
            val = row.get(key, "")
            if key == "model_sn":
                normalized[key] = val
                continue
            if isinstance(val, str):
                val = val.strip()
            if not val:
                val = "-"
            normalized[key] = val

        with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=headers).writerow(normalized)

    logger.info("  → [%s] depth2 목록 조회 중...", depth_1_value)
    for h in logger.handlers:
        h.flush()
    try:
        depth1_block.scroll_into_view_if_needed()
        page.wait_for_timeout(500)
    except Exception:
        pass
    # depth2는 브랜드(현대, 기아, 제네시스 등)만 대상 → 직계 li만 사용 (하위 depth3/depth4 li 제외)
    depth2_lis = depth1_block.locator(".depth2 > ul > li")
    if depth2_lis.count() == 0:
        depth2_lis = depth1_block.locator(".depth2 > li")
    if depth2_lis.count() == 0:
        depth2_lis = depth1_block.locator(".depth2 li")
    try:
        depth2_lis.first.wait_for(state="visible", timeout=10000)
    except Exception as e:
        logger.warning("  → [%s] depth2 요소 대기 실패: %s", depth_1_value, e)
        for h in logger.handlers:
            h.flush()
        return 0
    depth2_count = depth2_lis.count()
    logger.info("  → [%s] depth2 %d개 발견, 수집 시작...", depth_1_value, depth2_count)
    for h in logger.handlers:
        h.flush()
    row_count = 0

    for i in range(depth2_count):
        logger.info("  → [%s] %d/%d 번째 브랜드 요소 처리 중...", depth_1_value, i + 1, depth2_count)
        for h in logger.handlers:
            h.flush()
        d2_li = depth2_lis.nth(i)
        try:
            d2_li.scroll_into_view_if_needed()
            page.wait_for_timeout(200)
        except Exception:
            pass
        depth_2 = ""
        for sel in (".el-checkbox__label", "label.el-checkbox__label", "label", ".el-checkbox label"):
            try:
                el = d2_li.locator(sel).first
                el.wait_for(state="visible", timeout=2000)
                depth_2 = (el.inner_text(timeout=2000) or el.text_content() or "").strip()
                if depth_2:
                    break
            except Exception:
                continue
        if not depth_2:
            logger.warning("  → [%s] %d번째 브랜드 이름 읽기 실패 (모든 셀렉터 시도)", depth_1_value, i + 1)
            for h in logger.handlers:
                h.flush()
            continue

        current_num = start_index + i + 1
        row_count_this_brand = 0
        if total_brands > 0:
            logger.info("[%d/%d] 브랜드 처리 중 [%s]: %s", current_num, total_brands, depth_1_value, depth_2)
        logger.info("  → [%s] '%s' 펼치는 중...", depth_1_value, depth_2)
        for h in logger.handlers:
            h.flush()

        def _wait_depth3_visible():
            try:
                d2_li.locator(".depth3").first.wait_for(state="visible", timeout=6000)
                return True
            except Exception:
                pass
            try:
                depth1_block.locator(".depth3").first.wait_for(state="visible", timeout=6000)
                return True
            except Exception:
                pass
            return False

        expanded = False
        try:
            d2_li.scroll_into_view_if_needed()
            page.wait_for_timeout(400)
            label_el = d2_li.locator("label").first
            label_el.click()
            page.wait_for_timeout(1200)
            if _wait_depth3_visible():
                expanded = True
        except Exception:
            pass
        if not expanded:
            try:
                d2_li.locator(".el-checkbox").first.scroll_into_view_if_needed()
                page.wait_for_timeout(300)
                d2_li.locator(".el-checkbox").first.click()
                page.wait_for_timeout(1200)
                if _wait_depth3_visible():
                    expanded = True
            except Exception:
                pass
        if not expanded:
            try:
                page.wait_for_timeout(600)
                d2_li.locator("label").first.click(force=True)
                page.wait_for_timeout(1500)
                if _wait_depth3_visible():
                    expanded = True
            except Exception:
                pass
        if not expanded:
            try:
                d2_li.evaluate("el => el.querySelector('label')?.click()")
                page.wait_for_timeout(1500)
                if _wait_depth3_visible():
                    expanded = True
            except Exception:
                pass
        if not expanded:
            logger.warning("  → [%s] '%s' 펼치기 실패, 다음 브랜드로", depth_1_value, depth_2)
            for h in logger.handlers:
                h.flush()
            continue

        # depth_3: 클릭한 depth2 li 내부의 depth3 먼저, 없으면 depth1_block 전체에서
        depth3_container = d2_li.locator("ul.depth3").first
        depth3_lis = depth3_container.locator("> li")
        depth3_count = depth3_lis.count()
        if depth3_count == 0:
            depth3_container = d2_li.locator(".depth3").first
            depth3_lis = depth3_container.locator("> li")
            depth3_count = depth3_lis.count()
        if depth3_count == 0:
            depth3_container = depth1_block.locator("ul.depth3").first
            depth3_lis = depth3_container.locator("> li")
            depth3_count = depth3_lis.count()
        if depth3_count == 0:
            depth3_container = depth1_block.locator(".depth3").first
            depth3_lis = depth3_container.locator("> li")
            depth3_count = depth3_lis.count()
        if depth3_count == 0:
            depth3_lis = depth1_block.locator(".depth3 li")
            depth3_count = depth3_lis.count()
        logger.info("  → [%s] '%s' 하위 %d개 모델 수집 중...", depth_1_value, depth_2, depth3_count)
        for h in logger.handlers:
            h.flush()

        for j in range(depth3_count):
            d3_li = depth3_lis.nth(j)
            try:
                d3_li.scroll_into_view_if_needed()
                page.wait_for_timeout(100)
            except Exception:
                pass
            depth_3 = ""
            for sel in (".el-checkbox__label", "label.el-checkbox__label", "label", ".el-checkbox label"):
                try:
                    el = d3_li.locator(sel).first
                    el.wait_for(state="visible", timeout=1000)
                    depth_3 = (el.inner_text(timeout=1000) or el.text_content() or "").strip()
                    if depth_3:
                        break
                except Exception:
                    continue
            if not depth_3:
                continue

            try:
                d3_li.locator("label").first.scroll_into_view_if_needed()
                page.wait_for_timeout(300)
                d3_li.locator("label").first.click()
                page.wait_for_timeout(1200)
                # 현재 클릭한 depth_3 li 내부의 depth4 대기 (각 모델 li 안에 ul.depth4가 있음)
                d3_li.locator("ul.depth4 > li .el-checkbox__label").first.wait_for(state="visible", timeout=15000)
                page.wait_for_timeout(600)
            except Exception:
                try:
                    d3_li.locator(".el-checkbox").first.scroll_into_view_if_needed()
                    page.wait_for_timeout(300)
                    d3_li.locator(".el-checkbox").first.click()
                    page.wait_for_timeout(1200)
                    d3_li.locator("ul.depth4 > li .el-checkbox__label").first.wait_for(state="visible", timeout=15000)
                    page.wait_for_timeout(600)
                except Exception:
                    try:
                        page.wait_for_timeout(800)
                        d3_li.locator("label").first.click()
                        page.wait_for_timeout(1500)
                        d3_li.locator("ul.depth4 > li .el-checkbox__label").first.wait_for(state="visible", timeout=18000)
                        page.wait_for_timeout(500)
                    except Exception:
                        append_row({
                            "depth_1": depth_1_value,
                            "brand_list": depth_2,
                            "car_list": depth_3,
                            "model_list": "-",
                            "model_list_1": "-",
                            "model_list_2": "-",
                        })
                        row_count += 1
                        row_count_this_brand += 1
                        # logger.info("    → CSV 1건 추가: [%s] %s > %s > - > - > -", depth_1_value, depth_2, depth_3)
                        for h in logger.handlers:
                            h.flush()
                        try:
                            d3_li.locator("label").first.click()
                            page.wait_for_timeout(200)
                        except Exception:
                            pass
                        continue

            # depth_4: 현재 클릭한 depth_3 li 내부의 ul.depth4 > li만 수집 (i30 (PD) (16~20년), 더 뉴 i30 (15~16년) 등)
            depth4_container = d3_li.locator("ul.depth4").first
            depth4_lis = depth4_container.locator("> li")
            depth4_count = depth4_lis.count()
            if depth4_count == 0:
                depth4_container = d3_li.locator(".depth4").first
                depth4_lis = depth4_container.locator("> li")
                depth4_count = depth4_lis.count()
            if depth4_count == 0:
                logger.warning("  → [%s] '%s' > '%s' depth4 li 0개", depth_1_value, depth_2, depth_3)
                for h in logger.handlers:
                    h.flush()
                page.wait_for_timeout(500)
                depth4_lis = d3_li.locator("ul.depth4 > li")
                depth4_count = depth4_lis.count()
            depth4_added = 0
            for k in range(depth4_count):
                try:
                    d4_li = depth4_lis.nth(k)
                    d4_li.scroll_into_view_if_needed()
                    page.wait_for_timeout(50)
                except Exception:
                    pass
                depth_4 = ""
                for sel in (".el-checkbox__label", "label.el-checkbox__label", "label", ".el-checkbox label"):
                    try:
                        el = depth4_lis.nth(k).locator(sel).first
                        el.wait_for(state="visible", timeout=800)
                        depth_4 = (el.inner_text(timeout=800) or el.text_content() or "").strip()
                        if depth_4:
                            break
                    except Exception:
                        continue
                if not depth_4:
                    continue
                # depth_5: 현재 depth4 li 내부의 ul.depth5 > li 수집 (트림 등). 펼침이 필요하면 클릭 후 대기
                depth_5_values = []
                try:
                    d4_li.locator("label").first.click()
                    page.wait_for_timeout(800)
                except Exception:
                    pass
                depth5_container = d4_li.locator("ul.depth5").first
                depth5_lis = depth5_container.locator("> li") if depth5_container.count() > 0 else None
                if depth5_lis and depth5_lis.count() > 0:
                    for m in range(depth5_lis.count()):
                        d5_li = depth5_lis.nth(m)
                        depth_5 = ""
                        for sel in (".el-checkbox__label", "label.el-checkbox__label", "label", ".el-checkbox label"):
                            try:
                                el = d5_li.locator(sel).first
                                if el.count() > 0:
                                    depth_5 = (el.inner_text(timeout=500) or el.text_content() or "").strip()
                                    if depth_5:
                                        break
                            except Exception:
                                continue
                        if depth_5:
                            depth_5_values.append((depth_5, d5_li))
                if not depth_5_values:
                    depth_5_values.append(("-", None))
                for depth_5, d5_li in depth_5_values:
                    # depth_6: 현재 depth5 li 내부의 ul.depth6 > li 수집
                    depth_6_values = ["-"]
                    if d5_li and d5_li.count() > 0:
                        try:
                            d5_li.locator("label").first.click()
                            page.wait_for_timeout(600)
                        except Exception:
                            pass
                        depth6_container = d5_li.locator("ul.depth6").first
                        depth6_lis = depth6_container.locator("> li") if depth6_container.count() > 0 else None
                        if depth6_lis and depth6_lis.count() > 0:
                            depth_6_list = []
                            for n in range(depth6_lis.count()):
                                d6_li = depth6_lis.nth(n)
                                depth_6 = ""
                                for sel in (".el-checkbox__label", "label.el-checkbox__label", "label", ".el-checkbox label"):
                                    try:
                                        el = d6_li.locator(sel).first
                                        if el.count() > 0:
                                            depth_6 = (el.inner_text(timeout=500) or el.text_content() or "").strip()
                                            if depth_6:
                                                break
                                    except Exception:
                                        continue
                                if depth_6:
                                    depth_6_list.append(depth_6)
                            if depth_6_list:
                                depth_6_values = depth_6_list
                    for depth_6 in depth_6_values:
                        append_row({
                            "depth_1": depth_1_value,
                            "brand_list": depth_2,
                            "car_list": depth_3,
                            "model_list": _model_list_format(depth_4),
                            "model_list_1": depth_5 if depth_5 != "-" else "-",
                            "model_list_2": depth_6 if depth_6 != "-" else "-",
                        })
                        row_count += 1
                        row_count_this_brand += 1
                        depth4_added += 1

            if depth4_added == 0 and depth_3:
                append_row({
                    "depth_1": depth_1_value,
                    "brand_list": depth_2,
                    "car_list": depth_3,
                    "model_list": "-",
                    "model_list_1": "-",
                    "model_list_2": "-",
                })
                row_count += 1
                row_count_this_brand += 1

            if depth4_added > 0:
                logger.info("    → [%s] '%s' 하위 %d건 수집됨", depth_1_value, depth_3, depth4_added)
                for h in logger.handlers:
                    h.flush()

            try:
                d3_li.locator("label").first.click()
                page.wait_for_timeout(300)
            except Exception:
                pass

        logger.info("  → [%s] '%s' 수집 완료 (%d건)", depth_1_value, depth_2, row_count_this_brand)
        for h in logger.handlers:
            h.flush()

        try:
            d2_li.locator("label").first.click()
            page.wait_for_timeout(400)
        except Exception:
            pass

    return row_count


def run_kcar_brand_list(page, result_dir: Path, logger, csv_path: Path | None = None):
    """
    제조사/모델 영역(aria-label="제조사/모델")에서
    첫 번째 depth1=국산, 두 번째 depth1=수입 각각에 대해
    depth2 → depth3 → depth4 계층을 클릭하며 수집 (국산 먼저, 수입 후) → brand_list.csv
    """
    csv_path = csv_path or result_dir / "kcar_brand_list.csv"
    if csv_path.exists():
        csv_path.unlink()
    headers = [
        "model_sn",
        "depth_1",
        "brand_list",
        "car_list",
        "model_list",
        "model_list_1",
        "model_list_2",
        "production_period",
        "date_crtr_pnttm",
        "create_dt",
    ]
    url = "https://www.kcar.com/bc/search"

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(5000)

        for _ in range(3):
            try:
                confirm_btn = page.locator("button:has-text('확인')").first
                if confirm_btn.is_visible(timeout=1000):
                    confirm_btn.click()
                    page.wait_for_timeout(800)
            except Exception:
                break

        # 사이드 SNB 영역 대기 (클래스가 kcarSnbWrap 등으로 둘러싸인 경우 대비)
        for snb_sel in (".kcarSnb", ".kcarSnbWrap", ".menuItemList", "[class*='kcarSnb']", ".el-collapse-item__content"):
            try:
                page.wait_for_selector(snb_sel, timeout=15000)
                page.wait_for_timeout(1500)
                break
            except Exception:
                continue
        else:
            raise RuntimeError("사이드 SNB 영역(.kcarSnb 등)을 찾을 수 없습니다. 페이지가 완전히 로드되었는지 확인하세요.")

        # 제조사/모델 아코디언 헤더 클릭 후 컨텐츠 로딩 대기
        try:
            header = page.locator('[aria-label="제조사/모델"]').first
            header.wait_for(state="visible", timeout=8000)
            header.click()
            page.wait_for_timeout(2500)
        except Exception:
            pass

        # 스크롤하여 제조사/모델 영역이 보이도록
        try:
            snb = page.locator(".kcarSnb, .kcarSnbWrap").first
            if snb.count() > 0:
                snb.scroll_into_view_if_needed()
                page.wait_for_timeout(800)
        except Exception:
            pass

        section = None
        for selector in (
            '.kcarSnb .el-collapse-item:has([aria-label="제조사/모델"]) .el-collapse-item__content .modelList',
            '.kcarSnb .modelList',
            '.el-collapse-item__content .modelList',
            '.modelList',
        ):
            try:
                loc = page.locator(selector).first
                loc.wait_for(state="visible", timeout=8000)
                section = page.locator(selector)
                if section.locator(".depth1").count() > 0:
                    break
            except Exception:
                continue
        if section is None:
            try:
                section = page.locator(".modelList")
                section.first.wait_for(state="visible", timeout=8000)
            except Exception:
                raise RuntimeError("제조사/모델 .modelList 영역을 찾을 수 없습니다.")

        depth1_blocks = section.locator(".depth1")
        depth1_count = depth1_blocks.count()
        if depth1_count == 0:
            logger.warning("제조사/모델 .depth1을 찾지 못했습니다.")
            return

        depth_1_labels = ("국산", "수입")

        def _count_depth2_direct(block):
            c = block.locator(".depth2 > ul > li").count()
            if c > 0:
                return c
            c = block.locator(".depth2 > li").count()
            if c > 0:
                return c
            return block.locator(".depth2 li").count()

        # 수입(두 번째 depth1) 블록이 접혀 있을 수 있으므로, 먼저 펼치기
        if depth1_count >= 2:
            try:
                depth1_block_수입 = depth1_blocks.nth(1)
                depth1_block_수입.scroll_into_view_if_needed()
                page.wait_for_timeout(500)
                for _ in range(3):
                    cnt = _count_depth2_direct(depth1_block_수입)
                    if cnt > 0:
                        break
                    try:
                        depth1_block_수입.locator("label").first.click()
                        page.wait_for_timeout(1500)
                    except Exception:
                        try:
                            page.locator("text=수입").first.click()
                            page.wait_for_timeout(1500)
                        except Exception:
                            pass
                    page.wait_for_timeout(800)
            except Exception as e:
                logger.warning("수입 섹션 펼치기 시도 중 오류(무시하고 진행): %s", e)

        # 총 브랜드(depth2) 개수 미리 계산 - 직계 li만 (국산·수입 동일)
        depth2_counts = []
        for idx in range(min(depth1_count, len(depth_1_labels))):
            depth2_counts.append(_count_depth2_direct(depth1_blocks.nth(idx)))
        total_brands = sum(depth2_counts)

        logger.info("============================================================")
        logger.info("K Car 제조사/모델(brand) 계층 데이터 수집 시작 (국산 → 수입)")
        logger.info("============================================================")
        logger.info("총 %d개 브랜드 데이터 수집 시작", total_brands)
        # CSV 파일 즉시 생성 (헤더만) → 진행 중에도 파일 존재·확인 가능
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=headers).writeheader()
        logger.info("CSV 생성됨: %s (데이터 수집 중 추가됩니다)", csv_path)
        for h in logger.handlers:
            h.flush()

        total_rows = 0
        start_index = 0
        model_sn_ref = [1]  # 1, 2, 3 ... 행마다 증가
        for idx in range(min(depth1_count, len(depth_1_labels))):
            depth_1_value = depth_1_labels[idx]  # 국산 또는 수입
            depth1_block = depth1_blocks.nth(idx)
            if depth2_counts[idx] == 0:
                if idx == 1:
                    try:
                        depth1_block.scroll_into_view_if_needed()
                        page.wait_for_timeout(1500)
                        depth2_counts[idx] = _count_depth2_direct(depth1_block)
                    except Exception:
                        pass
                if depth2_counts[idx] == 0:
                    logger.info("[%s] depth2 항목 없음, 스킵", depth_1_value)
                    continue
            if idx == 1:
                try:
                    depth1_block.scroll_into_view_if_needed()
                    page.wait_for_timeout(600)
                except Exception:
                    pass
            logger.info("------------------------------------------------------------")
            logger.info("[%s] 데이터 수집 시작 (총 %d개 브랜드)", depth_1_value, depth2_counts[idx])
            logger.info("------------------------------------------------------------")
            n = _collect_brand_depth1_block(
                page, depth1_block, depth_1_value, csv_path, headers, logger, model_sn_ref,
                start_index=start_index, total_brands=total_brands,
            )
            total_rows += n
            logger.info("[%s] 데이터 수집 완료: %d건", depth_1_value, n)
            start_index += depth2_counts[idx]

        logger.info("============================================================")
        logger.info("수집 완료! 파일: %s", csv_path)
        logger.info("총 수집 모델 수: %s개", f"{total_rows:,}")
    except Exception as e:
        logger.error("제조사/모델 수집 오류: %s", e, exc_info=True)


def _get_selected_car_type_name(page):
    """현재 선택된 차종(필터) 이름을 사이드/필터 영역에서 추출."""
    for sel in (
        '.kcarSnb [aria-label="차종"] .el-checkbox.is-checked .el-checkbox__label',
        '.kcarSnb .el-collapse-item:has([aria-label="차종"]) .el-checkbox.is-checked .el-checkbox__label',
        '.kcarSnb .menuItemList .el-checkbox.is-checked .el-checkbox__label',
        '.el-tag.el-tag--primary',  # 선택된 필터 태그
    ):
        try:
            el = page.locator(sel).first
            if el.count() and el.is_visible(timeout=500):
                return (el.inner_text() or "").strip()
        except Exception:
            continue
    return ""


# 테스트: True면 1페이지만 수집, False면 모든 페이지 페이징 수집
COLLECT_ONLY_FIRST_PAGE = True


def _get_kcar_car_type_labels(page):
    """차종 아코디언을 연 뒤, 차종 체크박스 라벨만 반환. (run_kcar_car_type_list와 동일하게 차종 12개만 대상)."""
    try:
        header = page.locator('[aria-label="차종"]').first
        if header.is_visible(timeout=2000):
            header.click()
            page.wait_for_timeout(1000)
    except Exception:
        pass
    labels = None
    # 1) 차종 섹션만: [aria-label="차종"] 하위의 체크박스만 (12개)
    try:
        section = page.locator(
            '.kcarSnb .el-collapse-item:has([aria-label="차종"]) .menuItemList .el-checkbox-group .el-checkbox'
        )
        section.first.wait_for(state="visible", timeout=3000)
        labels = section.locator(".el-checkbox__label")
        if labels.count() > 0:
            return labels
    except Exception:
        pass
    # 2) 첫 번째 .menuItemList만 사용 (차종이 첫 번째 메뉴이므로 12개, 전체 메뉴 합치면 184개가 됨 방지)
    try:
        first_menu = page.locator(".kcarSnb .menuItemList").first
        first_menu.wait_for(state="visible", timeout=3000)
        labels = first_menu.locator(".el-checkbox .el-checkbox__label")
        if labels.count() > 0:
            return labels
    except Exception:
        pass
    # 3) 차종 chkGroup 첫 번째만
    try:
        first_group = page.locator(".kcarSnb .el-checkbox-group.chkGroup").first
        first_group.wait_for(state="visible", timeout=3000)
        labels = first_group.locator(".el-checkbox .el-checkbox__label")
        if labels.count() > 0:
            return labels
    except Exception:
        pass
    return None


def _normalize_key(s):
    """매칭용: 공백 여러 개를 하나로, 앞뒤 공백 제거."""
    if not s:
        return ""
    return " ".join((s or "").split())


def _load_brand_list_lookup(result_dir: Path, brand_path: Path | None = None):
    """
    kcar_brand_list.csv를 읽어, car_name 매칭용 키 -> (brand_list, car_list, model_list, model_list_1, model_list_2) 딕셔너리 반환.
    list 페이지 car_name이 '기아 올 뉴 모닝 (JA) 럭셔리'처럼 car_list(모닝) 없이 나오므로,
    키 두 종류 등록: (1) brand+car+model+m1+m2 (2) brand+model+m1+m2 (car 제외).
    brand_path가 주어지면 해당 파일 사용, 없으면 result_dir/kcar_brand_list.csv 사용.
    """
    path = brand_path or result_dir / "kcar_brand_list.csv"
    lookup = {}
    if not path.exists():
        return lookup
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                brand = (row.get("brand_list") or "").strip()
                car = (row.get("car_list") or "").strip()
                model = (row.get("model_list") or "").strip()
                m1 = (row.get("model_list_1") or "").strip()
                m2 = (row.get("model_list_2") or "").strip()
                parts_full = [p for p in [brand, car, model, m1] if p]
                if m2 and m2 != "-":
                    parts_full.append(m2)
                # list 쪽 car_name은 보통 '브랜드 + model + model_list_1 + model_list_2' (car_list 없음)
                parts_no_car = [p for p in [brand, model, m1] if p]
                if m2 and m2 != "-":
                    parts_no_car.append(m2)
                value = (brand or "-", car or "-", model or "-", m1 or "-", m2 or "-")
                for parts in (parts_full, parts_no_car):
                    key = _normalize_key(" ".join(parts))
                    if key:
                        lookup[key] = value
    except Exception:
        pass
    return lookup


def _download_list_image(page, img_src: str, save_path: Path, logger) -> bool:
    """이미지 URL을 save_path에 다운로드. 성공 시 True."""
    if not img_src or not img_src.startswith("http"):
        return False
    try:
        response = page.request.get(img_src, timeout=15000)
        if response.ok:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_bytes(response.body())
            return True
    except Exception as e:
        logger.debug("리스트 이미지 다운로드 실패 %s: %s", img_src[:80], e)
    return False


def run_kcar_list(
    page,
    result_dir: Path,
    logger,
    csv_path: Path | None = None,
    brand_path: Path | None = None,
    save_dir: Path | None = None,
):
    """목록 수집. csv_path, brand_path, save_dir가 None이면 result_dir 기준 기본값 사용."""
    # result/kcar/kcar_list.csv (DOM: .resultCnt .carListWrap .carListBox 기준)
    csv_path = csv_path or result_dir / "kcar_list.csv"
    headers = [
        "model_sn", "product_id", "car_type_name", "brand_list", "car_list",
        "model_list", "model_list_1", "model_list_2",
        "car_name", "car_exp", "car_pay_meth", "release_dt", "car_navi", "car_fuel", "local_dos",
        "detail_url", "car_imgs",
        "date_crtr_pnttm", "create_dt",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow(headers)

    # 리스트 이미지 저장 경로 (save_dir 미지정 시 result_dir/imgs/list 사용)
    list_save_dir = save_dir or result_dir / "imgs" / "list"
    list_save_dir.mkdir(parents=True, exist_ok=True)

    brand_lookup = _load_brand_list_lookup(result_dir, brand_path=brand_path)
    if brand_lookup:
        logger.info("brand_list 매칭용 %d건 로드됨", len(brand_lookup))

    url = "https://www.kcar.com/bc/search"
    model_sn = 0
    # car_type_list와 동일 포맷: date_crtr_pnttm=YYYYMMDD, create_dt=YYYYMMDDHHMM
    now = datetime.now()
    base_date_crtr_pnttm = now.strftime("%Y%m%d")
    base_create_dt = now.strftime("%Y%m%d%H%M")

    try:
        logger.info("K Car 페이지 접속 중...")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)

        labels_locator = _get_kcar_car_type_labels(page)
        if not labels_locator:
            logger.error("차종 목록을 찾을 수 없습니다.")
            return

        n_car_types = labels_locator.count()
        logger.info(f"총 {n_car_types}개 차종 대상 수집 시작")

        for idx in range(n_car_types):
            try:
                current_labels = _get_kcar_car_type_labels(page)
                target_label = current_labels.nth(idx)
                car_type_name = target_label.inner_text().strip()

                logger.info(f"[{idx+1}/{n_car_types}] >>> {car_type_name} 선택")
                target_label.scroll_into_view_if_needed()
                target_label.click()
                page.wait_for_timeout(3000)
            except Exception as e:
                logger.warning(f"{idx}번째 차종 선택 실패: {e}")
                continue

            box_selector = ".resultCnt .carListWrap .carListBox"
            try:
                page.wait_for_selector(box_selector, timeout=10000)
                page.wait_for_timeout(500)
            except Exception:
                logger.warning(f"[{car_type_name}] 매물이 없거나 로딩되지 않았습니다.")
                try:
                    target_label.click()
                except Exception:
                    pass
                continue

            result_cnt = page.locator(".resultCnt").first
            boxes = result_cnt.locator(".carListWrap .carListBox")
            count = boxes.count()
            logger.info(f"   ㄴ 데이터 추출 중: {count}개 발견")

            with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                for i in range(count):
                    try:
                        box = boxes.nth(i)
                        # 광고 슬롯(.adBannerBox)은 carListBox로 감싸져 있어 27개 중 1개가 광고 → 제외
                        if box.locator(".adBannerBox").count() > 0:
                            continue
                        box.scroll_into_view_if_needed()

                        # A. product_id 및 리스트 이미지: .carListImg 내 #mkt_clickCar img 또는 .carListImg a img
                        product_id = ""
                        img_el = box.locator(".carListImg #mkt_clickCar img").first
                        if img_el.count() == 0:
                            img_el = box.locator(".carListImg a img").first
                        if img_el.count() == 0:
                            img_el = box.locator(".carListImg img").first
                        img_src = ""
                        if img_el.count() > 0:
                            img_src = img_el.get_attribute("src") or ""
                            product_id = _extract_product_id_from_img_src(img_src)
                        if not product_id:
                            link_el = box.locator("a[href*='/detail/']").first
                            if link_el.count() > 0:
                                product_id = _extract_product_id_from_href(link_el.get_attribute("href") or "")

                        # B. car_name: .detailInfo.srchTimedeal .carName .carTit
                        car_name = ""
                        name_el = box.locator(".detailInfo.srchTimedeal .carName .carTit").first
                        if name_el.count() == 0:
                            name_el = box.locator(".carName .carTit, .carTit").first
                        if name_el.count() > 0:
                            car_name = name_el.inner_text().strip()

                        # C. car_exp: .carListFlex .carExpIn .carExp
                        car_exp = ""
                        exp_el = box.locator(".carListFlex .carExpIn .carExp").first
                        if exp_el.count() > 0:
                            car_exp = exp_el.inner_text().strip()

                        # D. car_pay_meth: .carPayMeth li .el-link .el-link--inner 전체 텍스트 (할부 월 9만원 등)
                        car_pay_meth = ""
                        pay_el = box.locator(".carPayMeth li .el-link .el-link--inner").first
                        if pay_el.count() > 0:
                            car_pay_meth = pay_el.inner_text().strip()

                        # E. .carListFlex .detailCarCon span: 1=release_dt(block), 2=car_navi, 3=car_fuel, 4=local_dos
                        release_dt = ""
                        car_navi = ""
                        car_fuel = ""
                        local_dos = ""
                        detail_spans = box.locator(".carListFlex .detailCarCon span")
                        n_span = detail_spans.count()
                        if n_span >= 1:
                            release_dt = detail_spans.nth(0).inner_text().strip()
                        if n_span >= 2:
                            car_navi = detail_spans.nth(1).inner_text().strip()
                        if n_span >= 3:
                            car_fuel = detail_spans.nth(2).inner_text().strip()
                        if n_span >= 4:
                            local_dos = detail_spans.nth(3).inner_text().strip()

                        # F. .infoTooltip: 내용을 | 구분으로 저장
                        info_tooltip = ""
                        tip_el = box.locator(".infoTooltip").first
                        if tip_el.count() > 0:
                            info_tooltip = tip_el.inner_text().strip().replace("\n", " | ")

                        if product_id or car_name:
                            model_sn += 1
                            # list.csv에는 product_id를 EC 접두사 붙여서 저장 (예: EC61320372)
                            product_id_out = ("EC" + product_id) if product_id and not str(product_id).strip().upper().startswith("EC") else (product_id or "-")
                            # car_name으로 brand_list.csv 매칭 (brand_list, car_list, model_list, model_list_1, model_list_2)
                            brand_list_val = car_list_val = model_list_val = model_list_1_val = model_list_2_val = "-"
                            if car_name and brand_lookup:
                                key = _normalize_key(car_name)
                                matched = brand_lookup.get(key)
                                if matched:
                                    brand_list_val, car_list_val, model_list_val, model_list_1_val, model_list_2_val = matched
                            # 상세 URL: i_sCarCd=product_id
                            detail_url = "https://www.kcar.com/bc/detail/carInfoDtl?i_sCarCd=" + product_id_out if product_id_out != "-" else "-"
                            # 리스트 이미지 다운로드: {product_id_out}_list.png → car_imgs에 경로 저장
                            car_imgs_val = "-"
                            if product_id_out != "-":
                                list_img_path = list_save_dir / f"{product_id_out}_list.png"
                                if img_src and _download_list_image(page, img_src, list_img_path, logger):
                                    car_imgs_val = str(list_img_path)
                            row = [
                                model_sn, product_id_out, car_type_name, brand_list_val, car_list_val,
                                model_list_val, model_list_1_val, model_list_2_val,
                                car_name, car_exp, car_pay_meth, release_dt, car_navi, car_fuel, local_dos,
                                detail_url, car_imgs_val,
                                base_date_crtr_pnttm, base_create_dt,
                            ]
                            writer.writerow(row)

                    except Exception as sub_e:
                        continue

                f.flush()

            # 다음 차종을 위해 선택 해제
            try:
                # 해제 시에도 로케이터를 새로 잡는 것이 안전함
                _get_kcar_car_type_labels(page).nth(idx).click()
                page.wait_for_timeout(1500)
            except:
                pass

    except Exception as e:
        logger.error(f"전체 공정 중 치명적 오류: {e}", exc_info=True)
    
    logger.info(f"최종 수집 완료. 총 {model_sn}건이 {csv_path}에 저장되었습니다.")


def _extract_bg_image_url(style_str):
    """style 속성에서 background-image: url(...) URL 추출. &quot; 등 엔티티 포함."""
    if not style_str:
        return ""
    m = re.search(r'url\s*\(\s*["\']?([^"\')\s]+)["\']?\s*\)', style_str)
    if m:
        return m.group(1).replace("&quot;", "").strip()
    m = re.search(r'url\s*\(\s*&quot;([^&]+)&quot;\s*\)', style_str)
    if m:
        return m.group(1).strip()
    return ""


def _download_kcar_detail_gallery_images(page, ec_product_id, save_dir: Path, detail_url_base: str, logger):
    """
    상세 페이지 갤러리 이미지를 저장.
    K Car 상세는 .kaps-slider .kaps-item .kaps-img 의 style="background-image: url(...)" 로 이미지 표시.
    저장: save_dir / {ec_product_id}_1.png, {ec_product_id}_2.png, ...
    """
    if not ec_product_id or not save_dir:
        return 0
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    urls = []
    # 1) kaps 갤러리 (background-image 사용)
    try:
        items = page.locator(".kaps-slider .kaps-item .kaps-img, .kaps-extra .kaps-img, .kaps-img")
        n = items.count()
        if n > 0:
            for i in range(n):
                style = items.nth(i).get_attribute("style")
                src = _extract_bg_image_url(style or "")
                if src and not src.startswith("data:"):
                    urls.append(src)
    except Exception:
        pass
    if urls:
        urls = list(dict.fromkeys(urls))
    # 2) 기존 img src 방식 fallback
    if not urls:
        for selector in (
            ".carInfoWrap .carInfoGallery .el-carousel-thumbnail-item img",
            ".carInfoGallery img",
            ".carInfoKeyArea img",
        ):
            try:
                items = page.locator(selector)
                n = items.count()
                if n > 0:
                    for i in range(n):
                        src = items.nth(i).get_attribute("src")
                        if src and not src.startswith("data:"):
                            urls.append(src)
                    if urls:
                        urls = list(dict.fromkeys(urls))
                        break
            except Exception:
                continue
    if not urls:
        logger.warning("%s: 갤러리 이미지 0건 (선택자 불일치 가능)", ec_product_id)
        return 0
    saved = 0
    for idx, src in enumerate(urls, start=1):
        try:
            if src.startswith("//"):
                full_url = "https:" + src
            elif src.startswith("http"):
                full_url = src
            else:
                full_url = urljoin(detail_url_base, src)
            resp = page.request.get(full_url, timeout=15000)
            if resp.ok:
                # 원본이 .jpg면 .jpg로, 아니면 .png로 저장
                ext = ".jpg" if ".jpg" in src.split("?")[0].lower() else ".png"
                path = save_dir / f"{ec_product_id}_{idx}{ext}"
                path.write_bytes(resp.body())
                saved += 1
        except Exception as e:
            logger.warning("이미지 저장 실패 %s_%s: %s", ec_product_id, idx, e)
    if saved > 0:
        logger.info("%s 이미지 %d장 저장: %s", ec_product_id, saved, save_dir)
    return saved


def run_kcar_detail_images(page, result_dir: Path, logger, product_ids=None, limit=None):
    """
    kcar_list.csv의 product_id 목록으로 상세 페이지 접속 후 갤러리 이미지를
    imgs/kcar/{년}년/{YYYYMMDD}/{product_id}_1.png, _2.png ... 로 저장.
    product_ids를 넘기면 해당 목록만 사용; 없으면 result_dir/kcar_list.csv에서 product_id 컬럼 읽음.
    limit: 최대 처리 건수 (None이면 전체).
    """
    base_dir = Path(__file__).resolve().parent.parent / "imgs" / "kcar"
    now = datetime.now()
    save_dir = base_dir / f"{now.year}년" / now.strftime("%Y%m%d")
    save_dir.mkdir(parents=True, exist_ok=True)
    logger.info("이미지 저장 경로: %s", save_dir)

    if product_ids is None:
        list_path = result_dir / "kcar_list.csv"
        product_ids = []
        if list_path.exists():
            try:
                with open(list_path, "r", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        pid = (row.get("product_id") or "").strip()
                        if pid and pid != "-":
                            product_ids.append(pid)
                # 중복 제거 순서 유지
                seen = set()
                unique = []
                for p in product_ids:
                    if p not in seen:
                        seen.add(p)
                        unique.append(p)
                product_ids = unique
            except Exception as e:
                logger.error("kcar_list.csv 읽기 실패: %s", e)
        if not product_ids:
            logger.warning("수집할 product_id가 없습니다. kcar_list.csv를 먼저 생성하세요.")
            return

    if limit is not None:
        product_ids = product_ids[:limit]
    total = len(product_ids)
    logger.info("상세 이미지 수집 대상 %d건", total)
    # 상세 페이지 URL: carInfoDtl?i_sCarCd=EC{product_id}
    for i, pid in enumerate(product_ids, start=1):
        ec_id = "EC" + pid if not pid.startswith("EC") else pid
        url = f"https://www.kcar.com/bc/detail/carInfoDtl?i_sCarCd={ec_id}"
        success = False
        for attempt in range(3):
            try:
                page.goto(url, wait_until="load", timeout=60000)
                page.wait_for_timeout(4000)
                page.wait_for_selector(
                    ".carInfoWrap, .carInfoGallery, .kaps-slider, .kaps-extra, body img",
                    timeout=30000,
                )
                page.wait_for_timeout(2000)
                _download_kcar_detail_gallery_images(page, ec_id, save_dir, url, logger)
                success = True
                break
            except Exception as e:
                logger.warning(
                    "[%d/%d][시도 %d/3] %s 상세 페이지 로딩 실패: %s",
                    i,
                    total,
                    attempt + 1,
                    pid,
                    e,
                )
                page.wait_for_timeout(2000)
        if not success:
            logger.warning("[%d/%d] %s 상세 페이지 최종 실패, 다음 차량으로 이동", i, total, pid)
        if i % 50 == 0:
            logger.info("진행: %d/%d", i, total)


# --- Airflow Task 래퍼 함수 ---


def _run_kcar_car_type_csv(datst_cd: str) -> str:
    """차종 CSV 수집 Task (data11)."""
    activate_paths_for_datst((datst_cd or KCAR_DATST_CAR_TYPE).lower() or KCAR_DATST_CAR_TYPE)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    car_type_csv = RESULT_DIR / f"kcar_car_type_list_{run_ts}.csv"
    logger.info("차종 CSV 저장 경로: %s", car_type_csv)
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        try:
            run_kcar_car_type_list(page, RESULT_DIR, logger, csv_path=car_type_csv)
        finally:
            browser.close()
    return str(car_type_csv)


def _run_kcar_brand_csv(datst_cd: str) -> str:
    """브랜드 CSV 수집 Task (data10)."""
    activate_paths_for_datst((datst_cd or KCAR_DATST_BRAND).lower() or KCAR_DATST_BRAND)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    brand_csv = RESULT_DIR / f"kcar_brand_list_{run_ts}.csv"
    logger.info("브랜드 CSV 저장 경로: %s", brand_csv)
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        try:
            run_kcar_brand_list(page, RESULT_DIR, logger, csv_path=brand_csv)
        finally:
            browser.close()
    return str(brand_csv)


def run_kcar_list_job(
    bsc: BscInfo,
    *,
    brand_list_csv_path: str,
    car_type_csv_path: str | None = None,
) -> dict[str, Any]:
    """
    DB 메타 1건(목록 datst) 기준 K Car 목록·이미지 수집.
    브랜드 CSV는 앞단 Task에서 생성한 절대경로를 사용한다.
    """
    activate_paths_for_datst((bsc.datst_cd or KCAR_DATST_LIST).lower() or KCAR_DATST_LIST)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (IMG_BASE / "list").mkdir(parents=True, exist_ok=True)

    logger = _get_file_logger(run_ts)
    brand_path = Path(brand_list_csv_path)
    if not brand_path.is_file():
        raise FileNotFoundError(
            f"브랜드 CSV가 없습니다. run_brand_csv 태스크를 먼저 성공시키고 brand_list_csv_path를 확인하세요: {brand_path}"
        )
    if car_type_csv_path and not Path(car_type_csv_path).is_file():
        logger.warning("차종 CSV 경로 없음(무시하고 진행): %s", car_type_csv_path)

    logger.info("🏁 K Car 목록 수집 시작")
    logger.info("- pvsn_site_cd=%s, datst_cd=%s, dtst_nm=%s", bsc.pvsn_site_cd, bsc.datst_cd, bsc.dtst_nm)
    logger.info("- link_url=%s", bsc.link_url)
    logger.info("- 브랜드 CSV(이전 태스크): %s", brand_path)

    list_path = RESULT_DIR / f"kcar_list_{run_ts}.csv"
    if list_path.exists():
        list_path.unlink()
    list_img_save_dir = IMG_BASE / "list"

    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        try:
            run_kcar_list(
                page,
                RESULT_DIR,
                logger,
                csv_path=list_path,
                brand_path=brand_path,
                save_dir=list_img_save_dir,
            )
        finally:
            browser.close()

    count = 0
    if list_path.exists():
        with open(list_path, "r", encoding="utf-8-sig") as f:
            count = sum(1 for _ in csv.DictReader(f))
    logger.info("✅ K Car 목록 수집 완료: 총 %d건", count)
    return {
        "brand_csv": str(brand_path),
        "car_type_csv": car_type_csv_path or "",
        "list_csv": str(list_path),
        "count": count,
        "log_dir": str(LOG_DIR),
        "img_base": str(IMG_BASE),
    }


# --- Airflow DAG ---


@dag(
    dag_id="sdag_kcar_crawler",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["used_car", "kcar", "crawler", "day"],
)
def kcar_crawler_dag():
    """
    K Car 브랜드/차종/목록 수집 DAG.

    - DB 메타(std.tn_data_bsc_info, ps00004, data10/11/12) 조회
    - 차종 CSV → 브랜드 CSV → 목록/이미지 순, 태스크별 파일 생성 (리본카/헤이딜러 DAG와 동일 패턴)
    """

    @task
    def insert_collect_data_info() -> dict[str, dict[str, Any]]:
        """std.tn_data_bsc_info에서 K Car(ps00004) 수집 대상 기본 정보 조회."""
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        site = KCAR_PVSN_SITE_CD.lower().strip()
        sql = f"""
        SELECT * FROM std.tn_data_bsc_info tdbi
        WHERE 1=1
          AND LOWER(clct_yn) = 'y'
          AND LOWER(link_yn) = 'y'
          AND LOWER(pvsn_site_cd) = '{site}'
          AND LOWER(datst_cd) IN ('data10','data11','data12')
        ORDER BY data_sn
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

        missing = [k for k in ("data10", "data11", "data12") if k not in out]
        if missing:
            raise ValueError(
                f"std.tn_data_bsc_info 조회 결과 누락: {missing} (pvsn_site_cd={KCAR_PVSN_SITE_CD})"
            )
        return out

    @task
    def run_car_type_csv(infos: dict[str, dict[str, Any]]) -> str:
        """[STEP1] K Car 차종 CSV 생성 (kcar_car_type_list.csv)."""
        dc = str((infos.get(KCAR_DATST_CAR_TYPE) or {}).get("datst_cd") or KCAR_DATST_CAR_TYPE).lower()
        return _run_kcar_car_type_csv(dc)

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]]) -> str:
        """[STEP2] K Car 브랜드 CSV 생성 (kcar_brand_list.csv)."""
        dc = str((infos.get(KCAR_DATST_BRAND) or {}).get("datst_cd") or KCAR_DATST_BRAND).lower()
        return _run_kcar_brand_csv(dc)

    @task
    def run_list_csv(
        bsc_infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        car_type_csv_path: str,
    ) -> dict[str, Any]:
        """[STEP3] K Car 목록/이미지 수집 후 kcar_list.csv 생성."""
        bsc = BscInfo.from_row(bsc_infos[KCAR_DATST_LIST])
        return run_kcar_list_job(
            bsc,
            brand_list_csv_path=brand_csv_path,
            car_type_csv_path=car_type_csv_path or None,
        )

    infos = insert_collect_data_info()
    car_type_path = run_car_type_csv(infos)
    brand_path = run_brand_csv(infos)
    run_list_csv(infos, brand_path, car_type_path)


kcar_dag = kcar_crawler_dag()


# --- standalone 실행 (python -m dags.sdag_kcar_crawler) ---


def main():
    """standalone 실행 시 사용 (Airflow 외부)."""
    activate_paths_for_datst(KCAR_DATST_LIST)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    logger = _get_file_logger(run_ts)
    result_dir = RESULT_DIR
    result_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = _launch_browser(p, not HEADLESS_MODE)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        try:
            run_kcar_car_type_list(page, result_dir, logger)
            run_kcar_brand_list(page, result_dir, logger)
            run_kcar_list(page, result_dir, logger)
        finally:
            browser.close()


if __name__ == "__main__":
    main()
