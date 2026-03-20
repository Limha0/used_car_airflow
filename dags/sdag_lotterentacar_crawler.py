import csv
import json
import logging
import math
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from playwright.sync_api import sync_playwright


@dag(
    dag_id="sdag_lotterentacar_crawler",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["used_car", "lotterentacar", "crawler", "day"],
)
def lotterentacar_crawler_dag():
    """
    롯데렌터카 브랜드/차종/목록 수집 DAG.

    - DB 메타(std.tn_data_bsc_info, ps00006, data16/17/18) 조회
    - 차종 CSV → 브랜드 CSV → 목록/이미지 순, 태스크별 파일 생성
    """

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id="car_db_conn")

    @task
    def insert_collect_data_info(**kwargs) -> dict[str, dict[str, Any]]:
        """std.tn_data_bsc_info에서 롯데렌터카(ps00006) 수집 대상 기본 정보 조회."""
        select_bsc_info_stmt = f"""
        SELECT * FROM std.tn_data_bsc_info tdbi
        WHERE 1=1
          AND LOWER(clct_yn) = 'y'
          AND LOWER(link_yn) = 'y'
          AND LOWER(pvsn_site_cd) = 'ps00006'
          AND LOWER(datst_cd) IN ('data16','data17','data18')
        ORDER BY data_sn
        """
        logging.info("select_bsc_info_stmt ::: %s", select_bsc_info_stmt)
        conn = pg_hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(select_bsc_info_stmt)
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

        missing = [
            k
            for k in (
                LOTTERENTACAR_DATST_BRAND,
                LOTTERENTACAR_DATST_CAR_TYPE,
                LOTTERENTACAR_DATST_LIST,
            )
            if k not in out
        ]
        if missing:
            raise ValueError(
                f"std.tn_data_bsc_info 조회 결과 누락: {missing} (pvsn_site_cd={LOTTERENTACAR_PVSN_SITE_CD})"
            )
        return out

    @task
    def run_car_type_csv(infos: dict[str, dict[str, Any]]) -> str:
        """[STEP1] 롯데렌터카 차종 CSV 생성."""
        dc = str((infos.get(LOTTERENTACAR_DATST_CAR_TYPE) or {}).get("datst_cd") or LOTTERENTACAR_DATST_CAR_TYPE).lower()
        return _run_lotterentacar_car_type_csv(dc)

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]]) -> str:
        """[STEP2] 롯데렌터카 브랜드 CSV 생성."""
        dc = str((infos.get(LOTTERENTACAR_DATST_BRAND) or {}).get("datst_cd") or LOTTERENTACAR_DATST_BRAND).lower()
        return _run_lotterentacar_brand_csv(dc)

    @task
    def run_list_csv(
        bsc_infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        car_type_csv_path: str,
    ) -> dict[str, Any]:
        """[STEP3] 롯데렌터카 목록/이미지 수집 후 list CSV 생성."""
        bsc = BscInfo.from_row(bsc_infos[LOTTERENTACAR_DATST_LIST])
        return run_lotterentacar_list_job(
            bsc,
            brand_list_csv_path=brand_csv_path,
            car_type_csv_path=car_type_csv_path or None,
        )

    @task_group(group_id="create_csv_process")
    def create_csv_process(bsc_infos: dict[str, dict[str, Any]]) -> dict[str, Any]:
        """차종/브랜드/목록 CSV 생성 그룹."""
        car_type_path = run_car_type_csv(bsc_infos)
        brand_path = run_brand_csv(bsc_infos)
        return run_list_csv(bsc_infos, brand_path, car_type_path)

    infos = insert_collect_data_info()
    create_csv_process(infos)


_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

# Airflow Variable (기존 크롤러 DAG와 동일)
USED_CAR_SITE_NAMES_VAR = "used_car_site_names"
CRAWL_BASE_PATH_VAR = "crawl_base_path"

# DB `std.tn_data_bsc_info`: 롯데렌터카 제공사이트 코드
LOTTERENTACAR_PVSN_SITE_CD = "ps00006"
LOTTERENTACAR_DATST_BRAND = "data16"
LOTTERENTACAR_DATST_CAR_TYPE = "data17"
LOTTERENTACAR_DATST_LIST = "data18"
LOTTERENTACAR_SITE_NAME = "롯데렌터카"


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
    return LOTTERENTACAR_SITE_NAME


def get_lotterentacar_site_name() -> str:
    """
    롯데렌터카는 brand/data16, car_type/data17, list/data18가 모두 같은 사이트이므로
    datst_cd와 무관하게 동일한 폴더명을 사용한다.
    """
    for datst_cd in (
        LOTTERENTACAR_DATST_LIST,
        LOTTERENTACAR_DATST_BRAND,
        LOTTERENTACAR_DATST_CAR_TYPE,
    ):
        site_name = get_site_name_by_datst(datst_cd)
        if site_name and site_name.strip():
            return site_name
    return LOTTERENTACAR_SITE_NAME


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
    """롯데렌터카 공통 crawl / log / img 루트 경로 설정 (각 Task 시작 시 호출)."""
    global RESULT_DIR, LOG_DIR, IMG_BASE, YEAR_STR, DATE_STR, RUN_TS

    base = _get_crawl_base_path()
    site = get_lotterentacar_site_name()
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
    activate_paths_for_datst(LOTTERENTACAR_DATST_LIST)
except Exception:
    pass

HEADLESS_MODE = True


def _lotterentacar_list_img_dir(dt=None):
    """단독 실행: imgs/lotterentacar/list/<YYYY년>/<YYYYMMDD>."""
    now = dt or datetime.now()
    return _root / "imgs" / "lotterentacar" / "list" / f"{now.year}년" / now.strftime("%Y%m%d")

URL = "https://tcar.lotterentacar.net/cr/search/list#page:1._.rows:15._.saleTyAll:true"

# body-ctn-search-list > #wrap > #container > #reactMainPage > .layout--container.ctn-search-list >
# .search-list-container > .search-element > .search-condition > #carTypeField > .select-type-checked > li (내부 input name=chkCartype)
SELECTOR_LI = "#carTypeField .select-type-checked li[name='chkCartype']"
SELECTOR_LI_INPUT = "#carTypeField .select-type-checked li:has(input[name='chkCartype'])"
SELECTOR_LI_FALLBACK = ".select-type-checked li[name='chkCartype'], .select-type-checked li:has(input[name='chkCartype'])"
SELECTOR_CONTAINER = "#wrap #container #reactMainPage .search-list-container"

# 제조사/모델: .body-ctn-search-list #wrap #container #reactMainPage ... .search-condition .condition-item.item-model .select-type-checked.mnfc
# mnfc > li → label[for^="br"] = brand_list, 클릭 시 .select-type-checked.model > li = car_list
# car 클릭 시 .select-type-checked.dmodel > li = model_list, 그 하위 dmodel = model_list_1, 그 하위 = model_list_2
# 리프마다 1행: model_sn, brand_list, car_list, model_list, model_list_1, model_list_2
SELECTOR_BRAND_SECTION = ".search-condition .condition-item.item-model .select-type-checked.mnfc"
SELECTOR_MNFC_LI = f"{SELECTOR_BRAND_SECTION} > li"
SELECTOR_MODEL = "ul.select-type-checked.model"
SELECTOR_DMODEL = "ul.select-type-checked.dmodel"
# 직계 자식 ul 아래의 li만 참조 (중첩 단계 혼입 방지)
MODEL_CHILD_LI = "> ul.select-type-checked.model > li"
DMODEL_CHILD_LI = "> ul.select-type-checked.dmodel > li"

# 검색 결과 리스트: .list-element > .list-content > .list-type-vertical.n3 > [class*="list-item"]
SELECTOR_LIST_CONTAINER = ".list-element .list-content .list-type-vertical.n3"
SELECTOR_LIST_ITEM = ".list-type-vertical.n3 [class*='list-item']"
# 차종(경차, 소형차, 준중형차 등) 필터: #carTypeField .select-type-checked 내 li
SELECTOR_CAR_TYPE_OPTIONS = "#carTypeField .select-type-checked li[name='chkCartype']"
SELECTOR_CAR_TYPE_OPTIONS_FALLBACK = "#carTypeField .select-type-checked li:has(input[name='chkCartype'])"
# 차종별 목록 수집 시 차종당 건수 (테스트용)
LIST_PER_CAR_LIMIT = 5

# AJAX API (목록 수집용)
URL_OPTIONS = "https://tcar.lotterentacar.net/cr/search/ajax/options"
URL_LIST = "https://tcar.lotterentacar.net/cr/search/ajax/list"
LIST_DEFAULT_PARAMS = {
    "country": "", "orderType": "", "categoryGroup": '[""]',
    "minYear": "", "maxYear": "", "minMileage": "", "maxMileage": "",
    "minPrice": "", "maxPrice": "", "minInstPrice": "", "maxInstPrice": "",
    "minRentPrice": "", "maxRentPrice": "", "minSalePrice": "", "maxSalePrice": "",
    "instPriceMonth": "", "color": "", "inColor": "", "fuel": "",
    "checkedCenterCodes": "", "option": "", "carNumber": "", "keyword": "",
    "themeId": "", "themeType": "", "themeSaleType": "", "brandCert": "",
    "retailShopId": "", "dealerRegKey": "", "sellAdminKey": "", "cert": "",
    "manage": "", "promotion": "", "reqDirect": "", "consult": "", "rentBuy": "",
    "tagList": "", "sTagList": "", "saleTyAll": "true", "saleTyRent": "", "saleTySale": "",
    "perPageNum": "100",  # 서버가 한 응답에 전부 안 넣어주는 경우가 있어 페이지 단위로 나눠 요청
}
OPTIONS_DEFAULT_PARAMS = {
    "country": "", "minMileage": "", "maxMileage": "", "minYear": "", "maxYear": "",
    "minPrice": "", "maxPrice": "", "minRentPrice": "", "maxRentPrice": "",
    "minSalePrice": "", "maxSalePrice": "", "fuel": "", "option": "", "color": "",
    "checkedCenterCodes": "", "lotte": "", "cert": "", "manage": "", "promotion": "",
    "reqDirect": "", "consult": "", "rentBuy": "", "isSale": "A",
}
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://tcar.lotterentacar.net/cr/search/list",
    "Accept": "application/json",
}
# 대형차/SUV 등 데이터 많을 때 로딩 완료까지 대기 (초). 서버 응답이 느릴 수 있어 넉넉히 설정
LIST_API_TIMEOUT = 250

# 상세 페이지 URL (carId, saleTySale 또는 saleTyRent 쿼리)
URL_VIEW_BASE = "https://tcar.lotterentacar.net/cr/search/view"
# 상세 페이지 이미지: 여러 셀렉터 시도 (React/동적 로딩 대응)
SELECTOR_DETAIL_IMAGES = [
    ".car-image-list .image-list li img",
    ".image-list li img",
    ".car-image-list li img",
    "[class*='car-image-list'] [class*='image-list'] li img",
]
SELECTOR_DETAIL_IMAGE_CONTAINER = ".car-image-list, .image-list, [class*='image-list']"
DETAIL_PAGE_TIMEOUT = 20000

# 리스트 페이지 이미지 (프로모션 / 일반 목록)
# 프로모션: .list-element > .promotionSale-wrap > .car-wt-container > .list-content.wt.swiper-container > .list-type-vertical.swiper-wrapper.n5 > li > .list-thumb.type > img
SELECTOR_PROMO_WRAP = ".list-element .promotionSale-wrap"
SELECTOR_PROMO_ITEMS = ".promotionSale-wrap .car-wt-container [class*='list-content'][class*='swiper'] .list-type-vertical.swiper-wrapper.n5 li"
# 일반: .list-content > .list-type-vertical.n3 > .list-item > .car-thumb-slider.swiper-container > .swiper-wrapper > .swiper-slide > img
SELECTOR_LIST_ITEMS = ".list-element .list-content .list-type-vertical.n3 [class*='list-item']"
LIST_PAGE_TIMEOUT = 30000
# 목록 내 페이지네이션 (다음 페이지)
SELECTOR_NEXT_PAGE = "a[href*='page'], [class*='pagination'] a, button:has-text('다음'), a:has-text('다음'), [class*='next']"
MAX_PAGES_PER_CAR_TYPE = 100


def _normalize_text(text):
    """줄바꿈/여러 공백을 하나로."""
    return " ".join((text or "").strip().split())


def get_logger():
    """Airflow 기본 로깅 사용."""
    return logging.getLogger("lotterentacar")


def _get_file_logger(run_ts: str) -> logging.Logger:
    """
    Airflow 태스크 로그(UI)와 별개로, LOG_DIR에도 파일 로그를 남기기 위한 로거.
    - 파일: LOG_DIR/lotterentacar_crawler_{run_ts}.log
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("lotterentacar_crawler")
    logger.setLevel(logging.INFO)
    log_path = str(LOG_DIR / f"lotterentacar_crawler_{run_ts}.log")
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == log_path:
            return logger
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    return logger


def setup_logger():
    """standalone 실행 시 사용하는 파일 로거."""
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    return _get_file_logger(run_ts)


def _launch_browser(playwright, headless: bool):
    """WSL/Airflow 환경에서 Chromium을 조금 더 안정적으로 실행."""
    return playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--no-sandbox",
        ],
    )


def run_lotterentacar_car_type_list(page, result_dir: Path, logger, csv_path: Path | None = None):
    """
    롯데렌터카 T car 검색 페이지에서 차종 목록 수집.
    #carTypeField .select-type-checked 내 li[name="chkCartype"] 텍스트를
    car_type_name으로, car_type_sn은 1,2,3... 으로 저장.
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    if csv_path is None:
        csv_path = result_dir / "lotterentacar_car_type_list.csv"
    if csv_path.exists():
        csv_path.unlink()
    headers = ["car_type_sn", "car_type_name", "date_crtr_pnttm", "create_dt"]
    # append 모드: 행 단위로 쓰고 flush → 중간에 끊겨도 수집된 행까지 디스크에 남음
    need_header = True

    try:
        logger.info("====================롯데렌터카 차종 목록 수집 시작 ====================")
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)

        # React 영역 로드 대기
        try:
            page.wait_for_selector(SELECTOR_CONTAINER, timeout=15000)
            page.wait_for_timeout(2000)
        except Exception as e:
            logger.debug("컨테이너 대기 실패(무시 가능): %s", e)

        li_locator = None
        for selector in (SELECTOR_LI, SELECTOR_LI_INPUT, SELECTOR_LI_FALLBACK):
            try:
                loc = page.locator(selector)
                loc.first.wait_for(state="visible", timeout=10000)
                li_locator = loc
                break
            except Exception as e:
                logger.debug("셀렉터 실패 %s: %s", selector, e)
                continue

        if li_locator is None:
            logger.warning("차종 목록(li[name=chkCartype]) 요소를 찾지 못했습니다.")
            return

        n = li_locator.count()
        if n == 0:
            logger.warning("차종 li 개수가 0입니다.")
            return

        logger.info("총 %d개 차종 수집 시작", n)

        car_type_sn = 1
        with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            if need_header:
                w.writeheader()
                f.flush()
            for i in range(n):
                name = (li_locator.nth(i).inner_text() or "").strip()
                name = " ".join(name.split()) if name else ""
                if not name:
                    continue
                now = datetime.now()
                date_crtr_pnttm = now.strftime("%Y%m%d")
                create_dt = now.strftime("%Y%m%d%H%M")
                w.writerow({
                    "car_type_sn": car_type_sn,
                    "car_type_name": name,
                    "date_crtr_pnttm": date_crtr_pnttm,
                    "create_dt": create_dt,
                })
                f.flush()
                logger.info("[%d/%d] %s", car_type_sn, n, name)
                car_type_sn += 1

        logger.info("저장 완료: %s (총 %d건)", csv_path, car_type_sn - 1)
    except Exception as e:
        logger.error("차종 수집 오류: %s", e, exc_info=True)


def _get_label_text(locator_li):
    """li 로케이터 하위 label 텍스트 반환 (img 제외 텍스트만)."""
    try:
        label = locator_li.locator("label").first
        if label.count() == 0:
            return ""
        return _normalize_text(label.inner_text())
    except Exception:
        return ""


def _click_expand(page, locator_li, logger):
    """펼치기용 클릭: 해당 li의 직계 자식 p.icon_toggle(화살표) 먼저, 없으면 label 시도.
    HTML: li > input, label, var, ul?, p.icon_toggle → 직계 자식만 클릭해야 중첩 li 화살표와 혼동 안 함."""
    for _ in range(2):
        try:
            locator_li.scroll_into_view_if_needed(timeout=3000)
            page.wait_for_timeout(200)
            # 직계 자식 p.icon_toggle만 클릭 (중첩된 하위 li의 화살표 제외)
            toggle = locator_li.locator("> p.icon_toggle")
            if toggle.count() > 0 and toggle.first.is_visible(timeout=1000):
                toggle.first.click()
                page.wait_for_timeout(450)
                return True
        except Exception as e:
            logger.debug("p.icon_toggle 클릭 실패: %s", e)
        try:
            lbl = locator_li.locator("> label").first
            if lbl.count() > 0:
                lbl.click()
                page.wait_for_timeout(450)
                return True
        except Exception as e:
            logger.debug("label 클릭 실패: %s", e)
        page.wait_for_timeout(300)
    return False


def _ensure_children_visible(page, locator_li, child_li_selector: str, logger, timeout_ms: int = 8000) -> bool:
    """이미 펼쳐져 있으면 클릭하지 않고, 비어있을 때만 펼치기 클릭 후 하위 li가 나타날 때까지 대기."""
    try:
        if locator_li.locator(child_li_selector).count() > 0:
            return True
    except Exception:
        pass

    if not _click_expand(page, locator_li, logger):
        return False

    try:
        locator_li.locator(child_li_selector).first.wait_for(state="visible", timeout=timeout_ms)
        return True
    except Exception:
        return locator_li.locator(child_li_selector).count() > 0


def run_lotterentacar_brand_list(page, result_dir: Path, logger, csv_path: Path | None = None):
    """
    롯데렌터카 제조사/모델 트리 수집 → lotterentacar_brand_list.csv
    - brand_list: .select-type-checked.mnfc > li 내 label[for^="br"] 텍스트
    - car_list: .select-type-checked.model > li (클릭 후 표시)
    - model_list: .select-type-checked.dmodel > li (car 클릭 후)
    - model_list_1: 그 하위 .select-type-checked.dmodel > li
    - model_list_2: 그 하위 .select-type-checked.dmodel > li
    리프 경로마다 1행 (예: 기아, K3, 더 뉴 K3, 1.6 가솔린, 트렌디).
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    if csv_path is None:
        csv_path = result_dir / "lotterentacar_brand_list.csv"
    if csv_path.exists():
        csv_path.unlink()
    headers = [
        "model_sn", "brand_list", "car_list", "model_list",
        "model_list_1", "model_list_2", "date_crtr_pnttm", "create_dt",
    ]
    need_header = True
    model_sn = 0

    def write_row(f, w, brand_list, car_list, model_list, model_list_1, model_list_2):
        nonlocal model_sn, need_header
        model_sn += 1
        now = datetime.now()
        row = {
            "model_sn": model_sn,
            "brand_list": brand_list or "",
            "car_list": car_list or "",
            "model_list": model_list or "",
            "model_list_1": model_list_1 or "",
            "model_list_2": model_list_2 or "",
            "date_crtr_pnttm": now.strftime("%Y%m%d"),
            "create_dt": now.strftime("%Y%m%d%H%M"),
        }
        if need_header:
            w.writeheader()
            need_header = False
        w.writerow(row)
        f.flush()

    try:
        logger.info("==================== 롯데렌터카 브랜드/차종/모델 목록 수집 시작 ====================")
        if URL not in (page.url or ""):
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        try:
            page.wait_for_selector(SELECTOR_MNFC_LI, timeout=15000)
            page.wait_for_timeout(1500)
        except Exception as e:
            logger.warning("브랜드 영역(ul.select-type-checked.mnfc > li) 대기 실패: %s", e)
            return

        brand_lis = page.locator(SELECTOR_MNFC_LI)
        brand_count = brand_lis.count()
        if brand_count == 0:
            logger.warning("브랜드 li가 0개입니다.")
            return
        logger.info("브랜드 %d개 탐색 시작", brand_count)

        with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)

            for i in range(brand_count):
                try:
                    brand_li = page.locator(SELECTOR_MNFC_LI).nth(i)
                    # brand_list = label[for^="br"] 텍스트 (예: 현대, 기아)
                    brand_name = ""
                    if brand_li.locator("label[for^='br']").count() > 0:
                        brand_name = _normalize_text(brand_li.locator("label[for^='br']").first.inner_text())
                    if not brand_name:
                        brand_name = _get_label_text(brand_li)
                    if not brand_name:
                        continue
                    logger.info("[브랜드 %d/%d] %s", i + 1, brand_count, brand_name)
                    # 브랜드 하위 차종 펼치기: 너무 오래 대기하지 않도록 타임아웃 축소
                    _ensure_children_visible(page, brand_li, MODEL_CHILD_LI, logger, timeout_ms=5000)
                    page.wait_for_timeout(300)
                except Exception as e:
                    logger.debug("브랜드 %d 클릭/이름 실패: %s", i, e)
                    continue

                car_lis = page.locator(SELECTOR_MNFC_LI).nth(i).locator(MODEL_CHILD_LI)
                car_count = car_lis.count()
                if car_count == 0:
                    write_row(f, w, brand_name, "", "", "", "")
                    continue

                for j in range(car_count):
                    try:
                        car_li = page.locator(SELECTOR_MNFC_LI).nth(i).locator(MODEL_CHILD_LI).nth(j)
                        car_name = _get_label_text(car_li)
                        if not car_name:
                            continue
                        # 차종 클릭 후 직계 자식 dmodel(더 뉴아이오닉 5 등) 노출
                        _ensure_children_visible(page, car_li, DMODEL_CHILD_LI, logger, timeout_ms=5000)
                        page.wait_for_timeout(250)

                    except Exception as e:
                        logger.debug("차종 %d 클릭/이름 실패: %s", j, e)
                        continue

                    # 직계 자식 ul.dmodel > li 만 사용 (model_list: 더 뉴아이오닉 5 등)
                    d1_lis = page.locator(SELECTOR_MNFC_LI).nth(i).locator(MODEL_CHILD_LI).nth(j).locator(DMODEL_CHILD_LI)
                    d1_count = d1_lis.count()
                    if d1_count == 0:
                        logger.debug("[%s > %s] dmodel li 0개 → 1건만 기록", brand_name, car_name)
                        write_row(f, w, brand_name, car_name, "", "", "")
                        continue
                    logger.info("[%s > %s] model_list %d개", brand_name, car_name, d1_count)

                    for k in range(d1_count):
                        try:
                            d1_li = page.locator(SELECTOR_MNFC_LI).nth(i).locator(MODEL_CHILD_LI).nth(j).locator(DMODEL_CHILD_LI).nth(k)
                            model_list_val = _get_label_text(d1_li)
                        except Exception:
                            model_list_val = ""
                        # model_list(더 뉴아이오닉 5 등) 클릭 후 직계 자식 dmodel(model_list_1) 노출
                        _ensure_children_visible(page, d1_li, DMODEL_CHILD_LI, logger, timeout_ms=4000)
                        page.wait_for_timeout(200)
                        d2_lis = d1_li.locator(DMODEL_CHILD_LI)
                        d2_count = d2_lis.count()
                        if d2_count == 0:
                            write_row(f, w, brand_name, car_name, model_list_val, "", "")
                            continue
                        for k2 in range(d2_count):
                            try:
                                d2_li = d1_li.locator(DMODEL_CHILD_LI).nth(k2)
                                model_list_1_val = _get_label_text(d2_li)
                            except Exception:
                                model_list_1_val = ""
                            # model_list_1(EV 2WD 등) 클릭 후 직계 자식 dmodel(model_list_2) 노출 (없을 수도 있음)
                            _ensure_children_visible(page, d2_li, DMODEL_CHILD_LI, logger, timeout_ms=3000)
                            page.wait_for_timeout(150)
                            d3_lis = d2_li.locator(DMODEL_CHILD_LI)
                            d3_count = d3_lis.count()
                            if d3_count == 0:
                                write_row(f, w, brand_name, car_name, model_list_val, model_list_1_val, "")
                                continue
                            for k3 in range(d3_count):
                                try:
                                    d3_li = d2_li.locator(DMODEL_CHILD_LI).nth(k3)
                                    model_list_2_val = _get_label_text(d3_li)
                                except Exception:
                                    model_list_2_val = ""
                                write_row(f, w, brand_name, car_name, model_list_val, model_list_1_val, model_list_2_val)

                # 다음 브랜드로 가기 전 현재 브랜드 접기(같은 li 다시 클릭)
                try:
                    page.locator(SELECTOR_MNFC_LI).nth(i).locator("label").first.click()
                    page.wait_for_timeout(300)
                except Exception:
                    pass

        logger.info("저장 완료: %s (총 %d건)", csv_path, model_sn)
    except Exception as e:
        logger.error("브랜드 목록 수집 오류: %s", e, exc_info=True)


def _model_key(*parts):
    """model_list + model_list_1 + model_list_2 또는 list_title + list_title_1 을 공백으로 이어서 정규화."""
    return _normalize_text(" ".join((p or "").strip() for p in parts if (p or "").strip()))


def _load_brand_model_mapping(result_dir: Path, logger, brand_path: Path | None = None):
    """
    lotterentacar_brand_list.csv에서 model_list+model_list_1+model_list_2(공백 연결) 키 → (brand_list, car_list) 매핑.
    """
    if brand_path is None:
        brand_path = result_dir / "lotterentacar_brand_list.csv"
    mapping = {}
    if not brand_path.exists():
        logger.debug("브랜드 CSV 없음: %s", brand_path)
        return mapping
    try:
        with open(brand_path, "r", encoding="utf-8-sig") as f:
            r = csv.DictReader(f)
            for row in r:
                key = _model_key(
                    row.get("model_list"),
                    row.get("model_list_1"),
                    row.get("model_list_2"),
                )
                if not key:
                    continue
                mapping[key] = (row.get("brand_list") or "", row.get("car_list") or "")
        logger.info("브랜드 매핑 %d건 로드 (list 매칭용)", len(mapping))
    except Exception as e:
        logger.warning("브랜드 CSV 로드 실패: %s", e)
    return mapping


def _extract_list_row(it, model_sn: int, brand_list: str, car_list: str):
    """리스트 아이템 locator(it)에서 한 행 dict 추출. brand_list/car_list는 인자로 넣음.
    .list-element > .list-content > .list-type-vertical.n3 > [class*='list-item'] > a.list-detail > .list-title 텍스트를 car_name으로 사용.
    """
    row = {"model_sn": model_sn, "brand_list": brand_list or "", "car_list": car_list or ""}
    try:
        title_el = it.locator(".list-detail .list-title").first
        if title_el.count() > 0:
            raw = (title_el.inner_text() or "").strip()
            parts = [p.strip() for p in raw.split("\n") if p.strip()]
            row["list_title"] = parts[0] if parts else ""
            row["list_title_1"] = parts[1] if len(parts) > 1 else ""
            row["car_name"] = _normalize_text(raw) if raw else ""
        else:
            row["list_title"] = row["list_title_1"] = row["car_name"] = ""
    except Exception:
        row["list_title"] = row["list_title_1"] = row["car_name"] = ""
    try:
        row["year"] = _normalize_text(it.locator(".list-option .year").first.inner_text()) if it.locator(".list-option .year").count() > 0 else ""
    except Exception:
        row["year"] = ""
    try:
        row["dstc"] = _normalize_text(it.locator(".list-option .dstc").first.inner_text()) if it.locator(".list-option .dstc").count() > 0 else ""
    except Exception:
        row["dstc"] = ""
    try:
        row["fuel"] = _normalize_text(it.locator(".list-option .fuel").first.inner_text()) if it.locator(".list-option .fuel").count() > 0 else ""
    except Exception:
        row["fuel"] = ""
    price_scope = it.locator("[class*='list-price']")
    try:
        disc = price_scope.locator(".discount").first
        row["discount"] = _normalize_text(disc.inner_text()) if disc.count() > 0 else ""
    except Exception:
        row["discount"] = ""
    try:
        pi = it.locator(".price-installment.flex.align-items-center, [class*='price-installment']").first
        row["price_installment"] = _normalize_text(pi.inner_text()) if pi.count() > 0 else "-"
    except Exception:
        row["price_installment"] = "-"
    try:
        price2 = it.locator("[class*='list-price'] .price .price2.block, .list-price .price .price2.block")
        row["price2_block"] = _normalize_text(price2.first.inner_text()) if price2.count() > 0 else "-"
    except Exception:
        row["price2_block"] = "-"
    try:
        row["prc"] = _normalize_text(it.locator("[class*='list-price'] .prc, .list-price .prc").first.inner_text()) if it.locator("[class*='list-price'] .prc, .list-price .prc").count() > 0 else ""
    except Exception:
        row["prc"] = ""
    try:
        row["em"] = _normalize_text(it.locator("[class*='list-price'] .type em, .list-price .type em").first.inner_text()) if it.locator("[class*='list-price'] .type em, .list-price .type em").count() > 0 else ""
    except Exception:
        row["em"] = ""
    try:
        row["price_new"] = _normalize_text(it.locator("[class*='list-price'] .price-new, .list-price .price-new").first.inner_text()) if it.locator("[class*='list-price'] .price-new, .list-price .price-new").count() > 0 else ""
    except Exception:
        row["price_new"] = ""
    try:
        badge_spans = it.locator(".badge span")
        cnt = badge_spans.count()
        if cnt > 0:
            parts = [_normalize_text(badge_spans.nth(j).inner_text()) for j in range(cnt)]
            row["badge"] = "|".join(p for p in parts if p)
        else:
            row["badge"] = ""
    except Exception:
        row["badge"] = ""
    now = datetime.now()
    row["date_crtr_pnttm"] = now.strftime("%Y%m%d")
    row["create_dt"] = now.strftime("%Y%m%d%H%M")
    return row


def _fetch_car_type_list_from_options(logger):
    """options API에서 carTypeList 조회 → cd → cdNm(경차, 소형차...) 매핑 반환."""
    cd_to_name = {}
    try:
        params = {**OPTIONS_DEFAULT_PARAMS, "carType": '[""]', "_": str(int(time.time() * 1000))}
        r = requests.get(URL_OPTIONS, params=params, headers=REQUEST_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        result = data.get("result") or data
        car_type_list = result.get("carTypeList") or []
        for item in car_type_list:
            cd = (item.get("cd") or "").strip()
            cd_nm = (item.get("cdNm") or "").strip()
            if cd:
                cd_to_name[cd] = cd_nm
        logger.info("options API carTypeList %d건 로드", len(cd_to_name))
    except Exception as e:
        logger.warning("options API 실패: %s", e)
    return cd_to_name


def run_lotterentacar_list_via_ajax(result_dir: Path, logger, list_csv_path: Path | None = None):
    """
    AJAX list/options API로 차종별 목록 수집 → lotterentacar_list.csv
    - options에서 carTypeList(cd→경차/소형차...) 획득
    - 각 cd별 list API 호출 시 promotionListData가 있으면 먼저 같은 CSV에 기록( model_sn 연번 ), 이어서 data 수집
    - CSV: model_sn, product_id, car_type, ..., poststart_dt, postend_dt, detail_url, car_imgs, date_crtr_pnttm, create_dt
    - detail_url: car_imgs 앞에 추가. saleType S(구매) → view?carId={product_id}&saleTySale=true, R(렌트) → view?carId={product_id}&saleTyRent=true
    - car_imgs: date_crtr_pnttm 앞에 추가, 목록 생성 시에는 빈값. 상세 이미지 저장 시 imgs/lotterentcar/년도/년월일 경로가 해당 행에 채워짐
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = list_csv_path if list_csv_path is not None else (result_dir / "lotterentacar_list.csv")
    if csv_path.exists():
        csv_path.unlink()
    headers = [
        "model_sn", "product_id", "car_type", "brand_list", "car_list", "model_list",
        "model_list_1", "model_list_2", "car_name", "release_dt", "car_navi", "fuel",
        "promotion_price", "return_price", "price_new", "sale_type",
        "poststart_dt", "postend_dt",
        "detail_url",
        "car_imgs",
        "date_crtr_pnttm", "create_dt",
    ]
    cd_to_name = _fetch_car_type_list_from_options(logger)
    if not cd_to_name:
        logger.warning("차종 목록(cd)이 비어 있어 list API 호출 시 cd를 그대로 사용합니다.")

    model_sn = 0
    api_total_sum = 0  # 차종별 recordsFiltered + recordsPromotionFiltered 합계 (비교용)
    last_car_total_count = None  # 응답의 carTotalCount (비교용)
    shortfall_log = []  # (car_type, cd, expected, collected, shortfall, reason)
    now = datetime.now()
    date_crtr_pnttm = now.strftime("%Y%m%d")
    create_dt = now.strftime("%Y%m%d%H%M")
    sale_type_map = {"R": "렌트", "S": "구매"}

    try:
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()

            for cd, car_type_name in cd_to_name.items():
                list_params = {
                    **LIST_DEFAULT_PARAMS,
                    "carType": '["' + cd + '"]',
                    "shuffleKey": "1",
                    "page": "1",
                    "_": str(int(time.time() * 1000)),
                }
                try:
                    r = requests.get(URL_LIST, params=list_params, headers=REQUEST_HEADERS, timeout=LIST_API_TIMEOUT)
                    r.raise_for_status()
                    body = r.json()
                except Exception as e:
                    logger.warning("[%s] list API 1페이지 실패: %s", car_type_name or cd, e)
                    continue
                result = body.get("result") or body
                records_filtered = int(result.get("recordsFiltered") or 0)
                records_promotion_filtered = int(result.get("recordsPromotionFiltered") or 0)
                total_count = records_filtered + records_promotion_filtered
                api_total_sum += total_count
                last_car_total_count = result.get("carTotalCount")  # 마지막 응답의 carTotalCount (전체 사이트 기준일 수 있음)
                data_list = result.get("data") or []
                promotion_list = result.get("promotionListData") or []
                if total_count == 0 and not data_list and not promotion_list:
                    continue
                per_page = int(list_params.get("perPageNum", 999))
                total_pages = math.ceil(total_count / per_page) if total_count else 1
                promotion_received = len(promotion_list)
                if promotion_received != records_promotion_filtered:
                    logger.warning("[%s] recordsPromotionFiltered(%d)와 1페이지 수신 promotion 건수(%d) 불일치 → 수집 목표를 실제 수신 기준으로 조정", car_type_name or cd, records_promotion_filtered, promotion_received)
                    total_count = records_filtered + promotion_received
                    total_pages = math.ceil(total_count / per_page) if total_count else 1
                logger.info("[%s] recordsFiltered=%d, recordsPromotionFiltered=%d, 총=%d, 총 %d페이지 (1페이지 수신 promotion %d건)", car_type_name or cd, records_filtered, records_promotion_filtered, total_count, total_pages, promotion_received)

                def _item_to_row(it, car_type_val):
                    product_id = (it.get("carId") or "")
                    saletype = (it.get("saletype") or "").strip()
                    if saletype == "S":
                        detail_url = f"{URL_VIEW_BASE}?carId={product_id}&saleTySale=true" if product_id else ""
                    else:
                        detail_url = f"{URL_VIEW_BASE}?carId={product_id}&saleTyRent=true" if product_id else ""
                    return {
                        "model_sn": model_sn,
                        "product_id": product_id,
                        "car_type": car_type_val,
                        "brand_list": (it.get("brandName") or "").strip(),
                        "car_list": (it.get("modelgroupName") or "").strip(),
                        "model_list": (it.get("modelName") or "").strip(),
                        "model_list_1": (it.get("gradeName") or "").strip(),
                        "model_list_2": (it.get("subgradeName") or "").strip() if it.get("subgradeName") else "",
                        "car_name": (
                            (it.get("carName") or it.get("listTitle") or it.get("title") or "").strip()
                            or _model_key(it.get("brandName"), it.get("modelName"), it.get("gradeName"), it.get("subgradeName"))
                        ),
                        "release_dt": (it.get("regDate") or "").strip(),
                        "car_navi": (it.get("mileage") or "").strip() if it.get("mileage") is not None else "",
                        "fuel": (it.get("fuel") or "").strip(),
                        "promotion_price": it.get("promotionPrice") if it.get("promotionPrice") is not None else "",
                        "return_price": it.get("returnPrice") if it.get("returnPrice") is not None else "",
                        "price_new": it.get("priceNew") if it.get("priceNew") is not None else "",
                        "sale_type": sale_type_map.get(saletype, saletype),
                        "poststart_dt": (it.get("postStartDt") or "").strip() if it.get("postStartDt") else "",
                        "postend_dt": (it.get("postEndDt") or "").strip() if it.get("postEndDt") else "",
                        "detail_url": detail_url,
                        "car_imgs": "",
                        "date_crtr_pnttm": date_crtr_pnttm,
                        "create_dt": create_dt,
                    }

                car_type_val = car_type_name or cd
                collected_this_type = 0
                total_promotion_skipped = 0  # promotionListData 중 dict가 아닌 항목 수 (스킵)
                page_no = 1
                break_reason = None  # 부족 시 원인
                page_error_msg = None
                while True:
                    if page_no > 1:
                        list_params["page"] = str(page_no)
                        list_params["_"] = str(int(time.time() * 1000))
                        try:
                            r = requests.get(URL_LIST, params=list_params, headers=REQUEST_HEADERS, timeout=LIST_API_TIMEOUT)
                            r.raise_for_status()
                            body = r.json()
                            result = body.get("result") or body
                            data_list = result.get("data") or []
                            promotion_list = []  # 2페이지부터는 프로모션 제외(1페이지에서만 반영, 중복 수집 방지)
                        except Exception as e:
                            page_error_msg = str(e)
                            logger.warning("[%s] list API page=%d 실패: %s", car_type_name or cd, page_no, e)
                            break_reason = "page%d 요청 실패: %s" % (page_no, page_error_msg)
                            break
                    # 1) 1페이지에서만: promotionListData가 있으면 먼저 수집, 없으면 data로 바로 진행
                    if page_no == 1 and promotion_list:
                        total_promotion_skipped += sum(1 for x in promotion_list if not isinstance(x, dict))
                        for item in promotion_list:
                            model_sn += 1
                            collected_this_type += 1
                            if isinstance(item, dict):
                                row = _item_to_row(item, car_type_val)
                            else:
                                row = {k: "" for k in headers}
                                row["model_sn"] = model_sn
                                row["car_type"] = car_type_val
                            row["model_sn"] = model_sn
                            w.writerow(row)
                            f.flush()
                    # 2) data 수집 (1페이지: 프로모션 다음에, 2페이지~: data만 수집)
                    for item in data_list:
                        model_sn += 1
                        collected_this_type += 1
                        row = _item_to_row(item, car_type_val)
                        row["model_sn"] = model_sn
                        w.writerow(row)
                        f.flush()
                    got = (len(promotion_list) if page_no == 1 else 0) + len(data_list)
                    promo_cnt = len(promotion_list) if page_no == 1 else 0
                    data_cnt = len(data_list)
                    page_total = promo_cnt + data_cnt
                    logger.info("[%s] page=%d 프로모션 %d건, data %d건 → 당페이지 %d건 (누적 %d/%d)", car_type_val, page_no, promo_cnt, data_cnt, page_total, collected_this_type, total_count)
                    if collected_this_type >= total_count:
                        break
                    if got == 0:
                        break_reason = "page%d에서 API가 빈 목록 반환 (이후 페이지 없음 또는 API 한계)" % page_no
                        break
                    page_no += 1
                    if page_no > total_pages:
                        break_reason = "총 %d페이지까지 수집했으나 API가 반환한 건수(%d)가 recordsFiltered+recordsPromotionFiltered(%d)보다 적음" % (total_pages, collected_this_type, total_count)
                        if total_promotion_skipped > 0:
                            break_reason += " (원인: promotionListData 중 객체(dict)가 아닌 항목 %d건 스킵됨)" % total_promotion_skipped
                        break
                    time.sleep(0.2)
                if collected_this_type != total_count:
                    shortfall = total_count - collected_this_type
                    reason = break_reason or "원인 미상"
                    if total_promotion_skipped > 0 and "스킵" not in reason:
                        reason += " (promotionListData 비객체 %d건 스킵)" % total_promotion_skipped
                    shortfall_log.append((car_type_val, cd, total_count, collected_this_type, shortfall, reason))
                    logger.warning("[%s] 수집 %d건 (API 총 %d건) - 부족분 %d | 원인: %s", car_type_name or cd, collected_this_type, total_count, shortfall, reason)
                time.sleep(0.2)
        logger.info(
            "저장 완료: %s (총 %d건, API 차종별 합계 recordsFiltered+recordsPromotionFiltered=%d건, carTotalCount=%s)",
            csv_path, model_sn, api_total_sum, last_car_total_count if last_car_total_count is not None else "N/A"
        )
    except Exception as e:
        logger.error("AJAX 목록 수집 오류: %s", e, exc_info=True)


def run_lotterentacar_fetch_detail_images(result_dir: Path, logger, list_csv_path: Path | None = None, img_dir: Path | None = None):
    """
    lotterentacar_list.csv를 읽어 각 행의 product_id, sale_type으로 상세 페이지 접속 후
    이미지를 imgs/lotterentcar/년도/년월일/product_id_1.png 형태로 저장.
    이미지가 저장된 행마다 car_imgs 컬럼( date_crtr_pnttm 앞 )에 경로( imgs/lotterentcar/년도/년월일 )를 append 후 CSV 재저장.
    - sale_type 구매 → view?carId=...&saleTySale=true
    - sale_type 렌트 → view?carId=...&saleTyRent=true
    """
    csv_path = list_csv_path if list_csv_path is not None else (result_dir / "lotterentacar_list.csv")
    if not csv_path.exists():
        logger.warning("목록 CSV 없음: %s", csv_path)
        return
    now = datetime.now()
    year_str = f"{now.year}년"
    date_str = now.strftime("%Y%m%d")
    if img_dir is None:
        img_dir = _root / "imgs" / "lotterentacar" / year_str / date_str
        car_imgs_value = f"imgs/lotterentacar/{year_str}/{date_str}"
    else:
        img_dir = Path(img_dir)
        car_imgs_value = str(img_dir)
    img_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        idx = headers.index("date_crtr_pnttm") if "date_crtr_pnttm" in headers else len(headers)
        if "car_imgs" not in headers:
            headers.insert(idx, "car_imgs")
        if "detail_url" not in headers:
            headers.insert(idx, "detail_url")
        for row in reader:
            rows.append(row)

    seen = set()  # (product_id, sale_type) 이미 처리한 경우 스킵
    logger.info("상세 페이지 이미지 저장 시작 (총 %d건, 저장 경로: %s)", len(rows), img_dir)

    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        try:
            context = browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent=REQUEST_HEADERS.get("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
            )
            page = context.new_page()
            for i, row in enumerate(rows):
                product_id = (row.get("product_id") or "").strip()
                sale_type = (row.get("sale_type") or "").strip()
                if not product_id:
                    row["car_imgs"] = ""
                    continue
                if not row.get("detail_url") and product_id:
                    row["detail_url"] = (
                        f"{URL_VIEW_BASE}?carId={product_id}&saleTySale=true"
                        if sale_type == "구매"
                        else f"{URL_VIEW_BASE}?carId={product_id}&saleTyRent=true"
                    )
                key = (product_id, sale_type)
                if key in seen:
                    row["car_imgs"] = car_imgs_value
                    continue
                if sale_type == "구매":
                    url = f"{URL_VIEW_BASE}?carId={product_id}&saleTySale=true"
                else:
                    url = f"{URL_VIEW_BASE}?carId={product_id}&saleTyRent=true"
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=DETAIL_PAGE_TIMEOUT)
                    # 이미지 영역 로드 대기 (React/동적 렌더)
                    try:
                        page.wait_for_selector(SELECTOR_DETAIL_IMAGE_CONTAINER, timeout=10000)
                    except Exception:
                        pass
                    page.wait_for_timeout(2500)
                    imgs = None
                    for sel in SELECTOR_DETAIL_IMAGES:
                        loc = page.locator(sel)
                        if loc.count() > 0:
                            imgs = loc
                            break
                    n = imgs.count() if imgs else 0
                    if n == 0 and i < 3:
                        logger.warning("[%s] 이미지 0건 (셀렉터 미일치 가능) url=%s", product_id, url)
                    download_headers = {**REQUEST_HEADERS, "Referer": url}
                    saved_count = 0
                    for j in range(n):
                        try:
                            el = imgs.nth(j)
                            src = el.get_attribute("src") or el.get_attribute("data-src")
                            if not src:
                                continue
                            abs_url = urljoin(page.url, src)
                            r = requests.get(abs_url, headers=download_headers, timeout=15)
                            r.raise_for_status()
                            out_path = img_dir / f"{product_id}_{j + 1}.png"
                            with open(out_path, "wb") as out:
                                out.write(r.content)
                            saved_count += 1
                        except Exception as e:
                            logger.debug("[%s] 이미지 %d 저장 실패: %s", product_id, j + 1, e)
                    seen.add(key)
                    # 이미지가 1장이라도 저장된 행에만 경로 append
                    row["car_imgs"] = car_imgs_value if saved_count > 0 else ""
                    if saved_count > 0:
                        logger.info("해당 %s 이미지 %d장 저장: %s", product_id, saved_count, img_dir)
                except Exception as e:
                    logger.warning("[%s] 상세 페이지 실패: %s", product_id, e)
                    row["car_imgs"] = ""
                # 수집되는 product_id마다 CSV에 car_imgs 반영
                with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                    w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
                    w.writeheader()
                    for r in rows:
                        if "detail_url" not in r:
                            r["detail_url"] = ""
                        if "car_imgs" not in r:
                            r["car_imgs"] = ""
                        w.writerow(r)
                if (i + 1) % 100 == 0:
                    logger.info("이미지 수집 진행: %d/%d", i + 1, len(rows))
                time.sleep(0.3)
        finally:
            browser.close()

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            if "detail_url" not in row:
                row["detail_url"] = ""
            if "car_imgs" not in row:
                row["car_imgs"] = ""
            w.writerow(row)
    logger.info("상세 이미지 반영 완료: %s (car_imgs=%s)", csv_path, car_imgs_value)


def _car_id_from_locator(locator) -> str:
    """한 요소(또는 그 하위)에서 carId 추출. a[href*='carId='], data-carid 등 시도."""
    try:
        # data-carid / data-car-id
        for attr in ("data-carid", "data-car-id"):
            el = locator.locator(f"[{attr}]").first
            if el.count() > 0:
                v = el.get_attribute(attr)
                if v and re.match(r"^\d+$", (v or "").strip()):
                    return (v or "").strip()
        # a[href*="carId="] 또는 hash 내 carId
        a = locator.locator('a[href*="carId="], a[href*="carId"]').first
        if a.count() > 0:
            href = (a.get_attribute("href") or "") + (" " + (a.get_attribute("data-href") or ""))
            m = re.search(r"carId[=:](\d+)", href, re.I)
            if m:
                return m.group(1)
            parsed = urlparse(href.split()[0])
            q = parse_qs(parsed.query) or parse_qs(parsed.fragment)
            for key in ("carId", "carid"):
                if key in q and q[key]:
                    v = q[key][0]
                    if re.match(r"^\d+$", (v or "").strip()):
                        return (v or "").strip()
    except Exception:
        pass
    return ""


def run_lotterentacar_update_car_name_from_list_page(page, result_dir: Path, logger):
    """
    list.csv가 있을 때, 목록 페이지 DOM에서 .list-detail .list-title 텍스트를 product_id별로 수집해
    CSV의 car_name 컬럼을 element 값으로 갱신.
    차종(경차, 소형차, ...)별로 클릭해 각 차종 목록에서 수집 후 병합.
    """
    csv_path = result_dir / "lotterentacar_list.csv"
    if not csv_path.exists():
        logger.warning("목록 CSV 없음: %s", csv_path)
        return
    if URL not in (page.url or ""):
        page.goto(URL, wait_until="domcontentloaded", timeout=LIST_PAGE_TIMEOUT)
        page.wait_for_timeout(3000)
    id_to_car_name = {}
    car_type_locator = None
    for sel in (SELECTOR_CAR_TYPE_OPTIONS, SELECTOR_CAR_TYPE_OPTIONS_FALLBACK, SELECTOR_LI, SELECTOR_LI_FALLBACK):
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                car_type_locator = loc
                break
        except Exception:
            continue
    def _scrape_current_list():
        list_item_selector = None
        for sel in (SELECTOR_LIST_ITEMS, ".list-type-vertical.n3 [class*='list-item']"):
            if page.locator(sel).count() > 0:
                list_item_selector = sel
                break
        if not list_item_selector:
            return
        items = page.locator(list_item_selector)
        n = items.count()
        for i in range(n):
            it = items.nth(i)
            pid = _car_id_from_locator(it)
            if not pid:
                continue
            try:
                title_el = it.locator(".list-detail .list-title").first
                if title_el.count() > 0:
                    raw = (title_el.inner_text() or "").strip()
                    if raw:
                        raw = raw.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
                        id_to_car_name[pid] = _normalize_text(raw)
            except Exception:
                pass

    type_count = car_type_locator.count() if car_type_locator else 0
    if car_type_locator is None:
        logger.warning("차종 필터 요소 없음, 현재 목록만 수집")
        _scrape_current_list()
    else:
        for type_idx in range(type_count):
            try:
                opt_li = car_type_locator.nth(type_idx)
                opt_li.scroll_into_view_if_needed(timeout=3000)
                page.wait_for_timeout(300)
                opt_li.locator("label").first.click() if opt_li.locator("label").count() > 0 else opt_li.click()
                page.wait_for_timeout(2500)
            except Exception as e:
                logger.debug("차종 %d 클릭 실패: %s", type_idx, e)
                continue
            _scrape_current_list()
    if not id_to_car_name:
        logger.warning("목록 페이지에서 car_name 수집 0건 (DOM 반영 생략, API fallback 유지)")
        return
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        for row in reader:
            rows.append(row)
    updated = 0
    for row in rows:
        pid = (row.get("product_id") or "").strip()
        if pid and pid in id_to_car_name:
            row["car_name"] = id_to_car_name[pid]
            updated += 1
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    logger.info("car_name 목록 페이지(element) 기준 %d건 반영 완료 (CSV 총 %d건 중)", len(id_to_car_name), updated)


def run_lotterentacar_fetch_list_images(
    result_dir: Path,
    logger,
    list_csv_path: Path | None = None,
    img_dir: Path | None = None,
):
    """
    목록 CSV(lotterentacar_list.csv)를 기준으로 각 차량 상세 페이지에 접속해
    첫 번째 이미지를 썸네일로 저장하고, car_imgs 컬럼에 해당 파일 경로를 즉시 반영.

    - 저장 경로: imgs/lotterentacar/list/연도년/YYYYMMDD/{product_id}_list.png
      (상세 이미지와 구분을 위해 list 하위에 저장)
    - 수집 도중 중단되더라도, 이미 저장된 이미지까지의 car_imgs는 CSV에 남도록
      이미지 저장 직후마다 CSV를 덮어쓴다.
    - img_dir 지정 시(Airflow): 해당 경로에 저장하고 car_imgs는 절대경로 문자열.
    """
    csv_path = list_csv_path if list_csv_path is not None else (result_dir / "lotterentacar_list.csv")
    if not csv_path.exists():
        logger.warning("목록 CSV 없음: %s", csv_path)
        return

    now = datetime.now()
    year_str = f"{now.year}년"
    date_str = now.strftime("%Y%m%d")

    use_absolute_car_imgs = img_dir is not None
    if img_dir is None:
        img_dir = _lotterentacar_list_img_dir(now)
        car_imgs_dir = f"imgs/lotterentacar/list/{year_str}/{date_str}"
    else:
        img_dir = Path(img_dir)
        car_imgs_dir = None  # unused; use str(out_path)

    img_dir.mkdir(parents=True, exist_ok=True)
    logger.info(
        "리스트(썸네일) 이미지 저장 디렉터리: %s (car_imgs=%s)",
        img_dir,
        car_imgs_dir if car_imgs_dir else "(절대경로)",
    )

    # CSV 전체를 메모리에 올려두고, 각 행이 처리될 때마다 car_imgs를 갱신 후 즉시 flush
    rows: list[dict] = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        rows = [row for row in reader]
    if "car_imgs" not in headers:
        idx = headers.index("date_crtr_pnttm") if "date_crtr_pnttm" in headers else len(headers)
        headers.insert(idx, "car_imgs")

    def flush_csv():
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as fw:
            w = csv.DictWriter(fw, fieldnames=headers, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                # car_imgs 키가 없으면 빈값으로 채움
                if "car_imgs" not in r:
                    r["car_imgs"] = r.get("car_imgs") or ""
                w.writerow(r)
            fw.flush()

    processed = 0
    saved = 0

    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        try:
            context = browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent=REQUEST_HEADERS.get(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                ),
            )
            page = context.new_page()

            total = len(rows)
            logger.info("리스트 썸네일 수집 시작: 총 %d건 대상 (CSV=%s)", total, csv_path)

            for i, row in enumerate(rows):
                product_id = (row.get("product_id") or "").strip()
                sale_type = (row.get("sale_type") or "").strip()

                if not product_id:
                    row["car_imgs"] = row.get("car_imgs") or ""
                    continue

                # 이미 썸네일이 있는 경우 스킵 (재시도 시 중복 저장 방지)
                existing = (row.get("car_imgs") or "").strip()
                if existing.endswith(f"/{product_id}_list.png"):
                    processed += 1
                    logger.debug("[list-thumb %s] 이미 썸네일 존재, 스킵 (car_imgs=%s)", product_id, existing)
                    continue

                # detail_url 컬럼이 있으면 우선 사용, 없으면 sale_type 기준으로 생성
                detail_url = (row.get("detail_url") or "").strip()
                if not detail_url:
                    if sale_type == "구매":
                        detail_url = f"{URL_VIEW_BASE}?carId={product_id}&saleTySale=true"
                    else:
                        # 기본값: 렌트
                        detail_url = f"{URL_VIEW_BASE}?carId={product_id}&saleTyRent=true"

                try:
                    logger.debug("[list-thumb %s] 상세 페이지 접속 시도: %s", product_id, detail_url)
                    page.goto(detail_url, wait_until="domcontentloaded", timeout=DETAIL_PAGE_TIMEOUT)
                    try:
                        page.wait_for_selector(SELECTOR_DETAIL_IMAGE_CONTAINER, timeout=10000)
                    except Exception:
                        pass
                    page.wait_for_timeout(2500)

                    imgs = None
                    for sel in SELECTOR_DETAIL_IMAGES:
                        loc = page.locator(sel)
                        if loc.count() > 0:
                            imgs = loc
                            break
                    n = imgs.count() if imgs else 0
                    if n == 0:
                        if i < 3:
                            logger.warning("[list-thumb %s] 상세 페이지 이미지 0건 (url=%s)", product_id, detail_url)
                        row["car_imgs"] = row.get("car_imgs") or ""
                    else:
                        # 첫 번째 이미지만 썸네일로 사용
                        try:
                            el = imgs.nth(0)
                            src = el.get_attribute("src") or el.get_attribute("data-src")
                            if not src:
                                row["car_imgs"] = row.get("car_imgs") or ""
                            else:
                                abs_url = urljoin(page.url, src)
                                download_headers = {**REQUEST_HEADERS, "Referer": detail_url}
                                r = requests.get(abs_url, headers=download_headers, timeout=15)
                                r.raise_for_status()
                                out_path = img_dir / f"{product_id}_list.png"
                                with open(out_path, "wb") as out:
                                    out.write(r.content)
                                row["car_imgs"] = (
                                    str(out_path) if use_absolute_car_imgs else f"{car_imgs_dir}/{product_id}_list.png"
                                )
                                saved += 1
                                logger.info("[list-thumb %s] 썸네일 저장 완료: %s → car_imgs=%s", product_id, out_path, row["car_imgs"])
                        except Exception as e:
                            logger.debug("[list-thumb %s] 썸네일 저장 실패: %s", product_id, e)
                            row["car_imgs"] = row.get("car_imgs") or ""
                except Exception as e:
                    logger.warning("[list-thumb %s] 상세 페이지 접속 실패: %s", product_id, e)
                    row["car_imgs"] = row.get("car_imgs") or ""

                processed += 1
                flush_csv()  # 매 행마다 즉시 반영
                if processed % 100 == 0:
                    logger.info("리스트 썸네일 진행 상태: %d/%d건 처리, 썸네일 %d건 저장", processed, total, saved)
        finally:
            browser.close()

    flush_csv()
    logger.info(
        "리스트 썸네일 수집 완료: 총 %d건 처리, 썸네일 %d건 저장 (car_imgs=%s)",
        processed,
        saved,
        car_imgs_dir if car_imgs_dir else str(img_dir),
    )


def run_lotterentacar_list(page, result_dir: Path, logger, list_csv_path: Path | None = None, brand_path: Path | None = None):
    """
    차종(경차, 소형차, 준중형차 등) 필터를 하나씩 선택한 뒤, 나온 목록에서 LIST_PER_CAR_LIMIT(5)건씩 수집 → lotterentacar_list.csv
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = list_csv_path if list_csv_path is not None else (result_dir / "lotterentacar_list.csv")
    if csv_path.exists():
        csv_path.unlink()
    headers = [
        "model_sn", "car_type_name", "brand_list", "car_list", "list_title", "list_title_1", "car_name",
        "year", "dstc", "fuel", "discount", "price_installment", "price2_block", "prc", "em", "price_new", "badge",
        "date_crtr_pnttm", "create_dt",
    ]
    model_to_brand_car = _load_brand_model_mapping(result_dir, logger, brand_path=brand_path)

    try:
        logger.info("롯데렌터카 검색 결과 리스트 수집 시작 (차종=경차/소형차/... 별 %d건씩): %s", LIST_PER_CAR_LIMIT, URL)
        if URL not in (page.url or ""):
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        car_type_locator = None
        for sel in (SELECTOR_CAR_TYPE_OPTIONS, SELECTOR_CAR_TYPE_OPTIONS_FALLBACK, SELECTOR_LI, SELECTOR_LI_FALLBACK):
            try:
                loc = page.locator(sel)
                loc.first.wait_for(state="visible", timeout=5000)
                car_type_locator = loc
                logger.info("차종 필터 셀렉터 사용: %s", sel)
                break
            except Exception:
                continue
        if car_type_locator is None:
            logger.warning("차종(경차/소형차/...) 필터 요소를 찾지 못했습니다.")
            return

        type_count = car_type_locator.count()
        if type_count == 0:
            logger.warning("차종 옵션 0개입니다.")
            return
        logger.info("차종 옵션 %d개 (각 %d건씩 수집)", type_count, LIST_PER_CAR_LIMIT)

        model_sn = 0
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()

            for i in range(type_count):
                try:
                    opt_li = car_type_locator.nth(i)
                    car_type_name = _get_label_text(opt_li)
                    if not car_type_name:
                        car_type_name = _normalize_text(opt_li.inner_text())
                    if not car_type_name:
                        continue
                    opt_li.scroll_into_view_if_needed(timeout=2000)
                    page.wait_for_timeout(200)
                    opt_li.locator("label").first.click() if opt_li.locator("label").count() > 0 else opt_li.click()
                    page.wait_for_timeout(2500)
                except Exception as e:
                    logger.debug("차종 옵션 %d 클릭 실패: %s", i, e)
                    continue

                try:
                    page.wait_for_selector(SELECTOR_LIST_ITEM, timeout=8000)
                    page.wait_for_timeout(500)
                except Exception:
                    pass
                items = page.locator(SELECTOR_LIST_ITEM)
                n = items.count()
                take = min(LIST_PER_CAR_LIMIT, n) if n else 0
                if take == 0:
                    logger.info("[%s] 목록 0건 → 스킵", car_type_name)
                    try:
                        opt_li.locator("label").first.click()
                        page.wait_for_timeout(500)
                    except Exception:
                        pass
                    continue
                logger.info("[%s] 목록 %d건 중 %d건 수집", car_type_name, n, take)
                for k in range(take):
                    model_sn += 1
                    row = _extract_list_row(items.nth(k), model_sn, "", "")
                    row["car_type_name"] = car_type_name
                    list_key = _model_key(row.get("list_title"), row.get("list_title_1"))
                    brand_list, car_list = model_to_brand_car.get(list_key, ("", ""))
                    if not brand_list and list_key:
                        for bkey, (b, c) in model_to_brand_car.items():
                            if bkey in list_key or list_key in bkey:
                                brand_list, car_list = b, c
                                break
                    row["brand_list"] = brand_list
                    row["car_list"] = car_list
                    w.writerow(row)
                    f.flush()
                    # logger.info("  model_sn=%d car_type=%s brand=%s car=%s %s", model_sn, car_type_name, row["brand_list"], row["car_list"], row.get("list_title", "")[:25])

                # 수집 후 현재 차종 한 번 더 클릭해 선택 해제 → 다음에 소형차/준중형차만 선택되도록
                try:
                    opt_li.locator("label").first.click() if opt_li.locator("label").count() > 0 else opt_li.click()
                    page.wait_for_timeout(600)
                except Exception:
                    pass

        logger.info("저장 완료: %s (총 %d건)", csv_path, model_sn)
    except Exception as e:
        logger.error("리스트 수집 오류: %s", e, exc_info=True)


def _run_lotterentacar_car_type_csv(datst_cd: str) -> str:
    """차종 CSV 수집 Task (data17)."""
    activate_paths_for_datst((datst_cd or LOTTERENTACAR_DATST_CAR_TYPE).lower() or LOTTERENTACAR_DATST_CAR_TYPE)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    car_type_csv = RESULT_DIR / f"lotterentacar_car_type_list_{run_ts}.csv"
    logger.info("차종 CSV 저장 경로: %s", car_type_csv)
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        context = browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=REQUEST_HEADERS.get(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ),
        )
        page = context.new_page()
        try:
            run_lotterentacar_car_type_list(page, RESULT_DIR, logger, csv_path=car_type_csv)
        finally:
            browser.close()
    return str(car_type_csv)


def _run_lotterentacar_brand_csv(datst_cd: str) -> str:
    """브랜드 CSV 수집 Task (data16)."""
    activate_paths_for_datst((datst_cd or LOTTERENTACAR_DATST_BRAND).lower() or LOTTERENTACAR_DATST_BRAND)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    brand_csv = RESULT_DIR / f"lotterentacar_brand_list_{run_ts}.csv"
    logger.info("브랜드 CSV 저장 경로: %s", brand_csv)
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        context = browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=REQUEST_HEADERS.get(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ),
        )
        page = context.new_page()
        try:
            run_lotterentacar_brand_list(page, RESULT_DIR, logger, csv_path=brand_csv)
        finally:
            browser.close()
    return str(brand_csv)


def run_lotterentacar_list_job(
    bsc: BscInfo,
    *,
    brand_list_csv_path: str,
    car_type_csv_path: str | None = None,
) -> dict[str, Any]:
    """
    DB 메타 1건(목록 datst) 기준 롯데렌터카 목록/이미지 수집.
    브랜드 CSV는 앞단 Task에서 생성한 절대경로를 사용한다.
    """
    activate_paths_for_datst((bsc.datst_cd or LOTTERENTACAR_DATST_LIST).lower() or LOTTERENTACAR_DATST_LIST)
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

    logger.info("🏁 롯데렌터카 목록 수집 시작")
    logger.info("- pvsn_site_cd=%s, datst_cd=%s, dtst_nm=%s", bsc.pvsn_site_cd, bsc.datst_cd, bsc.dtst_nm)
    logger.info("- link_url=%s", bsc.link_url)
    logger.info("- 브랜드 CSV(이전 태스크): %s", brand_path)

    list_path = RESULT_DIR / f"lotterentacar_list_{run_ts}.csv"
    if list_path.exists():
        list_path.unlink()

    run_lotterentacar_list_via_ajax(RESULT_DIR, logger, list_csv_path=list_path)
    run_lotterentacar_fetch_list_images(
        RESULT_DIR,
        logger,
        list_csv_path=list_path,
        img_dir=IMG_BASE / "list",
    )

    count = 0
    if list_path.exists():
        with open(list_path, "r", encoding="utf-8-sig") as f:
            count = sum(1 for _ in csv.DictReader(f))
    logger.info("✅ 롯데렌터카 목록 수집 완료: 총 %d건", count)
    return {
        "brand_csv": str(brand_path),
        "car_type_csv": car_type_csv_path or "",
        "list_csv": str(list_path),
        "count": count,
        "log_dir": str(LOG_DIR),
        "img_base": str(IMG_BASE),
    }


lotterentacar_dag = lotterentacar_crawler_dag()


def main():
    activate_paths_for_datst(LOTTERENTACAR_DATST_LIST)
    logger = setup_logger()
    result_dir = RESULT_DIR
    result_dir.mkdir(parents=True, exist_ok=True)

    # 1) 차종 목록 수집 → lotterentacar_car_type_list.csv
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        try:
            context = browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent=REQUEST_HEADERS.get(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                ),
            )
            page = context.new_page()
            run_lotterentacar_car_type_list(page, result_dir, logger)
            run_lotterentacar_brand_list(page, result_dir, logger)
        finally:
            browser.close()

    # 2) 목록 수집: Network AJAX API 사용 → lotterentacar_list.csv
    run_lotterentacar_list_via_ajax(result_dir, logger)

    # 3) 상세 페이지 이미지: list.csv의 product_id로 각 상세 페이지 접속 후 이미지 저장, car_imgs 컬럼 채움 (브라우저)
    #    (여러 장을 저장하고 싶을 때 사용)
    # run_lotterentacar_fetch_detail_images(result_dir, logger)

    # 4) 상세 페이지 첫 번째 이미지를 썸네일로 저장하여 list 폴더 및 car_imgs 컬럼 채움
    run_lotterentacar_fetch_list_images(result_dir, logger)


if __name__ == "__main__":
    main()
