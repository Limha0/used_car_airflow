# -*- coding: utf-8 -*-
"""
오토인사이드 중고차 목록 페이지에서 차종/브랜드/목록 수집.

Airflow DAG:
- DB 메타(std.tn_data_bsc_info, ps00001, data1/2/3) 조회
- 차종 CSV → 브랜드 CSV → 목록/이미지 순으로 단계별 Task 실행
"""
import csv
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from pathlib import Path
from typing import Any

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from playwright.sync_api import sync_playwright

# autoinside/ 에서 실행해도 프로젝트 루트 import 가능하도록
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


@dag(
    dag_id="sdag_autoinside_crawler",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["used_car", "autoinside", "crawler", "day"],
)
def autoinside_crawler_dag():
    """
    오토인사이드 브랜드/차종/목록 수집 DAG.

    - DB 메타(std.tn_data_bsc_info, ps00001, data1/2/3) 조회
    - 차종 CSV → 브랜드 CSV → 목록/이미지 순, 태스크별 파일 생성
    """

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id="car_db_conn")

    @task
    def insert_collect_data_info(**kwargs) -> dict[str, dict[str, Any]]:
        """std.tn_data_bsc_info에서 오토인사이드(ps00001) 수집 대상 기본 정보 조회."""
        select_bsc_info_stmt = f"""
        SELECT * FROM std.tn_data_bsc_info tdbi
        WHERE 1=1
          AND LOWER(clct_yn) = 'y'
          AND LOWER(link_yn) = 'y'
          AND LOWER(pvsn_site_cd) = 'ps00001'
          AND LOWER(datst_cd) IN ('data1','data2','data3')
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
                AUTOINSIDE_DATST_BRAND,
                AUTOINSIDE_DATST_CAR_TYPE,
                AUTOINSIDE_DATST_LIST,
            )
            if k not in out
        ]
        if missing:
            raise ValueError(
                f"std.tn_data_bsc_info 조회 결과 누락: {missing} (pvsn_site_cd={AUTOINSIDE_PVSN_SITE_CD})"
            )
        return out

    @task
    def run_car_type_csv(infos: dict[str, dict[str, Any]]) -> str:
        dc = str((infos.get(AUTOINSIDE_DATST_CAR_TYPE) or {}).get("datst_cd") or AUTOINSIDE_DATST_CAR_TYPE).lower()
        return _run_autoinside_car_type_csv(dc)

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]]) -> str:
        dc = str((infos.get(AUTOINSIDE_DATST_BRAND) or {}).get("datst_cd") or AUTOINSIDE_DATST_BRAND).lower()
        return _run_autoinside_brand_csv(dc)

    @task
    def run_list_csv(
        bsc_infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        car_type_csv_path: str,
    ) -> dict[str, Any]:
        bsc = BscInfo.from_row(bsc_infos[AUTOINSIDE_DATST_LIST])
        return run_autoinside_list_job(
            bsc,
            brand_list_csv_path=brand_csv_path,
            car_type_csv_path=car_type_csv_path or None,
        )

    @task_group(group_id="create_csv_process")
    def create_csv_process(bsc_infos: dict[str, dict[str, Any]]) -> dict[str, Any]:
        car_type_path = run_car_type_csv(bsc_infos)
        brand_path = run_brand_csv(bsc_infos)
        return run_list_csv(bsc_infos, brand_path, car_type_path)

    infos = insert_collect_data_info()
    create_csv_process(infos)

# Airflow Variable (기존 크롤러 DAG와 동일)
USED_CAR_SITE_NAMES_VAR = "used_car_site_names"
CRAWL_BASE_PATH_VAR = "crawl_base_path"

# DB `std.tn_data_bsc_info`: 오토인사이드 제공사이트 코드
AUTOINSIDE_PVSN_SITE_CD = "ps00001"
AUTOINSIDE_DATST_BRAND = "data1"
AUTOINSIDE_DATST_CAR_TYPE = "data2"
AUTOINSIDE_DATST_LIST = "data3"
AUTOINSIDE_SITE_NAME = "오토인사이드"


def _get_crawl_base_path() -> Path:
    try:
        v = Variable.get(CRAWL_BASE_PATH_VAR, default_var=None)
        if v:
            return Path(str(v).strip())
    except Exception:
        pass
    return Path(Variable.get("HEYDEALER_BASE_PATH", "/home/limhayoung/data"))


def get_site_name_by_datst(datst_cd: str) -> str:
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
    return AUTOINSIDE_SITE_NAME


def get_autoinside_site_name() -> str:
    for datst_cd in (
        AUTOINSIDE_DATST_LIST,
        AUTOINSIDE_DATST_BRAND,
        AUTOINSIDE_DATST_CAR_TYPE,
    ):
        site_name = get_site_name_by_datst(datst_cd)
        if site_name and site_name.strip():
            return site_name
    return AUTOINSIDE_SITE_NAME


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
    global RESULT_DIR, LOG_DIR, IMG_BASE, YEAR_STR, DATE_STR, RUN_TS

    base = _get_crawl_base_path()
    site = get_autoinside_site_name()
    now = datetime.now()
    YEAR_STR = now.strftime("%Y년")
    DATE_STR = now.strftime("%Y%m%d")
    RUN_TS = now.strftime("%Y%m%d%H%M")

    RESULT_DIR = base / "crawl" / YEAR_STR / site / DATE_STR
    LOG_DIR = base / "log" / YEAR_STR / site / DATE_STR
    IMG_BASE = base / "img" / YEAR_STR / site / DATE_STR


YEAR_STR = ""
DATE_STR = ""
RUN_TS = ""
RESULT_DIR = Path("/tmp")
LOG_DIR = Path("/tmp")
IMG_BASE = Path("/tmp")

try:
    activate_paths_for_datst(AUTOINSIDE_DATST_LIST)
except Exception:
    pass

# True: Chrome 창 띄워서 브랜드 클릭 수집 (제조사 전환 시 목록 갱신 안정). False: headless.
# 창 안 띄우려면 실행 전에 AUTOINSIDE_HEADED=0 설정.
USE_HEADED_FOR_BRAND = os.environ.get("AUTOINSIDE_HEADED", "1").strip().lower() in ("1", "true", "yes")
# 1/true/yes: 브랜드 목록을 AJAX API(display_bu_used_car_list_ajax.do)로 수집. 0: Playwright 클릭 방식.
USE_AJAX_FOR_BRAND = os.environ.get("AUTOINSIDE_USE_AJAX", "0").strip().lower() in ("1", "true", "yes")
HEADLESS_MODE = True

URL = "https://www.autoinside.co.kr/display/bu/display_bu_used_car_list.do"
URL_AJAX = "https://www.autoinside.co.kr/display/bu/display_bu_used_car_list_ajax.do"
# wrap > frm > container > ... > category_box cate_model_list > model_list > li
SELECTOR_LI = "#wrap #frm .container .container_inn .page.page_buy_car_list .car_list_wrap .car_list_wrap_l .car_list_wrap_inn .category_box.cate_model_list .model_list li"
SELECTOR_LI_FALLBACK = ".cate_model_list .model_list li"
# 우측 목록: 광고 제외(.banner), img_wrap·car_info 있는 차량만 (무한스크롤 목록 수집용)
SELECTOR_CAR_ITEM = (
    "#wrap #frm .container .container_inn .page.page_buy_car_list "
    ".car_list_wrap .car_list_wrap_r .car_item.tmp_item:not(.banner):has(.img_wrap):has(.car_info)"
)
# 국산/수입 라디오: label[for="i_sFlagDiff_N"]=국산, label[for="i_sFlagDiff_Y"]=수입
# 브랜드 트리: data-type="mnfc"(제조사) → data-type="brnd"(차종) → data-type="model"(모델, label span.nm이 UI와 동일한 값)
# 클릭은 input[data-type="mnfc"]/input[data-type="brnd"] 사용 시 라벨 가림 방지. 클래스명 변경 시에도 data-type 기준으로 동작.


def get_autoinside_imgs_relpath(dt=None):
    """standalone 실행 시 리스트 이미지 저장 상대 경로."""
    now = dt or datetime.now()
    return f"imgs/autoinside/list/{now.year}년/{now.strftime('%Y%m%d')}"


def _norm(s):
    """공백 정규화: 앞뒤 제거, 연속 공백 하나로."""
    return " ".join((s or "").strip().split())


def load_brand_nm_mapping(result_dir: Path, brand_path: Path | None = None):
    """
    autoinside_brand_list.csv에서 brand_list, model_list 컬럼을 읽어
    이은 값(brand_list + ' ' + model_list, 공백 정규화)을 키로,
    (brand_list, car_list, model_list)를 값으로 하는 딕셔너리 반환.
    list.csv의 car_name에 해당 키가 포함되면 해당 행의 brand_list, car_list, model_list에 매칭값 채움.
    """
    csv_path = brand_path if brand_path is not None else (result_dir / "autoinside_brand_list.csv")
    out = {}
    if not csv_path.exists():
        return out
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            r = csv.DictReader(f)
            for row in r:
                bl = (row.get("brand_list") or "").strip()
                cl = (row.get("car_list") or "").strip()
                ml = (row.get("model_list") or "").strip()
                key = _norm(bl + " " + ml)
                if key:
                    out[key] = {"brand_list": bl, "car_list": cl, "model_list": ml}
    except Exception:
        pass
    return out


def find_brand_match(nm_norm: str, brand_nm_map: dict):
    """
    brand_nm_map의 키(brand_list+model_list 이은 문자열)가 nm_norm(car_name)에 포함되어 있으면
    해당 brand_list, car_list, model_list를 반환. 여러 개 매칭 시 가장 긴 키(가장 구체적) 선택.
    """
    if not nm_norm or not brand_nm_map:
        return None
    match_key = None
    for key in brand_nm_map:
        if key and key in nm_norm:
            if match_key is None or len(key) > len(match_key):
                match_key = key
    return brand_nm_map.get(match_key) if match_key else None


def _get_file_logger(run_ts: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("autoinside_crawler")
    logger.setLevel(logging.INFO)
    log_path = str(LOG_DIR / f"autoinside_crawler_{run_ts}.log")
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == log_path:
            return logger
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    return logger


def setup_logger(log_basename="autoinside_car_type_list"):
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    return _get_file_logger(run_ts)


def _launch_browser(playwright, headless: bool, *, prefer_chrome: bool = False):
    launch_kwargs = {
        "headless": headless,
        "args": [
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--no-sandbox",
        ],
    }
    if prefer_chrome:
        try:
            return playwright.chromium.launch(channel="chrome", **launch_kwargs)
        except Exception:
            pass
    return playwright.chromium.launch(**launch_kwargs)


def run_autoinside_car_type_list(page, result_dir: Path, logger, csv_path: Path | None = None):
    """
    오토인사이드 중고차 목록 페이지에서 차종(모델) 목록 수집.
    car_list_wrap > ... > category_box cate_model_list > model_list 내 li 텍스트를
    car_type_name으로, car_type_sn은 1,2,3... 으로 저장.
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_path if csv_path is not None else (result_dir / "autoinside_car_type_list.csv")
    if csv_path.exists():
        csv_path.unlink()

    headers = ["car_type_sn", "car_type_name", "date_crtr_pnttm", "create_dt"]

    try:
        logger.info("오토인사이드 차종 목록 수집 시작: %s", URL)
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)

        # model_list 내 li 대기 (긴 셀렉터 먼저, 실패 시 fallback)
        li_locator = None
        for selector in (SELECTOR_LI, SELECTOR_LI_FALLBACK):
            try:
                loc = page.locator(selector)
                loc.first.wait_for(state="visible", timeout=10000)
                li_locator = loc
                # logger.info("차종 목록 영역 로드됨 (셀렉터: %s)", selector)
                break
            except Exception as e:
                logger.debug("셀렉터 실패 %s: %s", selector, e)
                continue

        if li_locator is None:
            logger.warning("차종 목록(li) 요소를 찾지 못했습니다.")
            return

        n = li_locator.count()
        if n == 0:
            logger.warning("차종 li 개수가 0입니다.")
            return

        logger.info("총 %d개 차종 수집 시작", n)

        car_type_sn = 1
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for i in range(n):
                name = (li_locator.nth(i).inner_text() or "").strip()
                # 줄바꿈/여러 공백을 하나의 공백으로
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
                logger.info("[%d/%d] %s", car_type_sn, n, name)
                car_type_sn += 1

        logger.info("저장 완료: %s (총 %d건)", csv_path, car_type_sn - 1)
    except Exception as e:
        logger.error("차종 수집 오류: %s", e, exc_info=True)


def _normalize_text(text):
    """줄바꿈/여러 공백을 하나의 공백으로."""
    return " ".join((text or "").strip().split())


def run_autoinside_brand_list_via_ajax(result_dir: Path, logger, csv_path: Path | None = None):
    """
    오토인사이드 display_bu_used_car_list_ajax.do API로 차량 목록을 페이지네이션하여
    수집한 뒤, 제조사·차종·모델 조합을 추출하여 autoinside_brand_list.csv 로 저장.
    (Playwright 클릭 방식 대신 사용 시 차종 목록 미갱신/클릭 가림 문제 회피)

    JSON 구조와 컬럼 매핑:
    - object.mnfc_list[]: 제조사 목록. v_flag_diff "N"→국산, "Y"→수입(category_cmn), xc_mkco_nm→제조사(brand_list).
      단, 하위(차종/모델)는 mnfc_list에 없음.
    - object.list[]: 실제 차량 광고 목록. 여기서 제조사·차종·모델 계층을 채움.
      각 항목: xc_mkco_nm=제조사(brand_list), xc_vcl_brnd_nm=차종(car_list), xc_vcmd_nm=모델(model_list).
      i_sFlagDiff=N/Y 요청으로 국산/수입을 나누어 호출하므로 category_cmn은 요청 기준으로 "국산"/"수입".
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_path if csv_path is not None else (result_dir / "autoinside_brand_list.csv")
    if csv_path.exists():
        csv_path.unlink()
    headers_csv = ["model_sn", "category_cmn", "brand_list", "car_list", "model_list", "production_period", "date_crtr_pnttm", "create_dt"]

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": URL + "/",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    })

    # (category_cmn, brand_list, car_list, model_list) 중복 제거용
    seen = set()
    rows = []

    for category_flag, category_cmn in [("N", "국산"), ("Y", "수입")]:
        page_no = 1
        page_size = 100
        logger.info("[%s] AJAX 목록 수집 시작", category_cmn)
        while True:
            try:
                data = {
                    "i_sFlagDiff": category_flag,
                    "i_iNowPageNo": page_no,
                    "i_iPageSize": page_size,
                }
                resp = session.post(URL_AJAX, data=data, timeout=30)
                resp.raise_for_status()
                body = resp.json()
            except requests.RequestException as e:
                logger.warning("[%s] AJAX 요청 실패 (page=%s): %s", category_cmn, page_no, e)
                break
            except ValueError as e:
                logger.warning("[%s] AJAX JSON 파싱 실패 (page=%s): %s", category_cmn, page_no, e)
                break

            if body.get("status") != "succ":
                logger.warning("[%s] AJAX status != succ: %s", category_cmn, body.get("status"))
                break

            obj = body.get("object") or {}
            # 차종(car_list)·모델(model_list)은 list[]에만 있음. mnfc_list는 제조사+국산/수입만 제공.
            lst = obj.get("list") or []
            total_pages = int(obj.get("i_iTotalPageCnt") or 0)
            if page_no == 1 and total_pages:
                total = int(obj.get("i_iRecordCnt") or 0)
                logger.info("[%s] 총 %d건, %d페이지", category_cmn, total, total_pages)

            for item in lst:
                # list 항목: xc_mkco_nm=제조사, xc_vcl_brnd_nm=차종, xc_vcmd_nm=모델
                mkco = (item.get("xc_mkco_nm") or "").strip()
                brnd = (item.get("xc_vcl_brnd_nm") or "").strip()
                model = (item.get("xc_vcmd_nm") or "").strip()
                if not mkco and not brnd and not model:
                    continue
                key = (category_cmn, mkco, brnd, model)
                if key in seen:
                    continue
                seen.add(key)
                now = datetime.now()
                rows.append({
                    "model_sn": len(rows) + 1,
                    "category_cmn": category_cmn,
                    "brand_list": mkco or "-",
                    "car_list": brnd or "-",
                    "model_list": model or brnd or "-",
                    "production_period": "",
                    "date_crtr_pnttm": now.strftime("%Y%m%d"),
                    "create_dt": now.strftime("%Y%m%d%H%M"),
                })

            if not lst or page_no >= total_pages:
                break
            page_no += 1

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers_csv)
        w.writeheader()
        w.writerows(rows)
    logger.info("저장 완료 (AJAX): %s (총 %d건)", csv_path, len(rows))


def run_autoinside_brand_list(page, result_dir: Path, logger, csv_path: Path | None = None):
    """
    오토인사이드에서 국산/수입 선택 후, 좌측 트리(제조사→차종→모델)를 수집 → autoinside_brand_list.csv
    - data-type/data-previd 기반으로 파싱해 클래스명·위치 변경에 강함.
    - 제조사·차종 클릭은 input[data-type="mnfc"]/input[data-type="brnd"] 사용(라벨 가림 방지).
    - brand_list = 제조사명(현대, 기아, …), car_list = 차종(그랜저, 베뉴, …), model_list = 모델(디 올 뉴 그랜저 등).
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_path if csv_path is not None else (result_dir / "autoinside_brand_list.csv")
    if csv_path.exists():
        csv_path.unlink()
    headers = ["model_sn", "category_cmn", "brand_list", "car_list", "model_list", "production_period", "date_crtr_pnttm", "create_dt"]

    try:
        logger.info("오토인사이드 브랜드 목록 수집 시작: %s", URL)
        try:
            page.set_viewport_size({"width": 1600, "height": 960})
        except Exception:
            pass
        if URL not in (page.url or ""):
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2000)

        try:
            page.locator(".category_box .sel_mnfc_type.category_cmn_btns").first.wait_for(state="visible", timeout=10000)
        except Exception:
            logger.warning("국산/수입 버튼 영역을 찾지 못했습니다.")
            return

        category_configs = [("i_sFlagDiff_N", "국산"), ("i_sFlagDiff_Y", "수입")]
        total_rows = 0

        for radio_for, category_cmn in category_configs:
            try:
                page.locator(f'label[for="{radio_for}"]').first.wait_for(state="visible", timeout=3000)
                page.locator(f'label[for="{radio_for}"]').first.click()
                page.wait_for_timeout(2000)
            except Exception as e:
                logger.warning("[%s] 라디오 클릭 실패: %s", category_cmn, e)
                continue

            # data-type 있는 트리 사용 시도 (클래스명 변경에 강함)
            mnfc_inputs = page.locator('input[data-type="mnfc"]')
            try:
                mnfc_inputs.first.wait_for(state="visible", timeout=8000)
            except Exception:
                logger.warning("[%s] input[data-type=mnfc] 없음. 기존 클래스 방식은 제거되어 있습니다. AUTOINSIDE_USE_AJAX=1 사용을 권장합니다.", category_cmn)
                continue

            mnfc_count = mnfc_inputs.count()
            logger.info("[%s] 제조사 %d개 (data-type 기준)", category_cmn, mnfc_count)

            for i in range(mnfc_count):
                mnfc_value = ""
                mnfc_name = ""
                for _ in range(3):
                    try:
                        mnfc_loc = page.locator('input[data-type="mnfc"]').nth(i)
                        mnfc_value = mnfc_loc.get_attribute("value") or ""
                        mnfc_name = mnfc_loc.evaluate("""el => {
                            const lb = document.querySelector('label[for="' + el.id + '"]');
                            if (!lb) return '';
                            const nm = lb.querySelector('.nm');
                            return (nm ? nm.textContent : lb.textContent || '').trim();
                        }""")
                        break
                    except Exception:
                        page.wait_for_timeout(500)
                        continue
                mnfc_name = _normalize_text(mnfc_name)
                if not mnfc_value or not mnfc_name:
                    continue
                logger.info("[%s] 수집 중: 제조사 %s (%d/%d)", category_cmn, mnfc_name, i + 1, mnfc_count)
                clicked = False
                for attempt in range(3):
                    try:
                        mnfc_loc = page.locator('input[data-type="mnfc"]').nth(i)
                        mnfc_loc.scroll_into_view_if_needed()
                        page.wait_for_timeout(300)
                        mnfc_loc.evaluate("el => el.click()")
                        page.wait_for_timeout(2500)
                        clicked = True
                        break
                    except Exception as e:
                        if attempt < 2:
                            page.wait_for_timeout(800)
                        else:
                            logger.warning("[%s] 제조사 클릭 실패 %s: %s", category_cmn, mnfc_name, e)
                if not clicked:
                    continue

                try:
                    page.locator('input[data-type="brnd"]').first.wait_for(state="visible", timeout=5000)
                except Exception:
                    pass

                # 이 제조사 하위 차종만 (data-previd가 mnfc_value인 brnd)
                brnd_locator = page.locator(f'input[data-type="brnd"][data-previd="{mnfc_value}"]')
                brnd_count = brnd_locator.count()
                if brnd_count == 0:
                    total_rows += 1
                    now = datetime.now()
                    row = {
                        "model_sn": total_rows,
                        "category_cmn": category_cmn, "brand_list": mnfc_name, "car_list": mnfc_name, "model_list": mnfc_name,
                        "production_period": "", "date_crtr_pnttm": now.strftime("%Y%m%d"), "create_dt": now.strftime("%Y%m%d%H%M"),
                    }
                    with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
                        w = csv.DictWriter(f, fieldnames=headers)
                        if total_rows == 1:
                            w.writeheader()
                        w.writerow(row)
                    logger.info("[%s] %s 차종 0개 → 1건 기록", category_cmn, mnfc_name)
                else:
                    for j in range(brnd_count):
                        # j>0이면 이전에 차종 클릭으로 목록이 모델로 바뀌었을 수 있으므로 제조사 재클릭 후 차종 목록 복원
                        if j > 0:
                            for _ in range(3):
                                try:
                                    page.locator('input[data-type="mnfc"]').nth(i).evaluate("el => el.click()")
                                    page.wait_for_timeout(1500)
                                    page.locator('input[data-type="brnd"]').first.wait_for(state="visible", timeout=5000)
                                    break
                                except Exception:
                                    page.wait_for_timeout(500)
                        brnd_value = ""
                        car_name = ""
                        for _ in range(3):
                            try:
                                brnd_loc = page.locator(f'input[data-type="brnd"][data-previd="{mnfc_value}"]').nth(j)
                                brnd_value = brnd_loc.get_attribute("value") or ""
                                car_name = brnd_loc.evaluate("""el => {
                                    const lb = document.querySelector('label[for="' + el.id + '"]');
                                    return lb ? (lb.querySelector('.nm') ? lb.querySelector('.nm').textContent : lb.textContent || '').trim() : '';
                                }""")
                                break
                            except Exception:
                                page.wait_for_timeout(500)
                                continue
                        car_name = _normalize_text(car_name) or mnfc_name
                        if not brnd_value:
                            continue
                        brnd_clicked = False
                        for attempt in range(3):
                            try:
                                brnd_loc = page.locator(f'input[data-type="brnd"][data-previd="{mnfc_value}"]').nth(j)
                                brnd_loc.scroll_into_view_if_needed()
                                page.wait_for_timeout(200)
                                brnd_loc.evaluate("el => el.click()")
                                page.wait_for_timeout(1200)
                                brnd_clicked = True
                                break
                            except Exception as e:
                                if attempt < 2:
                                    page.wait_for_timeout(600)
                                else:
                                    logger.warning("[%s] 차종 클릭 실패 %s/%s: %s", category_cmn, mnfc_name, (car_name or "")[:20], e)
                        if not brnd_clicked:
                            continue

                        # 이 차종 하위 모델 (data-previd="mnfc_value,brnd_value")
                        previd_prefix = mnfc_value + "," + brnd_value
                        model_inputs = page.locator(f'input[data-type="model"][data-previd="{previd_prefix}"]')
                        model_count = model_inputs.count()

                        if model_count == 0:
                            total_rows += 1
                            now = datetime.now()
                            row = {
                                "model_sn": total_rows,
                                "category_cmn": category_cmn, "brand_list": mnfc_name, "car_list": car_name, "model_list": car_name,
                                "production_period": "", "date_crtr_pnttm": now.strftime("%Y%m%d"), "create_dt": now.strftime("%Y%m%d%H%M"),
                            }
                            with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
                                w = csv.DictWriter(f, fieldnames=headers)
                                if total_rows == 1:
                                    w.writeheader()
                                w.writerow(row)
                        else:
                            for k in range(model_count):
                                model_loc = model_inputs.nth(k)
                                try:
                                    model_info = model_loc.evaluate("""el => {
                                    const lb = document.querySelector('label[for="' + el.id + '"]');
                                    if (!lb) return { nm: '', year: '' };
                                    const nmEl = lb.querySelector('.nm');
                                    const yearEl = lb.querySelector('.year');
                                    const nm = (nmEl ? nmEl.textContent : lb.textContent || '').trim();
                                    let year = (yearEl ? yearEl.textContent : '').trim();
                                    year = year.replace(/^[(]|[)]$/g, '');
                                    return { nm: nm, year: year };
                                }""")
                                except Exception:
                                    model_info = {"nm": car_name, "year": ""}
                                model_name = _normalize_text(model_info.get("nm") or "") or car_name
                                production_period = _normalize_text(model_info.get("year") or "").strip()
                                if production_period and production_period.startswith("(") and production_period.endswith(")"):
                                    production_period = production_period[1:-1].strip()
                                total_rows += 1
                                now = datetime.now()
                                row = {
                                    "model_sn": total_rows,
                                    "category_cmn": category_cmn, "brand_list": mnfc_name, "car_list": car_name, "model_list": model_name,
                                    "production_period": production_period,
                                    "date_crtr_pnttm": now.strftime("%Y%m%d"), "create_dt": now.strftime("%Y%m%d%H%M"),
                                }
                                with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
                                    w = csv.DictWriter(f, fieldnames=headers)
                                    if total_rows == 1:
                                        w.writeheader()
                                    w.writerow(row)

                    logger.info("[%s] %s 차종 %d개 모델 수집 완료", category_cmn, mnfc_name, brnd_count)
                    # 다음 제조사로 넘어가기 전 현재 제조사 접기 (DOM 갱신 후 같은 인덱스로 재조회, 재시도)
                    for _ in range(3):
                        try:
                            page.locator('input[data-type="mnfc"]').nth(i).evaluate("el => el.click()")
                            page.wait_for_timeout(800)
                            break
                        except Exception:
                            page.wait_for_timeout(500)

            logger.info("[%s] 소계 %d건", category_cmn, total_rows)

        logger.info("저장 완료: %s (총 %d건)", csv_path, total_rows)
    except Exception as e:
        logger.error("브랜드 목록 수집 오류: %s", e, exc_info=True)


DETAIL_URL_TEMPLATE = "https://www.autoinside.co.kr/display/bu/display_bu_used_ah_car_view.do?i_sCarCd={product_id}"


def download_autoinside_list_image(
    item_locator,
    product_id: str,
    save_dir: Path,
    logger,
    *,
    return_absolute: bool = False,
) -> str | None:
    """
    목록 페이지의 한 car_item에서 .img_wrap .main_img 이미지를 다운로드하여
    save_dir/{product_id}_list.png 로 저장. 저장된 파일의 상대 경로 문자열 반환, 실패 시 None.
    """
    if not product_id or not save_dir:
        return None
    img_el = None
    try:
        loc_inner = item_locator.locator(".img_wrap .main_img img").first
        loc_inner.wait_for(state="visible", timeout=3000)
        img_el = loc_inner
    except Exception:
        try:
            loc_main = item_locator.locator(".img_wrap .main_img").first
            loc_main.wait_for(state="visible", timeout=2000)
            tag = loc_main.evaluate("el => el.tagName && el.tagName.toUpperCase()")
            if tag == "IMG":
                img_el = loc_main
            else:
                img_el = item_locator.locator(".img_wrap .main_img img").first
        except Exception:
            logger.debug("목록 이미지 요소 없음: %s", product_id)
            return None
    if img_el is None:
        return None
    try:
        src = (img_el.get_attribute("data-src") or img_el.get_attribute("src") or "").strip()
    except Exception:
        return None
    if not src:
        return None
    if src.startswith("//"):
        src = "https:" + src
    elif src.startswith("/"):
        src = "https://www.autoinside.co.kr" + src
    try:
        resp = requests.get(src, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("목록 이미지 다운로드 실패 %s: %s", product_id, e)
        return None
    save_path = save_dir / f"{product_id}_list.png"
    try:
        save_dir.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(resp.content)
    except Exception as e:
        logger.warning("목록 이미지 저장 실패 %s: %s", save_path, e)
        return None
    return str(save_path) if return_absolute else f"{get_autoinside_imgs_relpath()}/{product_id}_list.png"


def download_autoinside_images(page, result_dir: Path, logger, product_ids, img_dir: Path | None = None):
    """
    주어진 product_id 목록에 대해 상세 페이지를 열고 메인 이미지를 저장한다.
    - 상세 URL: https://www.autoinside.co.kr/display/bu/display_bu_used_ah_car_view.do?i_sCarCd={product_id}
    - 이미지 저장 경로: imgs/autoinside/{YYYY}년/{YYYYMMDD}/{product_id}_{순번}.png (get_autoinside_imgs_relpath와 동일)
    """
    if not product_ids:
        return

    try:
        if img_dir is None:
            img_dir = IMG_BASE / "detail"
        img_dir.mkdir(parents=True, exist_ok=True)

        for idx, pid in enumerate(product_ids, start=1):
            detail_url = f"https://www.autoinside.co.kr/display/bu/display_bu_used_ah_car_view.do?i_sCarCd={pid}"
            try:
                logger.info("(%d/%d) 상세 이미지 수집 시작: %s", idx, len(product_ids), pid)
                page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(3500)
            except Exception as e:
                logger.warning("상세 페이지 이동 실패 %s: %s", pid, e)
                continue

            # 메인 슬라이드 이미지: 셀렉터 완화 (car_view_wrap 내 img 또는 main_slide 내 img)
            img_locator = page.locator(
                ".page.car_view_wrap .car_view_content .car_img_wrap .main_slide img, "
                ".car_view_wrap .car_view_content .section.car_img_wrap .main_slide img"
            )
            try:
                img_locator.first.wait_for(state="visible", timeout=10000)
            except Exception:
                # 더 관대한 셀렉터로 재시도
                img_locator = page.locator(".car_view_content .car_img_wrap img, .car_view_content .section.car_img_wrap img")
                try:
                    img_locator.first.wait_for(state="visible", timeout=5000)
                except Exception:
                    logger.warning("상세 이미지 영역을 찾지 못했습니다: %s", pid)
                    continue

            img_count = img_locator.count()
            if img_count == 0:
                logger.warning("상세 이미지 개수가 0입니다: %s", pid)
                continue

            for img_idx in range(img_count):
                try:
                    img_el = img_locator.nth(img_idx)
                    src = (img_el.get_attribute("data-src") or img_el.get_attribute("src") or "").strip()
                except Exception:
                    continue
                if not src:
                    continue
                if src.startswith("//"):
                    src = "https:" + src
                elif src.startswith("/"):
                    src = "https://www.autoinside.co.kr" + src

                try:
                    resp = requests.get(src, timeout=30)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning("이미지 다운로드 실패 %s #%d: %s", pid, img_idx + 1, e)
                    continue

                img_path = img_dir / f"{pid}_{img_idx + 1}.png"
                try:
                    with open(img_path, "wb") as f:
                        f.write(resp.content)
                except Exception as e:
                    logger.warning("이미지 저장 실패 %s: %s", img_path, e)
                    continue

            logger.info("상세 이미지 수집 완료 %s (%d개)", pid, img_count)
    except Exception as e:
        logger.error("이미지 수집 오류: %s", e, exc_info=True)


def _scroll_list_until_end(page, item_selector: str, logger, max_no_new_rounds: int = 2, scroll_pause_ms: int = 2000):
    """
    무한 스크롤 페이지에서 끝까지 스크롤하여 모든 목록이 DOM에 로드되도록 한다.
    스크롤 후 item_selector 개수가 더 이상 늘지 않으면 종료.
    """
    no_new_rounds = 0
    last_count = 0
    while True:
        try:
            count = page.locator(item_selector).count()
        except Exception:
            count = 0
        if count == last_count:
            no_new_rounds += 1
            if no_new_rounds >= max_no_new_rounds:
                logger.info("스크롤 완료: 목록 개수 %d로 고정", count)
                return count
            page.wait_for_timeout(1000)
            continue
        no_new_rounds = 0
        last_count = count
        last_height = page.evaluate("document.body.scrollHeight")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(scroll_pause_ms)
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            page.wait_for_timeout(1500)
            if page.evaluate("document.body.scrollHeight") == last_height:
                logger.info("페이지 끝 도달, 목록 %d개", page.locator(item_selector).count())
                return page.locator(item_selector).count()
    return last_count


def run_autoinside_list(
    page,
    result_dir: Path,
    logger,
    max_per_page: int = 5,
    list_csv_path: Path | None = None,
    brand_path: Path | None = None,
    img_dir: Path | None = None,
):
    """
    목록 페이지에서 우측 차량 리스트의 요약 정보를 수집하여 autoinside_list.csv 로 저장.

    - model_sn: 1,2,3,... (현재 페이지에서의 순번; 테스트용으로 max_per_page 개수까지만 수집)
    - product_id: .img_wrap .li_detail.go_car_detail 요소의 id 값
    - brand_list, car_list, model_list: brand_list.csv와 nm(car_name) 포함 매칭
    - car_name: car_info 내 .nm 텍스트 (기존 nm)
    - car_spec, pyy, dvml, main, sub: 기존과 동일
    - detail_url: https://...display_bu_used_ah_car_view.do?i_sCarCd={product_id}
    - car_imgs: 목록 .main_img 이미지 저장 경로 (imgs/autoinside/list/연도년/YYYYMMDD/{product_id}_list.png)
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = list_csv_path if list_csv_path is not None else (result_dir / "autoinside_list.csv")
    if csv_path.exists():
        csv_path.unlink()

    headers = [
        "model_sn",
        "product_id",
        "car_type_name",
        "brand_list",
        "car_list",
        "model_list",
        "car_name",
        "car_spec",
        "pyy",
        "dvml",
        "main",
        "sub",
        "detail_url",
        "car_imgs",
        "date_crtr_pnttm",
        "create_dt",
    ]
    brand_nm_map = load_brand_nm_mapping(result_dir, brand_path=brand_path)

    try:
        logger.info("오토인사이드 목록(list) 수집 시작: %s", URL)
        if URL not in (page.url or ""):
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2000)

        items = page.locator(SELECTOR_CAR_ITEM)
        try:
            items.first.wait_for(state="visible", timeout=8000)
        except Exception:
            logger.warning("car_item.tmp_item(광고 제외, img_wrap·car_info 있음) 요소를 찾지 못했습니다.")
            return

        total_items = items.count()
        if total_items == 0:
            logger.warning("car_item.tmp_item 개수가 0입니다.")
            return

        limit = min(total_items, max_per_page)
        logger.info("총 %d개 car_item 중 %d개만 테스트 수집", total_items, limit)

        list_img_save_dir = img_dir if img_dir is not None else (IMG_BASE / "list")
        list_img_save_dir.mkdir(parents=True, exist_ok=True)
        model_sn = 1
        product_ids_for_images = []
        with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            f.flush()

            for i in range(limit):
                item = items.nth(i)
                try:
                    detail_el = item.locator(".img_wrap .li_detail.go_car_detail").first
                    product_id = (detail_el.get_attribute("id") or "").strip()
                except Exception:
                    product_id = ""
                if product_id:
                    product_ids_for_images.append(product_id)

                car_spec_val = ""
                pyy_val = ""
                dvml_val = ""
                nm_val = ""
                main_val = ""
                sub_val = ""

                try:
                    spec_el = item.locator(".car_info .car_spec").first
                    car_spec_val = (spec_el.get_attribute("data-fuel") or "").strip()
                except Exception:
                    pass

                try:
                    pyy_val = (item.locator(".car_info .car_spec .pyy").first.inner_text() or "").strip()
                except Exception:
                    pass

                try:
                    dvml_val = (item.locator(".car_info .car_spec .dvml").first.inner_text() or "").strip()
                except Exception:
                    pass

                try:
                    nm_val = (item.locator(".car_info .nm").first.inner_text() or "").strip()
                except Exception:
                    pass

                try:
                    main_val = (item.locator(".car_info .price .main").first.inner_text() or "").strip()
                except Exception:
                    pass

                try:
                    sub_text = (item.locator(".car_info .price .sub").first.inner_text() or "").strip()
                    # 괄호 제거
                    sub_val = sub_text.replace("(", "").replace(")", "").strip()
                except Exception:
                    pass

                # brand_list+car_list+model_list 이은 문자열이 nm에 포함되면 brand.csv에서 매칭
                match = find_brand_match(_norm(nm_val), brand_nm_map) if nm_val else None
                brand_list_val = (match["brand_list"] or "") if match else ""
                car_list_val = (match["car_list"] or "") if match else ""
                model_list_val = (match["model_list"] or "") if match else ""

                # 목록 페이지 .main_img 이미지 다운로드 → {product_id}_list.png, car_imgs에 경로
                car_imgs_val = ""
                if product_id:
                    car_imgs_val = download_autoinside_list_image(
                        item,
                        product_id,
                        list_img_save_dir,
                        logger,
                        return_absolute=img_dir is not None,
                    ) or ""

                detail_url_val = DETAIL_URL_TEMPLATE.format(product_id=product_id) if product_id else ""

                now = datetime.now()
                row = {
                    "model_sn": model_sn,
                    "product_id": product_id,
                    "car_type_name": "",
                    "brand_list": brand_list_val,
                    "car_list": car_list_val,
                    "model_list": model_list_val,
                    "car_name": nm_val,
                    "car_spec": car_spec_val,
                    "pyy": pyy_val,
                    "dvml": dvml_val,
                    "main": main_val,
                    "sub": sub_val,
                    "detail_url": detail_url_val,
                    "car_imgs": car_imgs_val,
                    "date_crtr_pnttm": now.strftime("%Y%m%d"),
                    "create_dt": now.strftime("%Y%m%d%H%M"),
                }
                w.writerow(row)
                f.flush()
                logger.info(
                    "[%d/%d] product_id=%s car_name=%s",
                    model_sn,
                    limit,
                    product_id or "-",
                    (nm_val or "")[:30],
                )
                model_sn += 1

        logger.info("저장 완료: %s (총 %d건)", csv_path, model_sn - 1)

        # 수집된 product_id들에 대해 상세 이미지를 함께 저장 (테스트용 주석)
        # download_autoinside_images(page, result_dir, logger, product_ids_for_images)
    except Exception as e:
        logger.error("목록(list) 수집 오류: %s", e, exc_info=True)


def run_autoinside_list_by_car_type(
    page,
    result_dir: Path,
    logger,
    max_per_type: int | None = None,
    list_csv_path: Path | None = None,
    brand_path: Path | None = None,
    img_dir: Path | None = None,
):
    """
    상단 차종 카테고리(경소형, 준중형, 중형, 대형, SUV/RV, 스포츠, 승합, 트럭)를
    하나씩 클릭한 뒤, 각 차종별로 우측 목록을 무한 스크롤로 끝까지 로드하고 전체 수집.
    max_per_type이 None이면 해당 차종 전체, 숫자면 테스트용으로 그 개수만 수집.
    결과는 autoinside_list.csv 하나에 누적 저장.
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = list_csv_path if list_csv_path is not None else (result_dir / "autoinside_list.csv")
    if csv_path.exists():
        csv_path.unlink()

    headers = [
        "model_sn",
        "product_id",
        "car_type_name",
        "brand_list",
        "car_list",
        "model_list",
        "car_name",
        "car_spec",
        "pyy",
        "dvml",
        "main",
        "sub",
        "detail_url",
        "car_imgs",
        "date_crtr_pnttm",
        "create_dt",
    ]
    brand_nm_map = load_brand_nm_mapping(result_dir, brand_path=brand_path)

    car_type_names = ["경소형", "준중형", "중형", "대형", "SUV/RV", "스포츠", "승합", "트럭"]

    try:
        logger.info("오토인사이드 차종별 목록(list) 수집 시작: %s", URL)
        # 간헐적으로 Page.goto가 net::ERR_ABORTED 를 내뿜는 경우가 있어,
        # 한 번 경고만 남기고 현재 페이지에서 그대로 진행하도록 완화.
        try:
            if URL not in (page.url or ""):
                page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            logger.warning("초기 goto 중 오류 발생(무시하고 진행): %s", e)
        page.wait_for_timeout(2000)

        list_img_save_dir = img_dir if img_dir is not None else (IMG_BASE / "list")
        list_img_save_dir.mkdir(parents=True, exist_ok=True)
        model_sn = 1
        with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            f.flush()

            for car_type_name in car_type_names:
                # 차종 카테고리 버튼 클릭
                try:
                    btn = page.locator(f"text={car_type_name}").first
                    btn.click()
                    page.wait_for_timeout(2000)
                    logger.info("[차종] '%s' 선택 후 목록 수집 시작", car_type_name)
                except Exception as e:
                    logger.warning("[차종] '%s' 버튼 클릭 실패: %s", car_type_name, e)
                    continue

                # 우측 목록: 광고 제외(.banner), img_wrap·car_info 있는 차량만
                items = page.locator(SELECTOR_CAR_ITEM)
                try:
                    items.first.wait_for(state="visible", timeout=8000)
                except Exception:
                    logger.warning("[차종] '%s' 에 대한 car_item(광고 제외, img_wrap·car_info 있음) 요소를 찾지 못했습니다.", car_type_name)
                    continue

                # 무한 스크롤: 끝까지 스크롤하여 전체 목록 로드
                logger.info("[차종] '%s' 무한 스크롤로 전체 목록 로드 중", car_type_name)
                _scroll_list_until_end(page, SELECTOR_CAR_ITEM, logger)
                items = page.locator(SELECTOR_CAR_ITEM)
                total_items = items.count()
                if total_items == 0:
                    logger.info("[차종] '%s' 목록이 비어 있습니다.", car_type_name)
                    continue

                limit = min(total_items, max_per_type) if max_per_type is not None else total_items
                logger.info("[차종] '%s' car_item 전체 %d개 중 %d개 수집", car_type_name, total_items, limit)
                product_ids_for_images = []
                collected = 0

                for i in range(limit):
                    item = items.nth(i)
                    try:
                        detail_el = item.locator(".img_wrap .li_detail.go_car_detail").first
                        product_id = (detail_el.get_attribute("id") or "").strip()
                    except Exception:
                        product_id = ""

                    car_spec_val = ""
                    pyy_val = ""
                    dvml_val = ""
                    nm_val = ""
                    main_val = ""
                    sub_val = ""

                    try:
                        spec_el = item.locator(".car_info .car_spec").first
                        car_spec_val = (spec_el.get_attribute("data-fuel") or "").strip()
                    except Exception:
                        pass
                    try:
                        pyy_val = (item.locator(".car_info .car_spec .pyy").first.inner_text() or "").strip()
                    except Exception:
                        pass
                    try:
                        dvml_val = (item.locator(".car_info .car_spec .dvml").first.inner_text() or "").strip()
                    except Exception:
                        pass
                    try:
                        nm_val = (item.locator(".car_info .nm").first.inner_text() or "").strip()
                    except Exception:
                        pass
                    try:
                        main_val = (item.locator(".car_info .price .main").first.inner_text() or "").strip()
                    except Exception:
                        pass
                    try:
                        sub_text = (item.locator(".car_info .price .sub").first.inner_text() or "").strip()
                        sub_val = sub_text.replace("(", "").replace(")", "").strip()
                    except Exception:
                        pass

                    # product_id 또는 nm 중 하나라도 있으면 유효 행으로 수집 (빈 슬롯/광고 행 스킵)
                    if not product_id and not nm_val:
                        continue

                    if product_id:
                        product_ids_for_images.append(product_id)
                    collected += 1

                    # brand_list+car_list+model_list 이은 문자열이 nm에 포함되면 brand.csv에서 매칭
                    match = find_brand_match(_norm(nm_val), brand_nm_map) if nm_val else None
                    brand_list_val = (match["brand_list"] or "") if match else ""
                    car_list_val = (match["car_list"] or "") if match else ""
                    model_list_val = (match["model_list"] or "") if match else ""

                    # 목록 페이지 .main_img 이미지 다운로드 → {product_id}_list.png, car_imgs에 경로
                    car_imgs_val = ""
                    if product_id:
                        car_imgs_val = download_autoinside_list_image(
                            item,
                            product_id,
                            list_img_save_dir,
                            logger,
                            return_absolute=img_dir is not None,
                        ) or ""

                    detail_url_val = DETAIL_URL_TEMPLATE.format(product_id=product_id) if product_id else ""

                    now = datetime.now()
                    row = {
                        "model_sn": model_sn,
                        "product_id": product_id,
                        "car_type_name": car_type_name,
                        "brand_list": brand_list_val,
                        "car_list": car_list_val,
                        "model_list": model_list_val,
                        "car_name": nm_val,
                        "car_spec": car_spec_val,
                        "pyy": pyy_val,
                        "dvml": dvml_val,
                        "main": main_val,
                        "sub": sub_val,
                        "detail_url": detail_url_val,
                        "car_imgs": car_imgs_val,
                        "date_crtr_pnttm": now.strftime("%Y%m%d"),
                        "create_dt": now.strftime("%Y%m%d%H%M"),
                    }
                    w.writerow(row)
                    f.flush()
                    logger.info(
                        "[%d] 차종=%s product_id=%s car_name=%s",
                        model_sn,
                        car_type_name,
                        product_id or "-",
                        (nm_val or "")[:30],
                    )
                    model_sn += 1

                # 해당 차종에 대해 수집된 product_id들로 상세 페이지 속 이미지 저장 (테스트용 주석)
                # download_autoinside_images(page, result_dir, logger, product_ids_for_images)

                # 이미지 수집 후 목록 페이지로 복귀해야 다음 차종(준중형, 중형 등) 버튼을 찾을 수 있음
                try:
                    page.goto(URL, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_timeout(2000)
                except Exception as e:
                    logger.warning("목록 페이지 복귀 중 오류(다음 차종 시도): %s", e)

        logger.info("차종별 목록 저장 완료: %s (총 %d건)", csv_path, model_sn - 1)
    except Exception as e:
        logger.error("차종별 목록(list_by_car_type) 수집 오류: %s", e, exc_info=True)


def _run_autoinside_car_type_csv(datst_cd: str) -> str:
    activate_paths_for_datst((datst_cd or AUTOINSIDE_DATST_CAR_TYPE).lower() or AUTOINSIDE_DATST_CAR_TYPE)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    car_type_csv = RESULT_DIR / f"autoinside_car_type_list_{run_ts}.csv"
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        page = browser.new_page()
        try:
            run_autoinside_car_type_list(page, RESULT_DIR, logger, csv_path=car_type_csv)
        finally:
            browser.close()
    return str(car_type_csv)


def _run_autoinside_brand_csv(datst_cd: str) -> str:
    activate_paths_for_datst((datst_cd or AUTOINSIDE_DATST_BRAND).lower() or AUTOINSIDE_DATST_BRAND)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    brand_csv = RESULT_DIR / f"autoinside_brand_list_{run_ts}.csv"
    if USE_AJAX_FOR_BRAND:
        run_autoinside_brand_list_via_ajax(RESULT_DIR, logger, csv_path=brand_csv)
        return str(brand_csv)
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        page = browser.new_page()
        try:
            run_autoinside_brand_list(page, RESULT_DIR, logger, csv_path=brand_csv)
        finally:
            browser.close()
    return str(brand_csv)


def run_autoinside_list_job(
    bsc: BscInfo,
    *,
    brand_list_csv_path: str,
    car_type_csv_path: str | None = None,
) -> dict[str, Any]:
    activate_paths_for_datst((bsc.datst_cd or AUTOINSIDE_DATST_LIST).lower() or AUTOINSIDE_DATST_LIST)
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

    logger.info("🏁 오토인사이드 목록 수집 시작")
    logger.info("- pvsn_site_cd=%s, datst_cd=%s, dtst_nm=%s", bsc.pvsn_site_cd, bsc.datst_cd, bsc.dtst_nm)
    logger.info("- link_url=%s", bsc.link_url)
    logger.info("- 브랜드 CSV(이전 태스크): %s", brand_path)

    list_path = RESULT_DIR / f"autoinside_list_{run_ts}.csv"
    if list_path.exists():
        list_path.unlink()

    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        page = browser.new_page()
        try:
            run_autoinside_list_by_car_type(
                page,
                RESULT_DIR,
                logger,
                max_per_type=None,
                list_csv_path=list_path,
                brand_path=brand_path,
                img_dir=IMG_BASE / "list",
            )
        finally:
            browser.close()

    count = 0
    if list_path.exists():
        with open(list_path, "r", encoding="utf-8-sig") as f:
            count = sum(1 for _ in csv.DictReader(f))
    logger.info("✅ 오토인사이드 목록 수집 완료: 총 %d건", count)
    return {
        "brand_csv": str(brand_path),
        "car_type_csv": car_type_csv_path or "",
        "list_csv": str(list_path),
        "count": count,
        "log_dir": str(LOG_DIR),
        "img_base": str(IMG_BASE),
    }


autoinside_dag = autoinside_crawler_dag()


def main():
    activate_paths_for_datst(AUTOINSIDE_DATST_LIST)
    result_dir = RESULT_DIR
    # 브랜드 수집은 Chrome 창 띄워서 클릭하는 방식으로 동작 (제조사 전환 시 목록 갱신 안정화)
    headless = not USE_HEADED_FOR_BRAND
    with sync_playwright() as p:
        browser = _launch_browser(p, headless, prefer_chrome=USE_HEADED_FOR_BRAND)
        try:
            page = browser.new_page()
            # [테스트] list.csv + 이미지만 수행 (차종/브랜드 목록 수집 생략)
            # logger_ct = setup_logger("car_type_list")
            # run_autoinside_car_type_list(page, result_dir, logger_ct)
            # logger_br = setup_logger("brand_list")
            # if USE_AJAX_FOR_BRAND:
            #     run_autoinside_brand_list_via_ajax(result_dir, logger_br)
            # else:
            #     run_autoinside_brand_list(page, result_dir, logger_br)

            logger_list = setup_logger("list")
            # list.csv 전체 목록 수집 + car_imgs 컬럼(이미지 경로) append + list 목록 이미지 저장
            run_autoinside_list_by_car_type(page, result_dir, logger_list, max_per_type=None)
        finally:
            browser.close()


if __name__ == "__main__":
    main()
