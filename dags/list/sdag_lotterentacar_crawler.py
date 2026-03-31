import csv
import json
import logging
import math
import re
import sys
import time
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
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

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from dto.tn_data_bsc_info import TnDataBscInfo
from util.common_util import CommonUtil
from util.playwright_util import GotoSpec, goto_with_retry, install_route_blocking


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
                    serialized = CommonUtil.serialize_row(dict(zip(cols, row)))
                    k = str(serialized.get("datst_cd") or "").lower().strip()
                    if k and k not in out:
                        out[k] = serialized
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
        bsc = CommonUtil.build_bsc_info_dto(bsc_infos[LOTTERENTACAR_DATST_LIST])
        return run_lotterentacar_list_job(
            bsc,
            brand_list_csv_path=brand_csv_path,
            car_type_csv_path=car_type_csv_path or None,
        )

    @task
    def register_csv_collect_log_info(
        bsc_infos: dict[str, dict[str, Any]],
        datst_cd: str,
        csv_path: str,
    ) -> dict[str, Any]:
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        csv_file_path = Path(str(csv_path or ""))
        if not csv_file_path.is_file():
            raise FileNotFoundError(
                f"수집 메타 등록 대상 CSV가 없습니다: datst_cd={datst_cd}, path={csv_file_path}"
            )

        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[datst_cd])
        tn_data_clct_dtl_info = CommonUtil.upsert_collect_detail_info(
            hook,
            LOTTERENTACAR_COLLECT_DETAIL_TABLE,
            tn_data_bsc_info.datst_cd,
            csv_file_path,
        )
        registered_file_path = str(
            CommonUtil.build_collect_detail_file_path(tn_data_clct_dtl_info)
        )
        logging.info(
            "롯데렌터카 수집 메타 등록: datst_cd=%s, file=%s, clct_pnttm=%s, status=%s",
            datst_cd,
            registered_file_path,
            tn_data_clct_dtl_info.clct_pnttm,
            getattr(tn_data_clct_dtl_info, "status", ""),
        )
        tn_data_clct_dtl_info_dict = tn_data_clct_dtl_info.as_dict()
        tn_data_clct_dtl_info_dict["file_path"] = registered_file_path
        tn_data_clct_dtl_info_dict["status"] = getattr(
            tn_data_clct_dtl_info, "status", ""
        )
        return tn_data_clct_dtl_info_dict

    @task
    def load_csv_to_ods(
        bsc_infos: dict[str, dict[str, Any]],
        tn_data_clct_dtl_info_map: dict[str, dict[str, Any]],
        datst_cd: str,
    ) -> dict[str, Any]:
        if datst_cd not in tn_data_clct_dtl_info_map:
            raise ValueError(f"수집 메타 등록 결과가 없습니다: datst_cd={datst_cd}")

        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[datst_cd])
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        tn_data_clct_dtl_info = CommonUtil.get_latest_collect_detail_info(
            hook,
            LOTTERENTACAR_COLLECT_DETAIL_TABLE,
            datst_cd,
        )
        if not tn_data_clct_dtl_info:
            raise ValueError(f"최신 수집 메타 정보가 없습니다: datst_cd={datst_cd}")

        latest_csv_path = CommonUtil.build_collect_detail_file_path(
            tn_data_clct_dtl_info
        )
        if not latest_csv_path.is_file():
            raise ValueError(
                f"CSV 수집 완료 조건 미충족: datst_cd={datst_cd}, exists={latest_csv_path.is_file()}, path={latest_csv_path}"
            )

        target_table = _resolve_target_table_for_datst(
            datst_cd, tn_data_bsc_info.ods_tbl_phys_nm
        )
        logging.info(
            "롯데렌터카 최신 CSV 메타 선택: datst_cd=%s, selected=%s, clct_pnttm=%s, file_nm=%s",
            datst_cd,
            latest_csv_path,
            tn_data_clct_dtl_info.clct_pnttm or "",
            tn_data_clct_dtl_info.clct_data_file_nm or "",
        )
        rows = _read_csv_rows(latest_csv_path)
        if not rows:
            raise ValueError(
                f"적재할 CSV 데이터가 없습니다: datst_cd={datst_cd}, path={latest_csv_path}"
            )
        if datst_cd == LOTTERENTACAR_DATST_LIST:
            rows = _dedupe_lotterentacar_list_rows(rows)
            target_table, _ = _resolve_list_table_targets(tn_data_bsc_info)

        if datst_cd in (
            LOTTERENTACAR_DATST_BRAND,
            LOTTERENTACAR_DATST_CAR_TYPE,
            LOTTERENTACAR_DATST_LIST,
        ):
            _delete_snapshot_rows(hook, target_table, rows)

        _bulk_insert_rows(
            hook,
            target_table,
            rows,
            truncate=False,
            allow_only_table_cols=True,
        )
        table_count = CommonUtil.get_table_row_count(hook, target_table)
        logging.info(
            "롯데렌터카 CSV 적재 완료: datst_cd=%s, table=%s, inserted_rows=%d, table_count=%d, csv=%s",
            datst_cd,
            target_table,
            len(rows),
            table_count,
            latest_csv_path,
        )
        return {
            "done": True,
            "datst_cd": datst_cd,
            "target_table": target_table,
            "row_count": len(rows),
            "table_count": table_count,
            "csv_path": str(latest_csv_path),
        }

    @task
    def sync_list_tmp_to_source(
        bsc_infos: dict[str, dict[str, Any]],
        brand_load_result: dict[str, Any],
        car_type_load_result: dict[str, Any],
        list_load_result: dict[str, Any],
    ) -> dict[str, Any]:
        for load_result in (brand_load_result, car_type_load_result, list_load_result):
            if not load_result.get("done"):
                raise ValueError(f"CSV 적재 완료 조건 미충족: {load_result}")

        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(
            bsc_infos[LOTTERENTACAR_DATST_LIST]
        )
        tmp_table, source_table = _resolve_list_table_targets(tn_data_bsc_info)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        current_rows = _read_csv_rows(Path(str(list_load_result.get("csv_path") or "")))
        sync_result = _sync_lotterentacar_tmp_to_source(
            hook,
            current_rows=current_rows,
            tmp_table=tmp_table,
            source_table=source_table,
        )
        logging.info(
            "롯데렌터카 list tmp->source 반영 완료: tmp_table=%s, source_table=%s, tmp_count=%d, current_row_count=%d, inserted_count=%d, marked_existing_count=%d, marked_missing_count=%d, source_count=%d",
            tmp_table,
            source_table,
            sync_result["tmp_count"],
            sync_result["current_row_count"],
            sync_result["inserted_count"],
            sync_result["marked_existing_count"],
            sync_result["marked_missing_count"],
            sync_result["source_count"],
        )
        return {
            "done": True,
            "tmp_table": tmp_table,
            "source_table": source_table,
            "tmp_count": sync_result["tmp_count"],
            "current_row_count": sync_result["current_row_count"],
            "inserted_count": sync_result["inserted_count"],
            "marked_existing_count": sync_result["marked_existing_count"],
            "marked_missing_count": sync_result["marked_missing_count"],
            "source_count": sync_result["source_count"],
        }

    @task_group(group_id="create_csv_process")
    def create_csv_process(bsc_infos: dict[str, dict[str, Any]]) -> dict[str, Any]:
        """차종/브랜드/목록 CSV 생성 그룹."""
        car_type_path = run_car_type_csv(bsc_infos)
        car_type_collect_info = register_csv_collect_log_info.override(
            task_id="register_car_type_collect_log_info"
        )(bsc_infos, LOTTERENTACAR_DATST_CAR_TYPE, car_type_path)

        brand_path = run_brand_csv(bsc_infos)
        brand_collect_info = register_csv_collect_log_info.override(
            task_id="register_brand_collect_log_info"
        )(bsc_infos, LOTTERENTACAR_DATST_BRAND, brand_path)

        list_result = run_list_csv(bsc_infos, brand_path, car_type_path)
        list_collect_info = register_csv_collect_log_info.override(
            task_id="register_list_collect_log_info"
        )(bsc_infos, LOTTERENTACAR_DATST_LIST, list_result["list_csv"])

        return {
            LOTTERENTACAR_DATST_BRAND: brand_collect_info,
            LOTTERENTACAR_DATST_CAR_TYPE: car_type_collect_info,
            LOTTERENTACAR_DATST_LIST: list_collect_info,
        }

    @task_group(group_id="insert_csv_process")
    def insert_csv_process(
        bsc_infos: dict[str, dict[str, Any]],
        tn_data_clct_dtl_info_map: dict[str, dict[str, Any]],
    ) -> None:
        brand_load_result = load_csv_to_ods.override(task_id="load_brand_csv_to_ods")(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            LOTTERENTACAR_DATST_BRAND,
        )
        car_type_load_result = load_csv_to_ods.override(
            task_id="load_car_type_csv_to_ods"
        )(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            LOTTERENTACAR_DATST_CAR_TYPE,
        )
        list_load_result = load_csv_to_ods.override(task_id="load_list_csv_to_ods")(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            LOTTERENTACAR_DATST_LIST,
        )
        sync_list_tmp_to_source.override(task_id="sync_list_tmp_to_source")(
            bsc_infos,
            brand_load_result,
            car_type_load_result,
            list_load_result,
        )

    infos = insert_collect_data_info()
    tn_data_clct_dtl_info_map = create_csv_process(infos)
    insert_csv_process(infos, tn_data_clct_dtl_info_map)

# Airflow Variable (기존 크롤러 DAG와 동일)
USED_CAR_SITE_NAMES_VAR = "used_car_site_names"
CRAWL_BASE_PATH_VAR = "crawl_base_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"
LOTTERENTACAR_COLLECT_DETAIL_TABLE = "std.tn_data_clct_dtl_info"

# DB `std.tn_data_bsc_info`: 롯데렌터카 제공사이트 코드
LOTTERENTACAR_PVSN_SITE_CD = "ps00006"
LOTTERENTACAR_DATST_BRAND = "data16"
LOTTERENTACAR_DATST_CAR_TYPE = "data17"
LOTTERENTACAR_DATST_LIST = "data18"
LOTTERENTACAR_SITE_NAME = "롯데렌터카"
LOTTERENTACAR_BRAND_TABLE = "ods.ods_brand_list_lotterentacar"
LOTTERENTACAR_CAR_TYPE_TABLE = "ods.ods_car_type_list_lotterentacar"
LOTTERENTACAR_TMP_LIST_TABLE = "ods.ods_tmp_car_list_lotterentacar"
LOTTERENTACAR_SOURCE_LIST_TABLE = "ods.ods_car_list_lotterentacar"


def _get_crawl_base_path() -> Path:
    try:
        v = Variable.get(CRAWL_BASE_PATH_VAR, default_var=None)
        if v:
            return Path(str(v).strip())
    except Exception:
        pass
    return Path(Variable.get("HEYDEALER_BASE_PATH", "/home/limhayoung/data"))


def _get_variable_path(var_name: str) -> Path | None:
    try:
        v = Variable.get(var_name, default_var=None)
        if v and str(v).strip():
            return Path(str(v).strip())
    except Exception:
        pass
    return None


def _get_img_root_path() -> Path:
    direct = _get_variable_path(IMAGE_FILE_PATH_VAR)
    if direct is not None:
        return direct
    return _get_crawl_base_path() / "img"


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


def activate_paths_for_datst(datst_cd: str) -> None:
    """롯데렌터카 공통 crawl / log / img 루트 경로 설정 (각 Task 시작 시 호출)."""
    global RESULT_DIR, LOG_DIR, IMG_BASE, YEAR_STR, DATE_STR, RUN_TS

    base = _get_crawl_base_path()
    img_root = _get_img_root_path()
    site = get_lotterentacar_site_name()
    now = datetime.now()
    YEAR_STR = now.strftime("%Y년")
    DATE_STR = now.strftime("%Y%m%d")
    RUN_TS = now.strftime("%Y%m%d%H%M")

    RESULT_DIR = CommonUtil.build_dated_site_path(base / "crawl", site, now)
    LOG_DIR = CommonUtil.build_dated_site_path(base / "log", site, now)
    IMG_BASE = CommonUtil.build_year_site_path(img_root, site, now)


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


def _clear_directory_contents(dir_path: Path) -> None:
    p = Path(dir_path)
    if not p.exists():
        return
    if not p.is_dir():
        raise ValueError(f"디렉터리가 아닙니다: {p}")
    for child in p.iterdir():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            try:
                child.unlink()
            except FileNotFoundError:
                pass


def _get_lotterentacar_list_imgs_relpath(dt: datetime) -> str:
    year_str = dt.strftime("%Y년")
    # autoinside와 동일한 상대 경로 형태
    return f"imgs/{year_str}/{get_lotterentacar_site_name()}/list"

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
        goto_with_retry(
            page,
            GotoSpec(
                URL,
                wait_until="commit",
                timeout_ms=90_000,
                ready_selectors=(SELECTOR_CONTAINER,),
                ready_timeout_ms=20_000,
            ),
            logger=logger,
            attempts=3,
        )
        page.wait_for_timeout(800)

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
    rows_since_flush = 0

    def write_row(f, w, brand_list, car_list, model_list, model_list_1, model_list_2):
        nonlocal model_sn, need_header, rows_since_flush
        model_sn += 1
        rows_since_flush += 1
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
        if rows_since_flush >= 100:
            f.flush()
            rows_since_flush = 0

    try:
        logger.info("==================== 롯데렌터카 브랜드/차종/모델 목록 수집 시작 ====================")
        if URL not in (page.url or ""):
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1500)
        try:
            page.wait_for_selector(SELECTOR_MNFC_LI, timeout=15000)
            page.wait_for_timeout(500)
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
                    _ensure_children_visible(page, brand_li, MODEL_CHILD_LI, logger, timeout_ms=2500)
                    page.wait_for_timeout(150)
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
                        _ensure_children_visible(page, car_li, DMODEL_CHILD_LI, logger, timeout_ms=2500)
                        page.wait_for_timeout(120)

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
                        _ensure_children_visible(page, d1_li, DMODEL_CHILD_LI, logger, timeout_ms=1800)
                        page.wait_for_timeout(100)
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
                            _ensure_children_visible(page, d2_li, DMODEL_CHILD_LI, logger, timeout_ms=1200)
                            page.wait_for_timeout(80)
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
                    page.wait_for_timeout(100)
                except Exception:
                    pass

            if rows_since_flush > 0:
                f.flush()
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


def _fetch_brand_list_from_options(logger) -> list[dict[str, Any]]:
    try:
        params = {
            **OPTIONS_DEFAULT_PARAMS,
            "carType": '[""]',
            "_": str(int(time.time() * 1000)),
        }
        r = requests.get(
            URL_OPTIONS, params=params, headers=REQUEST_HEADERS, timeout=30
        )
        r.raise_for_status()
        data = r.json()
        result = data.get("result") or data
        brand_list = result.get("brandList") or []
        logger.info("options API brandList %d건 로드", len(brand_list))
        return brand_list
    except Exception as e:
        logger.warning("options API brandList 조회 실패: %s", e)
        return []


def _build_lotterentacar_detail_url(item: dict[str, Any]) -> str:
    product_id = str(item.get("carId") or "").strip()
    saletype = str(item.get("saletype") or "").strip()
    if not product_id:
        return ""
    if saletype == "S":
        return f"{URL_VIEW_BASE}?carId={product_id}&saleTySale=true"
    return f"{URL_VIEW_BASE}?carId={product_id}&saleTyRent=true"


def _build_lotterentacar_image_url(item: dict[str, Any]) -> str:
    for key in ("carThumbnail", "carImage"):
        raw = str(item.get(key) or "").strip()
        if raw:
            return urljoin("https://tcar.lotterentacar.net", raw)
    return ""


def _build_lotterentacar_list_row(
    item: dict[str, Any],
    *,
    model_sn: int,
    car_type_val: str,
    date_crtr_pnttm: str,
    create_dt: str,
) -> dict[str, Any]:
    sale_type_map = {"R": "렌트", "S": "구매"}
    saletype = str(item.get("saletype") or "").strip()
    product_id = str(item.get("carId") or "").strip()
    return {
        "model_sn": model_sn,
        "product_id": product_id,
        "car_type": car_type_val,
        "brand_list": str(item.get("brandName") or "").strip(),
        "car_list": str(item.get("modelgroupName") or "").strip(),
        "model_list": str(item.get("modelName") or "").strip(),
        "model_list_1": str(item.get("gradeName") or "").strip(),
        "model_list_2": str(item.get("subgradeName") or "").strip(),
        "car_name": (
            str(item.get("carName") or item.get("listTitle") or item.get("title") or "").strip()
            or _model_key(
                item.get("brandName"),
                item.get("modelName"),
                item.get("gradeName"),
                item.get("subgradeName"),
            )
        ),
        "release_dt": str(item.get("regDate") or "").strip(),
        "car_navi": str(item.get("mileage") or "").strip(),
        "fuel": str(item.get("fuel") or "").strip(),
        "promotion_price": item.get("promotionPrice") if item.get("promotionPrice") is not None else "",
        "return_price": item.get("returnPrice") if item.get("returnPrice") is not None else "",
        "price_new": item.get("priceNew") if item.get("priceNew") is not None else "",
        "sale_type": sale_type_map.get(saletype, saletype),
        "poststart_dt": str(item.get("postStartDt") or "").strip(),
        "postend_dt": str(item.get("postEndDt") or "").strip(),
        "detail_url": _build_lotterentacar_detail_url(item),
        "image_url": _build_lotterentacar_image_url(item),
        "car_imgs": "",
        "date_crtr_pnttm": date_crtr_pnttm,
        "create_dt": create_dt,
    }


def _collect_lotterentacar_list_items_via_ajax(logger) -> list[dict[str, Any]]:
    cd_to_name = _fetch_car_type_list_from_options(logger)
    if not cd_to_name:
        logger.warning("차종 목록(cd)이 비어 있습니다.")
        return []

    items: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    api_total_sum = 0
    last_car_total_count = None

    for cd, car_type_name in cd_to_name.items():
        list_params = {
            **LIST_DEFAULT_PARAMS,
            "carType": f'["{cd}"]',
            "shuffleKey": "1",
            "page": "1",
            "_": str(int(time.time() * 1000)),
        }
        car_type_val = car_type_name or cd
        collected_this_type = 0
        total_count = 0
        page_no = 1

        while True:
            if page_no > 1:
                list_params["page"] = str(page_no)
                list_params["_"] = str(int(time.time() * 1000))
            try:
                r = requests.get(
                    URL_LIST,
                    params=list_params,
                    headers=REQUEST_HEADERS,
                    timeout=LIST_API_TIMEOUT,
                )
                r.raise_for_status()
                body = r.json()
            except Exception as e:
                logger.warning("[%s] list API page=%d 실패: %s", car_type_val, page_no, e)
                break

            result = body.get("result") or body
            records_filtered = int(result.get("recordsFiltered") or 0)
            records_promotion_filtered = int(
                result.get("recordsPromotionFiltered") or 0
            )
            if page_no == 1:
                total_count = records_filtered + records_promotion_filtered
                api_total_sum += total_count
                last_car_total_count = result.get("carTotalCount")
                logger.info("[%s] 수집 시작: 총 %d건 대상", car_type_val, total_count)

            page_items: list[dict[str, Any]] = []
            if page_no == 1:
                for item in result.get("promotionListData") or []:
                    if isinstance(item, dict):
                        page_items.append(item)
            for item in result.get("data") or []:
                if isinstance(item, dict):
                    page_items.append(item)

            if not page_items:
                break

            for item in page_items:
                product_id = str(item.get("carId") or "").strip()
                saletype = str(item.get("saletype") or "").strip()
                dedupe_key = (product_id, saletype)
                if product_id and dedupe_key in seen_keys:
                    continue
                if product_id:
                    seen_keys.add(dedupe_key)
                normalized = dict(item)
                normalized["resolved_car_type"] = car_type_val
                items.append(normalized)
                collected_this_type += 1

            logger.info("[%s] 수집 진행: %d/%d", car_type_val, collected_this_type, total_count)
            if total_count and collected_this_type >= total_count:
                break

            per_page = int(list_params.get("perPageNum", 100))
            if len(result.get("data") or []) < per_page:
                break
            page_no += 1
            time.sleep(0.1)

    logger.info(
        "롯데렌터카 AJAX 목록 원천 수집 완료: unique_items=%d, api_total_sum=%d, carTotalCount=%s",
        len(items),
        api_total_sum,
        last_car_total_count if last_car_total_count is not None else "N/A",
    )
    return items


def run_lotterentacar_car_type_list_via_ajax(
    result_dir: Path, logger, csv_path: Path | None = None
):
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_path or (result_dir / "lotterentacar_car_type_list.csv")
    headers = ["car_type_sn", "car_type_name", "date_crtr_pnttm", "create_dt"]
    cd_to_name = _fetch_car_type_list_from_options(logger)
    now = datetime.now()
    date_crtr_pnttm = now.strftime("%Y%m%d")
    create_dt = now.strftime("%Y%m%d%H%M")

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for car_type_sn, car_type_name in enumerate(cd_to_name.values(), start=1):
            writer.writerow(
                {
                    "car_type_sn": car_type_sn,
                    "car_type_name": car_type_name,
                    "date_crtr_pnttm": date_crtr_pnttm,
                    "create_dt": create_dt,
                }
            )
    logger.info("차종 CSV 저장 완료: %s (총 %d건)", csv_path, len(cd_to_name))


def run_lotterentacar_brand_list_via_ajax(
    result_dir: Path, logger, csv_path: Path | None = None
):
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_path or (result_dir / "lotterentacar_brand_list.csv")
    headers = [
        "model_sn",
        "brand_list",
        "car_list",
        "model_list",
        "model_list_1",
        "model_list_2",
        "date_crtr_pnttm",
        "create_dt",
    ]
    now = datetime.now()
    date_crtr_pnttm = now.strftime("%Y%m%d")
    create_dt = now.strftime("%Y%m%d%H%M")
    items = _collect_lotterentacar_list_items_via_ajax(logger)
    valid_brand_names = {
        str(item.get("name") or "").strip()
        for item in _fetch_brand_list_from_options(logger)
        if str(item.get("name") or "").strip()
    }
    seen_keys: set[tuple[str, str, str, str, str]] = set()
    rows: list[dict[str, Any]] = []

    for item in items:
        key = (
            str(item.get("brandName") or "").strip(),
            str(item.get("modelgroupName") or "").strip(),
            str(item.get("modelName") or "").strip(),
            str(item.get("gradeName") or "").strip(),
            str(item.get("subgradeName") or "").strip(),
        )
        if not key[0]:
            continue
        if valid_brand_names and key[0] not in valid_brand_names:
            continue
        if key in seen_keys:
            continue
        seen_keys.add(key)
        rows.append(
            {
                "model_sn": len(rows) + 1,
                "brand_list": key[0],
                "car_list": key[1],
                "model_list": key[2],
                "model_list_1": key[3],
                "model_list_2": key[4],
                "date_crtr_pnttm": date_crtr_pnttm,
                "create_dt": create_dt,
            }
        )

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("브랜드 CSV 저장 완료: %s (총 %d건)", csv_path, len(rows))


def run_lotterentacar_list_via_ajax(result_dir: Path, logger, list_csv_path: Path | None = None):
    """AJAX list API로 목록 수집 후 CSV 저장, 이미지 URL 맵을 함께 반환."""
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = list_csv_path if list_csv_path is not None else (result_dir / "lotterentacar_list.csv")
    headers = [
        "model_sn", "product_id", "car_type", "brand_list", "car_list", "model_list",
        "model_list_1", "model_list_2", "car_name", "release_dt", "car_navi", "fuel",
        "promotion_price", "return_price", "price_new", "sale_type",
        "poststart_dt", "postend_dt",
        "detail_url",
        "image_url",
        "car_imgs",
        "date_crtr_pnttm", "create_dt",
    ]
    now = datetime.now()
    date_crtr_pnttm = now.strftime("%Y%m%d")
    create_dt = now.strftime("%Y%m%d%H%M")
    raw_items = _collect_lotterentacar_list_items_via_ajax(logger)
    rows: list[dict[str, Any]] = []
    image_url_map: dict[str, str] = {}
    for model_sn, item in enumerate(raw_items, start=1):
        row = _build_lotterentacar_list_row(
            item,
            model_sn=model_sn,
            car_type_val=str(item.get("resolved_car_type") or ""),
            date_crtr_pnttm=date_crtr_pnttm,
            create_dt=create_dt,
        )
        rows.append(row)
        product_id = str(row.get("product_id") or "").strip()
        image_url = str(row.get("image_url") or "").strip()
        if product_id and image_url:
            image_url_map[product_id] = image_url

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

    logger.info("AJAX 목록 CSV 저장 완료: %s (총 %d건)", csv_path, len(rows))
    return {
        "csv_path": str(csv_path),
        "row_count": len(rows),
        "image_url_map": image_url_map,
    }


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
            install_route_blocking(context)
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
                    goto_with_retry(
                        page,
                        GotoSpec(
                            url,
                            wait_until="commit",
                            timeout_ms=int(DETAIL_PAGE_TIMEOUT),
                            ready_selectors=(SELECTOR_DETAIL_IMAGE_CONTAINER,),
                            ready_timeout_ms=10_000,
                        ),
                        logger=logger,
                        attempts=3,
                    )
                    # 이미지 영역 로드 대기 (React/동적 렌더)
                    try:
                        page.wait_for_selector(SELECTOR_DETAIL_IMAGE_CONTAINER, timeout=10000)
                    except Exception:
                        pass
                    page.wait_for_timeout(1200)
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
    image_url_map: dict[str, str] | None = None,
):
    """
    목록 CSV 기준으로 목록 API가 제공한 이미지 URL을 병렬 다운로드하여 car_imgs를 반영한다.

    - 저장 경로: imgs/.../list/{product_id}_list.png (로컬은 repo 상대 imgs, Airflow는 IMG_BASE/list)
    - car_imgs: 상세 이미지(run_lotterentacar_fetch_detail_images)와 같이, 저장 파일 기준 경로 문자열
      (로컬: 상대 imgs/... 가능 구간과 동일하게 resolve된 절대경로로 통일)
    """
    csv_path = list_csv_path if list_csv_path is not None else (result_dir / "lotterentacar_list.csv")
    if not csv_path.exists():
        logger.warning("목록 CSV 없음: %s", csv_path)
        return

    now = datetime.now()
    rel_dir = _get_lotterentacar_list_imgs_relpath(now)
    if img_dir is None:
        # 로컬 실행(디버그): repo 내 imgs/.. 경로로 저장
        img_dir = _root / rel_dir
    else:
        img_dir = Path(img_dir)

    img_dir.mkdir(parents=True, exist_ok=True)
    img_dir_abs = str(Path(img_dir).resolve())
    logger.info(
        "리스트(썸네일) 이미지 저장 디렉터리: %s (car_imgs=각 행 저장 파일 절대경로, 예: %s/<product_id>_list.png)",
        img_dir_abs,
        img_dir_abs,
    )

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
                if "car_imgs" not in r:
                    r["car_imgs"] = r.get("car_imgs") or ""
                w.writerow(r)
            fw.flush()

    image_url_map = image_url_map or {}
    # 요청사항: 목록 DOM에서 alt="차량..." 이미지 src 사용(가능하면 우선 적용)
    # DOM은 첫 페이지/현재 렌더된 범위만 커버하므로, API image_url_map과 병합하되
    # 동일 product_id는 DOM src를 우선한다.
    try:
        dom_map: dict[str, str] = {}
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
                page.goto(URL, wait_until="domcontentloaded", timeout=LIST_PAGE_TIMEOUT)
                page.wait_for_timeout(2500)
                pairs = page.evaluate(
                    """
                    () => {
                        const out = [];
                        const items = Array.from(document.querySelectorAll('li.list-item'));
                        for (const it of items) {
                            const a = it.querySelector('a[data-carid]');
                            const pid = a ? (a.getAttribute('data-carid') || '').trim() : '';
                            if (!pid) continue;
                            const img =
                                it.querySelector('.list-thumb img[alt*="차량"]') ||
                                it.querySelector('img[alt*="차량"]');
                            const src = img ? (img.getAttribute('src') || img.getAttribute('data-src') || '').trim() : '';
                            if (!src) continue;
                            out.push([pid, src]);
                        }
                        return out;
                    }
                    """
                ) or []
                for pid, src in pairs:
                    pid = str(pid or "").strip()
                    src = str(src or "").strip()
                    if pid and src and pid not in dom_map:
                        dom_map[pid] = src
            finally:
                browser.close()

        if dom_map:
            merged = dict(image_url_map)
            merged.update(dom_map)  # DOM 우선
            image_url_map = merged
            logger.info(
                "썸네일 URL 병합: api=%d건, dom=%d건, merged=%d건",
                len(merged) - len(dom_map),
                len(dom_map),
                len(merged),
            )
    except Exception as e:
        logger.warning("목록 DOM 기반 썸네일 URL 로드 실패(무시): %s", e)

    def _normalize_thumbnail_url(raw_url: str) -> str:
        """
        롯데렌터카 목록 API가 내려주는 image_url은 종종 tcar 도메인 아래의 경로(404)로 들어온다.
        실제 썸네일은 img-mycarsave 도메인의 uploadFile 경로를 사용한다.
        예)
          https://tcar.lotterentacar.net/2026/03/19/LFILE_....png
        -> https://img-mycarsave.lotterentacar.net/uploadFile/2026/03/19/LFILE_....png/dims/resize/300x220
        """
        u = (raw_url or "").strip()
        if not u:
            return ""
        # already usable
        if "img-mycarsave.lotterentacar.net" in u:
            return u
        m = re.search(r"lotterentacar\.net/(\d{4})/(\d{2})/(\d{2})/(LFILE_[^/?#]+\.png)", u)
        if m:
            yyyy, mm, dd, fname = m.group(1), m.group(2), m.group(3), m.group(4)
            return f"https://img-mycarsave.lotterentacar.net/uploadFile/{yyyy}/{mm}/{dd}/{fname}/dims/resize/300x220"
        return u
    pending_rows: list[tuple[int, str, str]] = []
    for idx, row in enumerate(rows):
        product_id = str(row.get("product_id") or "").strip()
        if not product_id:
            continue
        image_url = str(image_url_map.get(product_id) or row.get("image_url") or "").strip()
        image_url = _normalize_thumbnail_url(image_url)
        if image_url:
            pending_rows.append((idx, product_id, image_url))

    if not pending_rows:
        logger.warning("다운로드할 목록 이미지 URL이 없습니다: %s", csv_path)
        return

    def _download_thumbnail(idx: int, product_id: str, image_url: str):
        out_path = img_dir / f"{product_id}_list.png"
        # 리퍼러/오리진을 강하게 맞춰서 403 방지(현장에서 이미지 저장이 안 되는 이슈 대응)
        referers = [
            "https://tcar.lotterentacar.net/cr/search/list",
            "https://tcar.lotterentacar.net/cr/search/list#page:1._.rows:15._.saleTyAll:true",
            "https://tcar.lotterentacar.net/",
        ]
        last_err: Exception | None = None
        r = None
        for ref in referers:
            try:
                headers = {
                    **REQUEST_HEADERS,
                    "Referer": ref,
                    "Origin": "https://tcar.lotterentacar.net",
                    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                }
                r = requests.get(
                    image_url,
                    headers=headers,
                    timeout=30,
                    allow_redirects=True,
                )
                r.raise_for_status()
                ctype = (r.headers.get("Content-Type") or "").lower()
                if ctype and "image" not in ctype:
                    raise ValueError(f"non-image content-type: {ctype}")
                break
            except Exception as e:
                last_err = e
                r = None
                continue
        if r is None:
            raise last_err or RuntimeError("thumbnail download failed")
        with open(out_path, "wb") as out:
            out.write(r.content)
        return idx, product_id, out_path

    saved = 0
    failed = 0
    total = len(pending_rows)
    logger.info("리스트 썸네일 병렬 수집 시작: 총 %d건 대상 (CSV=%s)", total, csv_path)
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {
            executor.submit(_download_thumbnail, idx, product_id, image_url): (idx, product_id)
            for idx, product_id, image_url in pending_rows
        }
        for seq, future in enumerate(as_completed(future_map), start=1):
            idx, product_id = future_map[future]
            try:
                _, _, out_path = future.result()
                rows[idx]["car_imgs"] = str(Path(out_path).resolve())
                saved += 1
            except Exception as e:
                failed += 1
                # 현장 디버깅을 위해 처음 몇 건은 INFO로 노출
                if failed <= 10:
                    logger.info("[list-thumb %s] 썸네일 저장 실패: %s", product_id, e)
                else:
                    logger.debug("[list-thumb %s] 썸네일 저장 실패: %s", product_id, e)
                rows[idx]["car_imgs"] = rows[idx].get("car_imgs") or ""

            if seq % 100 == 0 or seq == total:
                flush_csv()
                logger.info(
                    "리스트 썸네일 진행 상태: %d/%d건 처리, 썸네일 %d건 저장",
                    seq,
                    total,
                    saved,
                )

    flush_csv()
    logger.info(
        "리스트 썸네일 수집 완료: 총 %d건 처리, 썸네일 %d건 저장 (car_imgs=저장 파일 절대경로, 디렉터리=%s)",
        total,
        saved,
        img_dir_abs,
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


def _normalize_target_table(raw: str | None) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    s = s.strip('"').strip("'").strip()
    s = s.replace('"ods."', "ods.").replace("'ods.'", "ods.")
    s = s.replace('"', "").replace("'", "").strip()
    if not s:
        return None
    if "." not in s:
        return s
    parts = [p for p in s.split(".") if p]
    if len(parts) >= 2:
        return f"{parts[0]}.{parts[1]}"
    return s


def _resolve_target_table_for_datst(datst_cd: str, raw: str | None) -> str:
    normalized = _normalize_target_table(raw)
    key = (datst_cd or "").lower().strip()
    if key == LOTTERENTACAR_DATST_BRAND:
        return normalized or LOTTERENTACAR_BRAND_TABLE
    if key == LOTTERENTACAR_DATST_CAR_TYPE:
        return normalized or LOTTERENTACAR_CAR_TYPE_TABLE
    if key == LOTTERENTACAR_DATST_LIST:
        return normalized or LOTTERENTACAR_TMP_LIST_TABLE
    if normalized:
        return normalized
    raise ValueError(f"적재 대상 테이블을 확인할 수 없습니다: datst_cd={datst_cd}")


def _resolve_list_table_targets(tn_data_bsc_info: TnDataBscInfo) -> tuple[str, str]:
    tmp_table = (
        _normalize_target_table(getattr(tn_data_bsc_info, "tmpr_tbl_phys_nm", None))
        or LOTTERENTACAR_TMP_LIST_TABLE
    )
    source_table = (
        _normalize_target_table(getattr(tn_data_bsc_info, "ods_tbl_phys_nm", None))
        or LOTTERENTACAR_SOURCE_LIST_TABLE
    )
    return tmp_table, source_table


def _dedupe_lotterentacar_list_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen_product_ids: set[str] = set()
    for row in rows:
        product_id = str(row.get("product_id") or "").strip()
        if product_id:
            if product_id in seen_product_ids:
                continue
            seen_product_ids.add(product_id)
        deduped.append(row)
    return deduped


def _read_csv_rows(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _split_schema_table(full_name: str) -> tuple[str, str]:
    return CommonUtil.split_schema_table(full_name)


def _get_table_columns(hook: PostgresHook, full_table_name: str) -> list[str]:
    schema, table = _split_schema_table(full_table_name)
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = %s
    ORDER BY ordinal_position
    """
    rows = hook.get_records(sql, parameters=(schema, table))
    return [r[0] for r in rows]


def _bulk_insert_rows(
    hook: PostgresHook,
    full_table_name: str,
    rows: list[dict[str, Any]],
    *,
    truncate: bool = False,
    extra_static_cols: dict[str, Any] | None = None,
    allow_only_table_cols: bool = True,
):
    if not rows:
        return
    extra_static_cols = extra_static_cols or {}
    table_cols = _get_table_columns(hook, full_table_name) if allow_only_table_cols else []
    table_col_set = set(table_cols)

    candidate_cols: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in candidate_cols:
                candidate_cols.append(key)
    for key in extra_static_cols.keys():
        if key not in candidate_cols:
            candidate_cols.append(key)

    insert_cols = (
        [c for c in candidate_cols if c in table_col_set]
        if allow_only_table_cols and table_cols
        else candidate_cols
    )
    if not insert_cols:
        raise ValueError(f"insert 가능한 컬럼이 없습니다. table={full_table_name}")

    values = []
    for row in rows:
        merged = {**row, **extra_static_cols}
        values.append(tuple(merged.get(c) for c in insert_cols))

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            if truncate:
                cur.execute(f"TRUNCATE TABLE {full_table_name}")
            from psycopg2.extras import execute_values

            cols_sql = ", ".join([f'"{c}"' for c in insert_cols])
            sql = f"INSERT INTO {full_table_name} ({cols_sql}) VALUES %s"
            execute_values(cur, sql, values, page_size=2000)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _bulk_update_rows_by_key(
    hook: PostgresHook,
    full_table_name: str,
    rows: list[dict[str, Any]],
    *,
    key_col: str,
    allow_only_table_cols: bool = True,
) -> None:
    if not rows:
        return

    table_cols = _get_table_columns(hook, full_table_name) if allow_only_table_cols else []
    table_col_set = set(table_cols)

    candidate_cols: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in candidate_cols:
                candidate_cols.append(key)

    if key_col not in candidate_cols:
        raise ValueError(
            f"update 키 컬럼이 없습니다. table={full_table_name}, key_col={key_col}"
        )

    value_cols = [key_col] + [
        c
        for c in candidate_cols
        if c != key_col and (c in table_col_set if table_cols else True)
    ]
    update_cols = [c for c in value_cols if c != key_col]
    if not update_cols:
        return

    values = [
        tuple(row.get(col) for col in update_cols) + (row.get(key_col),)
        for row in rows
        if _normalize_compare_value(row.get(key_col))
    ]
    if not values:
        return

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_batch

            set_sql = ", ".join([f'"{col}" = %s' for col in update_cols])
            sql = f"""
            UPDATE {full_table_name}
            SET {set_sql}
            WHERE COALESCE("{key_col}"::text, '') = COALESCE(%s::text, '')
            """
            execute_batch(cur, sql, values, page_size=500)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _delete_snapshot_rows(
    hook: PostgresHook,
    full_table_name: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return

    table_cols = set(_get_table_columns(hook, full_table_name))
    create_dt_values = sorted(
        {
            str(row.get("create_dt") or "").strip()
            for row in rows
            if str(row.get("create_dt") or "").strip()
        }
    )
    date_values = sorted(
        {
            str(row.get("date_crtr_pnttm") or "").strip()
            for row in rows
            if str(row.get("date_crtr_pnttm") or "").strip()
        }
    )

    sql = None
    params: tuple[Any, ...] = ()
    if "create_dt" in table_cols and create_dt_values:
        sql = f'DELETE FROM {full_table_name} WHERE "create_dt" = ANY(%s)'
        params = (create_dt_values,)
    elif "date_crtr_pnttm" in table_cols and date_values:
        sql = f'DELETE FROM {full_table_name} WHERE "date_crtr_pnttm" = ANY(%s)'
        params = (date_values,)

    if not sql:
        return

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _normalize_compare_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _build_row_key(row: dict[str, Any], key_cols: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(_normalize_compare_value(row.get(col)) for col in key_cols)


def _pick_snapshot_audit_values(rows: list[dict[str, Any]]) -> tuple[str, str]:
    date_crtr_pnttm = ""
    create_dt = ""
    for row in rows:
        row_date = _normalize_compare_value(row.get("date_crtr_pnttm"))
        row_create_dt = _normalize_compare_value(row.get("create_dt"))
        if row_date and row_date > date_crtr_pnttm:
            date_crtr_pnttm = row_date
        if row_create_dt and row_create_dt > create_dt:
            create_dt = row_create_dt
    return date_crtr_pnttm, create_dt


def _rows_differ(
    current_row: dict[str, Any], latest_row: dict[str, Any], compare_cols: list[str]
) -> bool:
    for col in compare_cols:
        if _normalize_compare_value(current_row.get(col)) != _normalize_compare_value(
            latest_row.get(col)
        ):
            return True
    return False


def _fetch_latest_source_rows(
    hook: PostgresHook,
    source_table: str,
    key_cols: tuple[str, ...],
    order_cols: list[str],
) -> dict[tuple[str, ...], dict[str, Any]]:
    key_expr = ", ".join([f'"{col}"' for col in key_cols])
    order_expr = ", ".join(
        [f'COALESCE("{col}", \'\') DESC' for col in order_cols] + ["ctid DESC"]
    )
    sql = f"""
    WITH latest AS (
        SELECT DISTINCT ON ({key_expr})
            ctid::text AS _row_ctid,
            *
        FROM {source_table}
        ORDER BY {key_expr}, {order_expr}
    )
    SELECT * FROM latest
    """
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            latest_rows = [dict(zip(cols, row)) for row in cur.fetchall() or []]
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {
        _build_row_key(row, key_cols): row
        for row in latest_rows
        if any(_normalize_compare_value(row.get(col)) for col in key_cols)
    }


def _sync_lotterentacar_tmp_to_source(
    hook: PostgresHook,
    *,
    current_rows: list[dict[str, Any]],
    tmp_table: str,
    source_table: str,
    key_cols: tuple[str, ...] = ("product_id", "detail_url"),
    flag_col: str = "register_flag",
):
    src_cols = _get_table_columns(hook, source_table)
    src_col_set = set(src_cols)
    src_has_flag = flag_col in src_col_set
    missing_key_cols = [col for col in key_cols if col not in src_col_set]
    if missing_key_cols:
        raise ValueError(f"source 테이블 키 컬럼 누락: table={source_table}, cols={missing_key_cols}")

    tmp_count = CommonUtil.get_table_row_count(hook, tmp_table)
    if not current_rows:
        return {
            "tmp_count": tmp_count,
            "current_row_count": 0,
            "inserted_count": 0,
            "marked_existing_count": 0,
            "updated_changed_count": 0,
            "marked_missing_count": 0,
            "source_count": CommonUtil.get_table_row_count(hook, source_table),
        }

    current_rows = _dedupe_lotterentacar_list_rows(current_rows)
    date_crtr_pnttm, create_dt = _pick_snapshot_audit_values(current_rows)
    order_cols = [col for col in ("create_dt", "date_crtr_pnttm") if col in src_col_set]

    update_key_col = key_cols[0]  # product_id
    latest_source_map = _fetch_latest_source_rows(hook, source_table, (update_key_col,), order_cols)

    compare_exclude_cols = set(key_cols) | {"model_sn", "date_crtr_pnttm", "create_dt", flag_col, "_row_ctid"}
    compare_cols = [col for col in src_cols if col in current_rows[0] and col not in compare_exclude_cols]

    current_update_key_set: set[str] = set()
    rows_to_insert: list[dict[str, Any]] = []
    rows_to_update_changed: list[dict[str, Any]] = []
    rows_to_mark_existing: list[dict[str, Any]] = []
    missing_product_ids: list[str] = []

    for current_row in current_rows:
        update_key = _normalize_compare_value(current_row.get(update_key_col))
        compare_key = _build_row_key(current_row, key_cols)
        if not update_key:
            continue
        current_update_key_set.add(update_key)

        latest_row = latest_source_map.get((update_key,))
        if latest_row is None:
            row_to_insert = {col: current_row.get(col) for col in src_cols if col in current_row}
            if src_has_flag:
                row_to_insert[flag_col] = "A"
            rows_to_insert.append(row_to_insert)
            continue

        latest_flag = _normalize_compare_value(latest_row.get(flag_col))
        latest_compare_key = _build_row_key(latest_row, key_cols)
        has_changes = compare_key != latest_compare_key or _rows_differ(current_row, latest_row, compare_cols)

        if has_changes:
            row_to_update = {col: current_row.get(col) for col in src_cols if col in current_row}
            if src_has_flag:
                row_to_update[flag_col] = "Y"
            rows_to_update_changed.append(row_to_update)
            continue

        if src_has_flag and latest_flag != "Y":
            rows_to_mark_existing.append(
                {
                    update_key_col: current_row.get(update_key_col),
                    flag_col: "Y",
                }
            )

    for row_key, latest_row in latest_source_map.items():
        latest_flag = _normalize_compare_value(latest_row.get(flag_col))
        if row_key[0] not in current_update_key_set and latest_flag != "N":
            product_id = _normalize_compare_value(latest_row.get(update_key_col))
            if product_id:
                missing_product_ids.append(product_id)

    if rows_to_update_changed:
        _bulk_update_rows_by_key(
            hook,
            source_table,
            rows_to_update_changed,
            key_col=update_key_col,
            allow_only_table_cols=True,
        )

    if rows_to_mark_existing:
        _bulk_update_rows_by_key(
            hook,
            source_table,
            rows_to_mark_existing,
            key_col=update_key_col,
            allow_only_table_cols=True,
        )

    if rows_to_insert:
        _bulk_insert_rows(
            hook,
            source_table,
            rows_to_insert,
            truncate=False,
            allow_only_table_cols=True,
        )

    if missing_product_ids and src_has_flag:
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                set_parts = [f'"{flag_col}" = %s']
                params: list[Any] = ["N"]
                if "date_crtr_pnttm" in src_col_set and date_crtr_pnttm:
                    set_parts.append('"date_crtr_pnttm" = %s')
                    params.append(date_crtr_pnttm)
                if "create_dt" in src_col_set and create_dt:
                    set_parts.append('"create_dt" = %s')
                    params.append(create_dt)
                params.append(missing_product_ids)
                cur.execute(
                    f"""
                    UPDATE {source_table}
                    SET {", ".join(set_parts)}
                    WHERE "{update_key_col}" = ANY(%s)
                    """,
                    tuple(params),
                )
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass

    return {
        "tmp_count": tmp_count,
        "current_row_count": len(current_rows),
        "inserted_count": len(rows_to_insert),
        "marked_existing_count": len(rows_to_mark_existing),
        "updated_changed_count": len(rows_to_update_changed),
        "marked_missing_count": len(missing_product_ids),
        "source_count": CommonUtil.get_table_row_count(hook, source_table),
    }


def _run_lotterentacar_car_type_csv(datst_cd: str) -> str:
    """차종 CSV 수집 Task (data17)."""
    activate_paths_for_datst((datst_cd or LOTTERENTACAR_DATST_CAR_TYPE).lower() or LOTTERENTACAR_DATST_CAR_TYPE)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    car_type_csv = RESULT_DIR / f"lotterentacar_car_type_list_{run_ts}.csv"
    logger.info("차종 CSV 저장 경로: %s", car_type_csv)
    run_lotterentacar_car_type_list_via_ajax(RESULT_DIR, logger, csv_path=car_type_csv)
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
    run_lotterentacar_brand_list_via_ajax(RESULT_DIR, logger, csv_path=brand_csv)
    return str(brand_csv)


def run_lotterentacar_list_job(
    bsc: TnDataBscInfo,
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
    _clear_directory_contents(IMG_BASE / "list")

    logger = _get_file_logger(run_ts)
    brand_path = Path(brand_list_csv_path)
    if not brand_path.is_file():
        raise FileNotFoundError(
            f"브랜드 CSV가 없습니다. run_brand_csv 태스크를 먼저 성공시키고 brand_list_csv_path를 확인하세요: {brand_path}"
        )
    if car_type_csv_path and not Path(car_type_csv_path).is_file():
        logger.warning("차종 CSV 경로 없음(무시하고 진행): %s", car_type_csv_path)

    logger.info("🏁 롯데렌터카 목록 수집 시작")
    logger.info("- pvsn_site_cd=%s, datst_cd=%s, datst_nm=%s", bsc.pvsn_site_cd, bsc.datst_cd, bsc.datst_nm)
    logger.info("- link_url=%s", bsc.link_data_clct_url)
    logger.info("- 브랜드 CSV(이전 태스크): %s", brand_path)

    list_path = RESULT_DIR / f"lotterentacar_list_{run_ts}.csv"
    if list_path.exists():
        list_path.unlink()

    list_result = run_lotterentacar_list_via_ajax(
        RESULT_DIR,
        logger,
        list_csv_path=list_path,
    )
    run_lotterentacar_fetch_list_images(
        RESULT_DIR,
        logger,
        list_csv_path=list_path,
        img_dir=IMG_BASE / "list",
        image_url_map=list_result.get("image_url_map"),
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
    run_lotterentacar_car_type_list_via_ajax(result_dir, logger)
    run_lotterentacar_brand_list_via_ajax(result_dir, logger)

    # 2) 목록 수집: Network AJAX API 사용 → lotterentacar_list.csv
    list_result = run_lotterentacar_list_via_ajax(result_dir, logger)

    # 3) 상세 페이지 이미지: list.csv의 product_id로 각 상세 페이지 접속 후 이미지 저장, car_imgs 컬럼 채움 (브라우저)
    #    (여러 장을 저장하고 싶을 때 사용)
    # run_lotterentacar_fetch_detail_images(result_dir, logger)

    # 4) 상세 페이지 첫 번째 이미지를 썸네일로 저장하여 list 폴더 및 car_imgs 컬럼 채움
    run_lotterentacar_fetch_list_images(
        result_dir,
        logger,
        image_url_map=list_result.get("image_url_map"),
    )


if __name__ == "__main__":
    main()
