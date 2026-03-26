import csv
import logging
import re
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

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


@dag(
    dag_id="sdag_heydealer_crawler",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["used_car", "heydealer", "crawler", "day"],
)
def heydealer_crawler():
    """
    헤이딜러(중고차) 브랜드/차종/목록 수집 DAG.

    - DB 메타(std.tn_data_bsc_info, ps00002 data4/5/6) 조회
    - 브랜드/차종은 API로 CSV 생성
    - 목록은 playwright로 사이트 크롤링 후 CSV/이미지 저장
    """

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id="car_db_conn")

    @task
    def insert_collect_data_info(**kwargs) -> dict[str, dict[str, Any]]:
        """std.tn_data_bsc_info에서 헤이딜러(ps00002) 수집 대상 기본 정보 조회."""
        select_bsc_info_stmt = f"""
        SELECT * FROM std.tn_data_bsc_info tdbi
        WHERE 1=1
          AND LOWER(clct_yn) = 'y'
          AND LOWER(link_yn) = 'y'
          AND LOWER(pvsn_site_cd) = '{HEYDEALER_PVSN_SITE_CD}'
          AND LOWER(datst_cd) IN ('{HEYDEALER_DATST_BRAND}','{HEYDEALER_DATST_CAR_TYPE}','{HEYDEALER_DATST_LIST}')
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
                    dto = CommonUtil.build_bsc_info_dto(raw)
                    key = str(dto.datst_cd or "").lower().strip()
                    if key and key not in out:
                        out[key] = CommonUtil.bsc_info_to_dict(dto)
        finally:
            try:
                conn.close()
            except Exception:
                pass

        missing = [
            k
            for k in (
                HEYDEALER_DATST_BRAND,
                HEYDEALER_DATST_CAR_TYPE,
                HEYDEALER_DATST_LIST,
            )
            if k not in out
        ]
        if missing:
            raise ValueError(
                f"std.tn_data_bsc_info 조회 결과 누락: {missing} (pvsn_site_cd={HEYDEALER_PVSN_SITE_CD})"
            )
        return out

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        dc = str((infos.get(HEYDEALER_DATST_BRAND) or {}).get("datst_cd") or HEYDEALER_DATST_BRAND).lower()
        return _run_heydealer_brand_csv(dc, kwargs=kwargs)

    @task
    def run_car_type_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        dc = str((infos.get(HEYDEALER_DATST_CAR_TYPE) or {}).get("datst_cd") or HEYDEALER_DATST_CAR_TYPE).lower()
        return _run_heydealer_car_type_csv(dc, kwargs=kwargs)

    @task
    def run_list_csv(
        bsc_infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        car_type_csv_path: str,
        **kwargs,
    ) -> dict[str, Any]:
        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[HEYDEALER_DATST_LIST])
        return run_heydealer_job(
            tn_data_bsc_info,
            brand_list_csv_path=brand_csv_path,
            car_type_csv_path=car_type_csv_path,
            kwargs=kwargs,
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
            raise FileNotFoundError(f"수집 메타 등록 대상 CSV가 없습니다: datst_cd={datst_cd}, path={csv_file_path}")

        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[datst_cd])
        tn_data_clct_dtl_info = CommonUtil.upsert_collect_detail_info(
            hook,
            HEYDEALER_COLLECT_DETAIL_TABLE,
            tn_data_bsc_info.datst_cd,
            csv_file_path,
        )
        registered_file_path = str(CommonUtil.build_collect_detail_file_path(tn_data_clct_dtl_info))
        logging.info(
            "헤이딜러 수집 메타 등록: datst_cd=%s, file=%s, clct_pnttm=%s, status=%s",
            datst_cd,
            registered_file_path,
            tn_data_clct_dtl_info.clct_pnttm,
            getattr(tn_data_clct_dtl_info, "status", ""),
        )
        tn_data_clct_dtl_info_dict = tn_data_clct_dtl_info.as_dict()
        tn_data_clct_dtl_info_dict["file_path"] = registered_file_path
        tn_data_clct_dtl_info_dict["status"] = getattr(tn_data_clct_dtl_info, "status", "")
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
            HEYDEALER_COLLECT_DETAIL_TABLE,
            datst_cd,
        )
        if not tn_data_clct_dtl_info:
            raise ValueError(f"최신 수집 메타 정보가 없습니다: datst_cd={datst_cd}")

        latest_csv_path = CommonUtil.build_collect_detail_file_path(tn_data_clct_dtl_info)
        if not latest_csv_path.is_file():
            raise ValueError(
                f"CSV 수집 완료 조건 미충족: datst_cd={datst_cd}, exists={latest_csv_path.is_file()}, path={latest_csv_path}"
            )

        target_table = _resolve_target_table_for_datst(datst_cd, tn_data_bsc_info.ods_tbl_phys_nm)
        logging.info(
            "헤이딜러 최신 CSV 메타 선택: datst_cd=%s, selected=%s, clct_pnttm=%s, file_nm=%s",
            datst_cd,
            latest_csv_path,
            tn_data_clct_dtl_info.clct_pnttm or "",
            tn_data_clct_dtl_info.clct_data_file_nm or "",
        )
        rows = _read_csv_rows(latest_csv_path)
        if not rows:
            raise ValueError(f"적재할 CSV 데이터가 없습니다: datst_cd={datst_cd}, path={latest_csv_path}")
        if datst_cd == HEYDEALER_DATST_LIST:
            rows = _dedupe_heydealer_list_rows(rows)
            target_table, _ = _resolve_list_table_targets(tn_data_bsc_info)

        if datst_cd in (HEYDEALER_DATST_BRAND, HEYDEALER_DATST_CAR_TYPE, HEYDEALER_DATST_LIST):
            _delete_snapshot_rows(hook, target_table, rows)

        _bulk_insert_rows(hook, target_table, rows, truncate=False, allow_only_table_cols=True)
        table_count = CommonUtil.get_table_row_count(hook, target_table)
        logging.info(
            "헤이딜러 CSV 적재 완료: datst_cd=%s, table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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

        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[HEYDEALER_DATST_LIST])
        tmp_table, source_table = _resolve_list_table_targets(tn_data_bsc_info)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        current_rows = _read_csv_rows(Path(str(list_load_result.get("csv_path") or "")))
        sync_result = _sync_tmp_and_source_register_flag(
            hook,
            current_rows=current_rows,
            tmp_table=tmp_table,
            source_table=source_table,
        )
        logging.info(
            "헤이딜러 list tmp->source 반영 완료: tmp_table=%s, source_table=%s, tmp_count=%d, current_row_count=%d, inserted_count=%d, marked_existing_count=%d, updated_changed_count=%d, marked_missing_count=%d, source_count=%d",
            tmp_table,
            source_table,
            sync_result["tmp_count"],
            sync_result["current_row_count"],
            sync_result["inserted_count"],
            sync_result["marked_existing_count"],
            sync_result["updated_changed_count"],
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
            "updated_changed_count": sync_result["updated_changed_count"],
            "marked_missing_count": sync_result["marked_missing_count"],
            "source_count": sync_result["source_count"],
        }

    @task_group(group_id="create_csv_process")
    def create_csv_process(bsc_infos: dict[str, dict[str, Any]]) -> dict[str, Any]:
        brand_path = run_brand_csv(bsc_infos)
        brand_collect_info = register_csv_collect_log_info.override(
            task_id="register_brand_collect_log_info"
        )(bsc_infos, HEYDEALER_DATST_BRAND, brand_path)

        car_type_path = run_car_type_csv(bsc_infos)
        car_type_collect_info = register_csv_collect_log_info.override(
            task_id="register_car_type_collect_log_info"
        )(bsc_infos, HEYDEALER_DATST_CAR_TYPE, car_type_path)

        list_result = run_list_csv(bsc_infos, brand_path, car_type_path)
        list_collect_info = register_csv_collect_log_info.override(
            task_id="register_list_collect_log_info"
        )(bsc_infos, HEYDEALER_DATST_LIST, list_result["list_csv"])

        return {
            HEYDEALER_DATST_BRAND: brand_collect_info,
            HEYDEALER_DATST_CAR_TYPE: car_type_collect_info,
            HEYDEALER_DATST_LIST: list_collect_info,
        }

    @task_group(group_id="insert_csv_process")
    def insert_csv_process(
        bsc_infos: dict[str, dict[str, Any]],
        tn_data_clct_dtl_info_map: dict[str, dict[str, Any]],
    ) -> None:
        brand_load_result = load_csv_to_ods.override(task_id="load_brand_csv_to_ods")(bsc_infos, tn_data_clct_dtl_info_map, HEYDEALER_DATST_BRAND)
        car_type_load_result = load_csv_to_ods.override(task_id="load_car_type_csv_to_ods")(bsc_infos, tn_data_clct_dtl_info_map, HEYDEALER_DATST_CAR_TYPE)
        list_load_result = load_csv_to_ods.override(task_id="load_list_csv_to_ods")(bsc_infos, tn_data_clct_dtl_info_map, HEYDEALER_DATST_LIST)
        sync_list_tmp_to_source.override(task_id="sync_list_tmp_to_source")(
            bsc_infos,
            brand_load_result,
            car_type_load_result,
            list_load_result,
        )

    infos = insert_collect_data_info()
    tn_data_clct_dtl_info_map = create_csv_process(infos)
    insert_csv_process(infos, tn_data_clct_dtl_info_map)


# Airflow Variable 키
#   (UI에서 소문자 key 사용: used_car_site_names)
USED_CAR_SITE_NAMES_VAR = "used_car_site_names"
CRAWL_BASE_PATH_VAR = "crawl_base_path"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
COLLECT_LOG_FILE_PATH_VAR = "used_car_collect_log_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"

HEYDEALER_PVSN_SITE_CD = "ps00002"
HEYDEALER_DATST_BRAND = "data4"
HEYDEALER_DATST_CAR_TYPE = "data5"
HEYDEALER_DATST_LIST = "data6"
HEYDEALER_SITE_NAME = "헤이딜러"
HEYDEALER_BRAND_TABLE = "ods.ods_brand_list_heydealer"
HEYDEALER_CAR_TYPE_TABLE = "ods.ods_car_type_list_heydealer"
HEYDEALER_TMP_LIST_TABLE = "ods.ods_tmp_car_list_heydealer"
HEYDEALER_SOURCE_LIST_TABLE = "ods.ods_car_list_heydealer"
HEYDEALER_COLLECT_DETAIL_TABLE = "std.tn_data_clct_dtl_info"


def _get_context_var_value(kwargs: dict[str, Any] | None, var_name: str) -> Any:
    if not kwargs:
        return None
    try:
        var_ctx = kwargs.get("var")
        if isinstance(var_ctx, dict):
            accessor = var_ctx.get("value")
            if isinstance(accessor, dict):
                return accessor.get(var_name)
            if accessor is not None:
                return getattr(accessor, var_name, None)
        if var_ctx is not None:
            accessor = getattr(var_ctx, "value", None)
            if accessor is not None:
                return getattr(accessor, var_name, None)
    except Exception:
        return None
    return None


def _get_context_var_json(kwargs: dict[str, Any] | None, var_name: str) -> Any:
    if not kwargs:
        return None
    try:
        var_ctx = kwargs.get("var")
        if isinstance(var_ctx, dict):
            accessor = var_ctx.get("json")
            if isinstance(accessor, dict):
                return accessor.get(var_name)
            if accessor is not None:
                return getattr(accessor, var_name, None)
        if var_ctx is not None:
            accessor = getattr(var_ctx, "json", None)
            if accessor is not None:
                return getattr(accessor, var_name, None)
    except Exception:
        return None
    return None


def _get_variable_path(var_name: str, kwargs: dict[str, Any] | None = None) -> Path | None:
    ctx_value = _get_context_var_value(kwargs, var_name)
    if ctx_value and str(ctx_value).strip():
        return Path(str(ctx_value).strip())
    try:
        value = Variable.get(var_name, default_var=None)
        if value and str(value).strip():
            return Path(str(value).strip())
    except Exception:
        pass
    return None


def _get_crawl_base_path(kwargs: dict[str, Any] | None = None) -> Path:
    base = _get_variable_path(CRAWL_BASE_PATH_VAR, kwargs=kwargs)
    if base is not None:
        return base
    legacy = _get_variable_path("HEYDEALER_BASE_PATH", kwargs=kwargs)
    if legacy is not None:
        return legacy
    return Path("/home/limhayoung/data")


def _get_result_root_path(kwargs: dict[str, Any] | None = None) -> Path:
    direct = _get_variable_path(FINAL_FILE_PATH_VAR, kwargs=kwargs)
    if direct is not None:
        return direct
    return _get_crawl_base_path(kwargs=kwargs) / "crawl"


def _get_log_root_path(kwargs: dict[str, Any] | None = None) -> Path:
    direct = _get_variable_path(COLLECT_LOG_FILE_PATH_VAR, kwargs=kwargs)
    if direct is not None:
        return direct
    return _get_crawl_base_path(kwargs=kwargs) / "log"


def _get_img_root_path(kwargs: dict[str, Any] | None = None) -> Path:
    direct = _get_variable_path(IMAGE_FILE_PATH_VAR, kwargs=kwargs)
    if direct is not None:
        return direct
    return _get_crawl_base_path(kwargs=kwargs) / "img"


def _build_year_site_path(root_path: str | Path, site_name: str, dt: datetime | None = None) -> Path:
    now = dt or datetime.now()
    return Path(root_path) / now.strftime("%Y년") / site_name


def _clear_directory_contents(dir_path: Path) -> None:
    if not dir_path.exists():
        return
    for child in dir_path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def get_site_name_by_datst(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    """Variable USED_CAR_SITE_NAMES(JSON)에서 datst_cd에 해당하는 한글 폴더명 조회."""
    key = (datst_cd or "").lower().strip()
    raw_mapping = _get_context_var_json(kwargs, USED_CAR_SITE_NAMES_VAR)
    mapping: dict[str, Any] = {}
    if isinstance(raw_mapping, dict):
        mapping = {str(k).lower(): v for k, v in raw_mapping.items()}
    try:
        if not mapping:
            raw = Variable.get(
                USED_CAR_SITE_NAMES_VAR,
                default_var="{}",
                deserialize_json=True,
            )
            if isinstance(raw, dict):
                mapping = {str(k).lower(): v for k, v in raw.items()}
    except Exception:
        pass
    name = mapping.get(key)
    if name is not None and str(name).strip():
        return str(name).strip()
    return HEYDEALER_SITE_NAME


def _build_logger_for_file(
    log_path: Path, logger_name: str, *, to_stdout: bool = True
) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    if to_stdout:
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    logger.propagate = False
    return logger


def get_heydealer_site_name(kwargs: dict[str, Any] | None = None) -> str:
    return get_site_name_by_datst(HEYDEALER_DATST_LIST, kwargs=kwargs)


def activate_paths_for_datst(datst_cd: str, kwargs: dict[str, Any] | None = None) -> None:
    """
    datst_cd 기준으로 crawl/log/img 경로 및 CSV 파일명을 설정한다.
    (각 Task / run_heydealer_job 시작 시 호출)
    """
    global RESULT_DIR, LOG_DIR, IMG_BASE, IMG_LIST_REL, DETAIL_IMG_REL
    global BRAND_LIST_FILE, CAR_TYPE_LIST_FILE, LIST_FILE, LOG_FILE, BRAND_HIERARCHY_LOG
    global _logger_brand, YEAR_STR, DATE_STR, RUN_TS

    result_root = _get_result_root_path(kwargs=kwargs)
    log_root = _get_log_root_path(kwargs=kwargs)
    img_root = _get_img_root_path(kwargs=kwargs)
    site = get_site_name_by_datst(datst_cd, kwargs=kwargs)
    now = datetime.now()
    YEAR_STR = now.strftime("%Y년")
    DATE_STR = now.strftime("%Y%m%d")
    RUN_TS = now.strftime("%Y%m%d%H%M")

    RESULT_DIR = CommonUtil.build_dated_site_path(result_root, site, now)
    LOG_DIR = CommonUtil.build_dated_site_path(log_root, site, now)
    IMG_BASE = _build_year_site_path(img_root, site, now)
    IMG_LIST_REL = f"data/img/{YEAR_STR}/{site}/list"
    DETAIL_IMG_REL = f"data/img/{YEAR_STR}/{site}/detail"

    BRAND_LIST_FILE = RESULT_DIR / f"heydealer_brand_list_{RUN_TS}.csv"
    CAR_TYPE_LIST_FILE = RESULT_DIR / f"heydealer_car_type_list_{RUN_TS}.csv"
    LIST_FILE = RESULT_DIR / f"heydealer_list_{RUN_TS}.csv"

    LOG_FILE = LOG_DIR / "heydealer_type_to_list.log"
    BRAND_HIERARCHY_LOG = LOG_DIR / "heydealer_brand_hierarchy.log"
    # 브랜드 상세 로그는 파일만 (Airflow task 로그에 안 뿌림)
    _logger_brand = _build_logger_for_file(
        BRAND_HIERARCHY_LOG, "heydealer_brand", to_stdout=False
    )


def _normalize_heydealer_list_url(link_url: str) -> str:
    """기본 목록 URL을 정규화한다. 쿼리는 path 앞이 아니라 URL query 위치에 유지된다."""
    raw = (link_url or "").strip() or "https://www.heydealer.com/market/cars"
    parsed = urlparse(raw)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or "www.heydealer.com"
    path = parsed.path or "/market/cars"
    query = urlencode(parse_qsl(parsed.query, keep_blank_values=True), doseq=True)
    return urlunparse((scheme, netloc, path, "", query, parsed.fragment))


def _build_heydealer_list_url(link_url: str, *, car_shape: str | None = None) -> str:
    """
    헤이딜러 목록 URL 생성.
    - 기본 목록은 `link_url`의 path/query/fragment를 유지
    - 차종 필터(`car-shape`)도 목록 경로(`/market/cars`) query에 반영
    """
    base_url = _normalize_heydealer_list_url(link_url)
    parsed = urlparse(base_url)
    path = parsed.path or "/market/cars"
    fragment = parsed.fragment

    query_pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != "car-shape"]
    if car_shape:
        query_pairs.append(("car-shape", str(car_shape)))
    query = urlencode(query_pairs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, path, "", query, fragment))


def _normalize_target_table(raw: str | None) -> str | None:
    """
    예시 입력: '"ods."ods_brand_list_heydealer'
    - 양끝 따옴표 제거
    - '"ods."' 같은 prefix가 섞여있으면 제거
    - 결과: 'ods.ods_brand_list_heydealer'
    """
    if not raw:
        return None
    s = str(raw).strip()
    s = s.strip('"').strip("'").strip()
    s = s.replace('"ods."', "ods.").replace("'ods.'", "ods.").replace("'ods.'", "ods.")
    s = s.replace('"ods."', "ods.").replace('"', "").replace("'", "").strip()
    if not s:
        return None
    if "." not in s:
        return s
    parts = [p for p in s.split(".") if p]
    if len(parts) >= 2:
        return parts[0] + "." + parts[1]
    return s


def _resolve_target_table_for_datst(datst_cd: str, raw: str | None) -> str:
    normalized = _normalize_target_table(raw)
    key = (datst_cd or "").lower().strip()
    if key == HEYDEALER_DATST_BRAND:
        return normalized or HEYDEALER_BRAND_TABLE
    if key == HEYDEALER_DATST_CAR_TYPE:
        return normalized or HEYDEALER_CAR_TYPE_TABLE
    if key == HEYDEALER_DATST_LIST:
        return normalized or HEYDEALER_TMP_LIST_TABLE
    if normalized:
        return normalized
    raise ValueError(f"적재 대상 테이블을 확인할 수 없습니다: datst_cd={datst_cd}")


def _resolve_list_table_targets(tn_data_bsc_info: TnDataBscInfo) -> tuple[str, str]:
    tmp_table = _normalize_target_table(getattr(tn_data_bsc_info, "tmpr_tbl_phys_nm", None)) or HEYDEALER_TMP_LIST_TABLE
    source_table = _normalize_target_table(getattr(tn_data_bsc_info, "ods_tbl_phys_nm", None)) or HEYDEALER_SOURCE_LIST_TABLE
    return tmp_table, source_table


def _dedupe_heydealer_list_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    if "." in full_name:
        s, t = full_name.split(".", 1)
        return s.strip(), t.strip()
    return "public", full_name.strip()


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

    # rows의 키를 기준으로 insert 컬럼 결정 (테이블에 존재하는 컬럼만)
    candidate_cols: list[str] = []
    for r in rows:
        for k in r.keys():
            if k not in candidate_cols:
                candidate_cols.append(k)
    for k in extra_static_cols.keys():
        if k not in candidate_cols:
            candidate_cols.append(k)

    if allow_only_table_cols and table_cols:
        insert_cols = [c for c in candidate_cols if c in table_col_set]
    else:
        insert_cols = candidate_cols

    if not insert_cols:
        raise ValueError(f"insert 가능한 컬럼이 없습니다. table={full_table_name}")

    values = []
    for r in rows:
        merged = {**r, **extra_static_cols}
        values.append(tuple(merged.get(c) for c in insert_cols))

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            if truncate:
                cur.execute(f"TRUNCATE TABLE {full_table_name}")
            # execute_values 사용 (psycopg2)
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
        raise ValueError(f"update 키 컬럼이 없습니다. table={full_table_name}, key_col={key_col}")

    value_cols = [key_col] + [c for c in candidate_cols if c != key_col and (c in table_col_set if table_cols else True)]
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
        {str(row.get("create_dt") or "").strip() for row in rows if str(row.get("create_dt") or "").strip()}
    )
    date_values = sorted(
        {
            str(row.get("date_crtr_pnttm") or row.get("data_crtr_pnttm") or "").strip()
            for row in rows
            if str(row.get("date_crtr_pnttm") or row.get("data_crtr_pnttm") or "").strip()
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
    elif "data_crtr_pnttm" in table_cols and date_values:
        sql = f'DELETE FROM {full_table_name} WHERE "data_crtr_pnttm" = ANY(%s)'
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
        row_date = _normalize_compare_value(row.get("date_crtr_pnttm") or row.get("data_crtr_pnttm"))
        row_create_dt = _normalize_compare_value(row.get("create_dt"))
        if row_date and row_date > date_crtr_pnttm:
            date_crtr_pnttm = row_date
        if row_create_dt and row_create_dt > create_dt:
            create_dt = row_create_dt
    return date_crtr_pnttm, create_dt


def _rows_differ(current_row: dict[str, Any], latest_row: dict[str, Any], compare_cols: list[str]) -> bool:
    for col in compare_cols:
        if _normalize_compare_value(current_row.get(col)) != _normalize_compare_value(latest_row.get(col)):
            return True
    return False


def _fetch_latest_source_rows(
    hook: PostgresHook,
    source_table: str,
    key_cols: tuple[str, ...],
    order_cols: list[str],
) -> dict[tuple[str, ...], dict[str, Any]]:
    key_expr = ", ".join([f'"{col}"' for col in key_cols])
    order_expr = ", ".join([f'COALESCE("{col}", \'\') DESC' for col in order_cols] + ["ctid DESC"])
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

# --- 설정 및 경로 ---
# ----- 목록 수집 모드 (테스트 vs 전체 무한스크롤) -----
# [테스트] 몇 개만 수집: TARGET_COUNT = 숫자 (해당 개수 모이면 수집 종료)
# [전체]  무한스크롤 끝까지: TARGET_COUNT = None (새 매물 없을 때까지 스크롤)
# 사용법: 둘 중 하나만 유지하고 나머지는 주석 처리
# TARGET_COUNT = 5
TARGET_COUNT = None

BASE_DIR = Path(__file__).resolve().parent

# 경로/파일명은 USED_CAR_SITE_NAMES JSON + datst_cd 로 결정 → activate_paths_for_datst() 에서 설정
# (DAG import 시 한 번 기본 호출; 각 Task 에서도 datst_cd 마다 재호출)
YEAR_STR = ""
DATE_STR = ""
RUN_TS = ""
RESULT_DIR = Path("/tmp")
LOG_DIR = Path("/tmp")
IMG_BASE = Path("/tmp")
IMG_LIST_REL = ""
DETAIL_IMG_REL = ""
BRAND_LIST_FILE = Path("/tmp")
CAR_TYPE_LIST_FILE = Path("/tmp")
LIST_FILE = Path("/tmp")
LOG_FILE = Path("/tmp")
BRAND_HIERARCHY_LOG = Path("/tmp")
_logger_brand = logging.getLogger("heydealer_brand_placeholder")

try:
    activate_paths_for_datst("data6")
except Exception:
    pass

# --- 로그 설정 (경로는 위 activate 결과 사용) ---
# LOG_FILE, BRAND_HIERARCHY_LOG, _logger_brand 는 activate_paths_for_datst 에서 갱신됨

BRAND_CSV_FIELDS = [
    "model_sn",
    # "brand_id",
    "brand_list",  # brand_name
    # "model_group_id",
    "car_list",  # model_group_name
    # "model_id",
    "model_list",  # model_name
    "model_list_1",
    # "model_list_2_id",
    "model_list_2",
    "production_period", "data_crtr_pnttm", "create_dt"
]


def _brand_collect_log(level: int, msg: str) -> None:
    """
    브랜드 API 수집 로그를 (1) 파일 전용 _logger_brand (2) Airflow task 로그(UI) 에 동시 기록.
    """
    _logger_brand.log(level, msg)
    try:
        logging.getLogger("airflow.task").log(level, msg)
    except Exception:
        pass


def fetch_and_save_brand_csv():
    """crawl_heydealer_brand.py와 동일: API로 브랜드·모델 계층 수집 후 brand CSV 저장. 로그는 heydealer_brand_hierarchy.log 사용."""
    if BRAND_LIST_FILE.exists():
        BRAND_LIST_FILE.unlink()
    API_BASE = "https://api.heydealer.com/v2/customers/web/market/car_meta"
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    d_pnttm = datetime.now().strftime("%Y%m%d")
    c_dt = datetime.now().strftime("%Y%m%d%H%M")
    n_written = 0
    try:
        _brand_collect_log(logging.INFO, "[0단계] 브랜드 API 수집 → heydealer_brand_list.csv")
        _brand_collect_log(logging.INFO, f"   출력 파일: {BRAND_LIST_FILE}")
        _brand_collect_log(logging.INFO, "=" * 60)
        _brand_collect_log(logging.INFO, "헤이딜러 브랜드-모델 계층 데이터 수집 시작 (날짜 정보 포함)")
        _brand_collect_log(logging.INFO, "=" * 60)
        brands_resp = session.get(f"{API_BASE}/brands/", timeout=15)
        brands_resp.raise_for_status()
        raw = brands_resp.json()
        brands = raw if isinstance(raw, list) else (raw.get("brands") or raw.get("data") or []) if isinstance(raw, dict) else []
        n_brands = len(brands)
        _brand_collect_log(logging.INFO, f"총 {n_brands}개 브랜드 데이터 수집 시작")
        for b_idx, brand in enumerate(brands, 1):
            brand_id = brand.get("hash_id")
            brand_name = brand.get("name")
            _brand_collect_log(logging.INFO, f"[{b_idx}/{n_brands}] 브랜드 처리 중: {brand_name}")
            mg_resp = session.get(f"{API_BASE}/brands/{brand_id}/", timeout=15)
            if mg_resp.status_code != 200:
                continue
            for mg in mg_resp.json().get("model_groups", []):
                mg_id = mg.get("hash_id")
                mg_name = mg.get("name")
                sub_resp = session.get(f"{API_BASE}/model_groups/{mg_id}/", timeout=15)
                if sub_resp.status_code != 200:
                    continue
                for model in sub_resp.json().get("models", []):
                    model_id = model.get("hash_id", "")
                    model_name = model.get("name", "")
                    period = model.get("period", "")
                    # models/{model_id}/ API로 grades·details 수집 (model_list_1, model_list_2_id, model_list_2)
                    model_resp = session.get(f"{API_BASE}/models/{model_id}/", timeout=15)
                    if model_resp.status_code != 200:
                        row = {
                            "model_sn": n_written + 1,
                            "brand_list": brand_name,
                            "car_list": mg_name,
                            "model_list": model_name,
                            "model_list_1": "",
                            "model_list_2": "",
                            "production_period": period,
                            "data_crtr_pnttm": d_pnttm,
                            "create_dt": c_dt,
                        }
                        save_to_csv_append(BRAND_LIST_FILE, BRAND_CSV_FIELDS, row)
                        n_written += 1
                        continue
                    grades = model_resp.json().get("grades") or []
                    if not grades:
                        row = {
                            "model_sn": n_written + 1,
                            "brand_list": brand_name,
                            "car_list": mg_name,
                            "model_list": model_name,
                            "model_list_1": "",
                            "model_list_2": "",
                            "production_period": period,
                            "data_crtr_pnttm": d_pnttm,
                            "create_dt": c_dt,
                        }
                        save_to_csv_append(BRAND_LIST_FILE, BRAND_CSV_FIELDS, row)
                        n_written += 1
                    else:
                        for grade in grades:
                            grade_name = grade.get("name", "")
                            details = grade.get("details") or []
                            if not details:
                                row = {
                                    "model_sn": n_written + 1,
                                    "brand_list": brand_name,
                                    "car_list": mg_name,
                                    "model_list": model_name,
                                    "model_list_1": grade_name,
                                    "model_list_2": "",
                                    "production_period": period,
                                    "data_crtr_pnttm": d_pnttm,
                                    "create_dt": c_dt,
                                }
                                save_to_csv_append(BRAND_LIST_FILE, BRAND_CSV_FIELDS, row)
                                n_written += 1
                            else:
                                for detail in details:
                                    row = {
                                        "model_sn": n_written + 1,
                                        "brand_list": brand_name,
                                        "car_list": mg_name,
                                        "model_list": model_name,
                                        "model_list_1": grade_name,
                                        "model_list_2": detail.get("name", ""),
                                        "production_period": period,
                                        "data_crtr_pnttm": d_pnttm,
                                        "create_dt": c_dt,
                                    }
                                    save_to_csv_append(BRAND_LIST_FILE, BRAND_CSV_FIELDS, row)
                                    n_written += 1
        if n_written:
            _brand_collect_log(logging.INFO, "=" * 60)
            _brand_collect_log(logging.INFO, f"✅ 수집 완료! 파일: {BRAND_LIST_FILE}")
            _brand_collect_log(logging.INFO, f"총 수집 모델 수: {n_written:,}개")
            _brand_collect_log(logging.INFO, "=" * 60)
        else:
            _brand_collect_log(logging.WARNING, "⚠️ 수집된 데이터가 없습니다.")
    except Exception as e:
        _brand_collect_log(logging.ERROR, f"❌ 크롤링 중 치명적 오류: {e}")
        import traceback
        traceback.print_exc()

def load_brand_mapping():
    """model_name(정확) -> {brand_id, brand_name}, brand_name(브랜드명) -> {brand_id, brand_name} 둘 다 반환."""
    brand_map = {}
    brand_by_name = {}
    if BRAND_LIST_FILE.exists():
        with open(BRAND_LIST_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                info = {"brand_id": "", "brand_name": (row.get("brand_list") or "").strip()}
                model_name = (row.get("model_list") or "").strip()
                if model_name:
                    brand_map[model_name] = info
                bn = info["brand_name"]
                if bn and bn not in brand_by_name:
                    brand_by_name[bn] = info
    else:
        print(f"⚠️ 매핑 파일이 없습니다: {BRAND_LIST_FILE}")
    return brand_map, brand_by_name


def _row_key(row, trim_model_list=False, drop_first_word=False):
    """list/brand 행에서 model_list+model_list_1+model_list_2 결합.
    trim_model_list=True: model_list는 띄어쓰기 앞단(첫 단어)만.
    drop_first_word=True: model_list에서 앞에서부터 띄어쓰기 한 부분(첫 단어)을 지우고 나머지 사용."""
    m = (row.get("model_list") or "").strip()
    if trim_model_list and m:
        m = m.split()[0]
    elif drop_first_word and m:
        m = m.split(" ", 1)[1].strip() if " " in m else ""
    m1 = (row.get("model_list_1") or "").strip()
    m2 = (row.get("model_list_2") or "").strip()
    return m + m1 + m2


def _key_match(a, b):
    """포함 관계 매칭: in 2가지(a in b, b in a) + str.find 2가지(a.find(b), b.find(a)) 모두 사용."""
    if not a or not b:
        return False
    if a in b or b in a:           # in 2가지
        return True
    if a.find(b) >= 0 or b.find(a) >= 0:  # find 2가지
        return True
    return False


def _find_matching_brand_row(list_row, brand_matcher):
    """list 행과 brand 행 매칭:
    1) 동일: list(model_list+model_list_1+model_list_2) == brand(동일)
    2) list model_list 앞단만(첫 단어) + model_list_1+2 로 in/find 비교
    3) list model_list에서 앞 한 단어 지우고 나머지 + model_list_1+2 로 in/find 비교 (폭스바겐 더 뉴 파사트 → 더 뉴 파사트+... 와 brand 매칭)"""
    if not brand_matcher:
        return None
    list_key = _row_key(list_row)
    list_key_trim = _row_key(list_row, trim_model_list=True)
    list_key_drop = _row_key(list_row, drop_first_word=True)
    exact_map = brand_matcher.get("exact", {})
    key_rows = brand_matcher.get("key_rows", [])
    if list_key and list_key in exact_map:
        return exact_map[list_key]
    for brand_key, br in key_rows:
        if _key_match(list_key_trim, brand_key):
            return br
    for brand_key, br in key_rows:
        if _key_match(list_key_drop, brand_key):
            return br
    return None


def load_brand_rows():
    """brand_list.csv 전체 행 로드 (brand_list ~ model_list_2 매칭용)."""
    rows = []
    if not BRAND_LIST_FILE.exists():
        return rows
    with open(BRAND_LIST_FILE, "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def build_brand_row_matcher(brand_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    brand 행 매칭용 사전/캐시 생성.
    기존에는 list 행마다 brand_rows 전체를 돌며 key를 다시 계산해서
    SUV/RV처럼 건수가 많은 구간에서 급격히 느려졌다.
    """
    exact_map: dict[str, dict[str, Any]] = {}
    key_rows: list[tuple[str, dict[str, Any]]] = []
    for row in brand_rows:
        key = _row_key(row)
        if not key:
            continue
        if key not in exact_map:
            exact_map[key] = row
        key_rows.append((key, row))
    return {
        "exact": exact_map,
        "key_rows": key_rows,
    }


def merge_brand_into_list(raw_list, list_fields):
    """list.csv 행에 대해 brand.csv와 model_list+model_list_1+model_list_2로 매칭 후, 일치하면 brand_list~model_list_2 채움."""
    brand_rows = load_brand_rows()
    if not brand_rows:
        return
    brand_matcher = build_brand_row_matcher(brand_rows)
    updated = 0
    for item in raw_list:
        br = _find_matching_brand_row(item, brand_matcher)
        if br is None:
            continue
        item["brand_list"] = (br.get("brand_list") or "").strip()
        item["car_list"] = (br.get("car_list") or "").strip()
        item["model_list"] = (br.get("model_list") or "").strip()
        item["model_list_1"] = (br.get("model_list_1") or "").strip()
        item["model_list_2"] = (br.get("model_list_2") or "").strip()
        updated += 1
    if updated:
        rewrite_csv_atomic(LIST_FILE, list_fields, raw_list)
        print(f"   [매칭] brand.csv와 일치하여 list.csv에 brand_list~model_list_2 반영: {updated}건")


def get_now_times():
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%Y%m%d%H%M")

FILTERS_API = "https://api.heydealer.com/v2/customers/web/market/filters/"
CAR_TYPE_CSV_FIELDS = ["car_type_sn", "car_type_name", "date_crtr_pnttm", "create_dt"]


def fetch_filters_car_type_entries():
    """filters API에서 차종 (value, name) 목록만 가져옴. CSV 저장 없음. list 수집 시 차종 선택용."""
    entries = []
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        resp = session.get(FILTERS_API, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        for item in (data.get("car_shape") or []):
            name = (item.get("name") or "").strip()
            value = (item.get("value") or "").strip()
            if value:
                entries.append((value, name))
        if entries:
            print(f"   [차종] 목록 수집용 차종 {len(entries)}개 로드 (경∙소형, 세단, SUV∙RV 등)")
    except Exception as e:
        print(f"   ⚠️ filters API 실패: {e}")
    return entries


def fetch_filters_and_save_car_type_list():
    """filters API의 car_shape로 차종 목록을 가져와 heydealer_car_type_list.csv 저장. (value, name) 리스트 반환."""
    if CAR_TYPE_LIST_FILE.exists():
        CAR_TYPE_LIST_FILE.unlink()
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    d_pnttm, c_dt = get_now_times()
    entries = []
    try:
        resp = session.get(FILTERS_API, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        car_shape = data.get("car_shape") or []
        for sn, item in enumerate(car_shape, 1):
            name = (item.get("name") or "").strip()
            value = (item.get("value") or "").strip()
            if not value:
                continue
            entries.append((value, name))
            save_to_csv_append(CAR_TYPE_LIST_FILE, CAR_TYPE_CSV_FIELDS, {
                "car_type_sn": sn,
                "car_type_name": name,
                "date_crtr_pnttm": d_pnttm,
                "create_dt": c_dt,
            })
        if entries:
            print(f" 📄 차종 목록 API 수집: {CAR_TYPE_LIST_FILE} ({len(entries)}개)")
        else:
            print("   ⚠️ filters API에서 car_shape 없음")
    except Exception as e:
        print(f"   ⚠️ filters API 수집 실패: {e}")
    return entries


def _normalize_filter_text(text: str) -> str:
    s = (text or "").strip()
    s = s.replace("·", "∙")
    s = re.sub(r"\s*∙\s*", " ∙ ", s)
    return " ".join(s.split()).strip()


def _extract_apply_count(button_text: str) -> int:
    digits = re.sub(r"[^0-9]", "", button_text or "")
    return int(digits) if digits else 0


def _read_apply_button_text(locator) -> str:
    try:
        text = (locator.inner_text() or "").strip()
        if text:
            return text
    except Exception:
        pass
    try:
        return locator.evaluate(
            """el => {
                const text = (el.textContent || '').trim();
                if (text) return text;
                const nf = el.querySelector('number-flow-react');
                if (!nf) return '';
                const raw = nf.getAttribute('data') || '';
                try {
                    const parsed = JSON.parse(raw);
                    return parsed.valueAsString || '';
                } catch (e) {
                    return raw;
                }
            }"""
        ) or ""
    except Exception:
        return ""


def _read_button_label(locator) -> str:
    try:
        return locator.evaluate(
            """el => {
                const span = el.querySelector('span[data-content]');
                return (
                    (span && (span.getAttribute('data-content') || span.textContent)) ||
                    el.textContent ||
                    ''
                ).trim();
            }"""
        ) or ""
    except Exception:
        try:
            return (locator.inner_text() or "").strip()
        except Exception:
            return ""


def _find_button_by_labels(scope, labels: list[str], timeout_ms: int = 10000):
    normalized_labels = {_normalize_filter_text(label) for label in labels if _normalize_filter_text(label)}
    deadline = time.time() + (timeout_ms / 1000)
    last_error = None

    while time.time() < deadline:
        buttons = scope.locator("button")
        try:
            count = buttons.count()
        except Exception as e:
            last_error = e
            time.sleep(0.2)
            continue

        for idx in range(count):
            btn = buttons.nth(idx)
            try:
                if not btn.is_visible():
                    continue
                label = _normalize_filter_text(_read_button_label(btn))
                if label in normalized_labels:
                    return btn
            except Exception as e:
                last_error = e
                continue
        time.sleep(0.2)

    if last_error:
        raise last_error
    raise ValueError(f"버튼을 찾지 못했습니다: {labels}")


def _find_apply_button(layer):
    deadline = time.time() + 10
    last_error = None

    while time.time() < deadline:
        buttons = layer.locator("button")
        try:
            count = buttons.count()
        except Exception as e:
            last_error = e
            time.sleep(0.2)
            continue

        for idx in range(count):
            btn = buttons.nth(idx)
            try:
                if not btn.is_visible():
                    continue
                btn_text = _read_apply_button_text(btn)
                if "대 보기" in btn_text:
                    return btn
            except Exception as e:
                last_error = e
                continue
        time.sleep(0.2)

    if last_error:
        raise last_error
    raise ValueError("N대 보기 버튼을 찾지 못했습니다.")


def _click_locator_via_dom(locator) -> None:
    locator.wait_for(state="visible", timeout=10000)
    try:
        locator.scroll_into_view_if_needed()
        locator.evaluate("el => el.click()")
    except Exception:
        locator.click(force=True)


def _open_car_type_layer(page, current_trigger_name: str | None = None) -> None:
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(500)
    candidate_names: list[str] = []
    normalized_current = _normalize_filter_text(current_trigger_name or "")
    if normalized_current:
        candidate_names.append(normalized_current)
    candidate_names.append("차체")

    last_error = None
    opened = False
    for name in candidate_names:
        try:
            trigger = _find_button_by_labels(page, [name], timeout_ms=3000)
            _click_locator_via_dom(trigger)
            page.locator('div[tabindex="-1"][data-floating-ui-focusable=""]').first.wait_for(state="visible", timeout=5000)
            opened = True
            break
        except Exception as e:
            last_error = e
            continue

    if not opened:
        if last_error:
            raise last_error
        raise ValueError("차체 필터 트리거 버튼을 찾지 못했습니다.")

    page.locator('div[tabindex="-1"][data-floating-ui-focusable=""]').first.wait_for(state="visible", timeout=10000)


def _apply_car_type_filter(page, car_type_name: str, current_trigger_name: str | None = None) -> int:
    display_name = _normalize_filter_text(car_type_name)
    _open_car_type_layer(page, current_trigger_name=current_trigger_name)

    layer = page.locator('div[tabindex="-1"][data-floating-ui-focusable=""]').first
    layer.wait_for(state="visible", timeout=10000)

    reset_btn = layer.locator('button:has-text("초기화")').first
    if reset_btn.count():
        _click_locator_via_dom(reset_btn)
        page.wait_for_timeout(300)

    option_btn = _find_button_by_labels(layer, [display_name], timeout_ms=5000)
    _click_locator_via_dom(option_btn)
    page.wait_for_timeout(300)

    apply_btn = _find_apply_button(layer)
    apply_text = _read_apply_button_text(apply_btn)
    expected_count = _extract_apply_count(apply_text)
    _click_locator_via_dom(apply_btn)
    page.wait_for_timeout(1500)
    try:
        layer.wait_for(state="hidden", timeout=5000)
    except Exception:
        pass
    return expected_count

def _csv_cell_excel_text(val):
    """Excel이 숫자-숫자(예: 9-3, 9-5)를 날짜로 바꾸지 않도록 앞에 ' 붙여 텍스트로 저장."""
    if val is None:
        return ""
    s = (val if isinstance(val, str) else str(val)).strip()
    if not s:
        return s
    # 숫자-숫자 형태만 (사브 9-3, 9-5 등) → Excel에서 9월3일 등으로 안 바뀌게
    if re.match(r"^\d+-\d+$", s):
        return "'" + s
    return s


def save_to_csv_append(file_path, fieldnames, data_dict):
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = file_path.exists()
    # 셀 값 중 Excel 날짜로 해석될 수 있는 형태 보정
    row = {k: _csv_cell_excel_text(v) if isinstance(v, str) or v is None else v for k, v in data_dict.items()}
    with open(file_path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def rewrite_csv_atomic(file_path, fieldnames, rows):
    """CSV를 현재 rows 기준으로 원자적으로 재작성 (car_imgs 등 진행 반영)."""
    file_path = Path(file_path)
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            row = {k: _csv_cell_excel_text(v) if isinstance(v, str) or v is None else v for k, v in r.items()}
            writer.writerow(row)
    tmp_path.replace(file_path)


def get_today_img_rel_dir():
    r"""오늘 날짜 기준 상대 디렉터리 (예: data/img/2026년/리본카/20260312/list)."""
    return IMG_LIST_REL


def get_today_detail_img_rel_dir():
    """상세 이미지 상대 디렉터리: data/img/2026년/리본카/20260312/detail"""
    return DETAIL_IMG_REL


def download_list_image(img_url, product_id):
    """상품 대표 이미지 1장만 다운로드 → product_id_list.png. 성공 시 상대 경로 반환, 실패 시 ""."""
    try:
        if not img_url or "svg" in img_url.lower():
            return ""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://www.heydealer.com",
        }
        response = requests.get(img_url, stream=True, timeout=15, headers=headers)
        if response.status_code != 200:
            return ""
        # 예: /home/limhayoung/data/img/2026년/리본카/20260317/list
        save_dir = IMG_BASE / "list"
        save_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{product_id}_list.png"
        save_path = save_dir / filename
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        return f"{get_today_img_rel_dir()}/{filename}"
    except Exception:
        return ""


def download_detail_image(img_url, product_id, idx):
    """상세 페이지 이미지 1장 다운로드 → detail/연도/날짜/{product_id}_1.png, _2.png ... (예: Wnqe5KnL_1.png). 성공 시 상대 경로 반환."""
    try:
        if not img_url or "svg" in img_url.lower():
            return ""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://www.heydealer.com",
        }
        response = requests.get(img_url, stream=True, timeout=15, headers=headers)
        if response.status_code != 200:
            return ""
        # 예: /home/limhayoung/data/img/2026년/리본카/20260317/detail
        save_dir = IMG_BASE / "detail"
        save_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{product_id}_{idx}.png"
        save_path = save_dir / filename
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        return f"{get_today_detail_img_rel_dir()}/{filename}"
    except Exception:
        return ""


def download_image(img_url, product_id, idx):
    """이미지 다운로드. 저장 경로: imgs/heydealer/연도/YYYYMMDD/product_id_idx.ext"""
    try:
        if not img_url or "svg" in img_url.lower():
            return False
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://www.heydealer.com",
        }
        response = requests.get(img_url, stream=True, timeout=15, headers=headers)
        if response.status_code != 200:
            return False
        ext = img_url.split(".")[-1].split("?")[0].lower()
        if len(ext) > 4 or len(ext) < 2:
            ext = "jpg"
        save_dir = IMG_BASE
        save_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{product_id}_{idx}.{ext}"
        save_path = save_dir / filename
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        return True
    except Exception:
        return False

# 상세 페이지 리스트 이미지 DOM: #root > .css-kuuk2w > ... > .css-1fg02ng > .css-vdxqtk > img
LIST_IMG_DOM_SELECTOR = (
    "#root .css-kuuk2w .css-18e6263 .css-17qdlp1 .css-fhycda .css-1x0imnr "
    ".css-1t74t4t .css-a97e7u .css-a97e7u .css-8n2v9x .css-di7boj .css-1fg02ng .css-vdxqtk img"
)


def _collect_image_urls_from_detail_page(page):
    """상세 페이지에서 차량 이미지 URL 목록을 수집 (중복 제거, 순서 유지). list 폴더용이 아닌 detail 폴더 저장용."""
    seen = set()
    urls = []
    try:
        page.wait_for_timeout(1200)
        imgs = page.query_selector_all(LIST_IMG_DOM_SELECTOR)
        for img in imgs:
            src = (img.get_attribute("src") or img.get_attribute("data-src") or "").strip()
            if src and "svg" not in src.lower() and src not in seen:
                seen.add(src)
                urls.append(src)
        try:
            page.wait_for_selector(".css-12qft46", timeout=20000)
        except Exception:
            try:
                page.wait_for_selector(".css-113wzqa", timeout=10000)
            except Exception:
                pass
        page.wait_for_timeout(1200)
        for i in range(1, 14):
            page.evaluate(f"window.scrollTo(0, {i * 500})")
            time.sleep(0.15)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(800)
        detail_container = page.query_selector(".css-1uus6sd .css-12qft46")
        if not detail_container:
            detail_container = page.query_selector(".css-12qft46")
        if detail_container:
            ltrevz_sections = detail_container.query_selector_all(".css-ltrevz")
            if len(ltrevz_sections) >= 2:
                sec2 = ltrevz_sections[1]
                for sel in [".css-5pr39e .css-1i3qy3r .css-1dpi6xl button.css-q47uzu img.css-q38rgl", "button.css-q47uzu img.css-q38rgl", "button img, .css-q47uzu img"]:
                    for img in sec2.query_selector_all(sel):
                        src = (img.get_attribute("src") or img.get_attribute("data-src") or "").strip()
                        if src and "svg" not in src.lower() and src not in seen:
                            seen.add(src)
                            urls.append(src)
            if len(ltrevz_sections) >= 4:
                sec4 = ltrevz_sections[3]
                for sel in [".css-5pr39e .css-1i3qy3r .css-hf19cn .css-1a3591h img.css-158t7i4", ".css-5pr39e .css-1i3qy3r .css-w9nhgi img.css-158t7i4", ".css-hf19cn .css-1a3591h img", ".css-hf19cn .css-w9nhgi img", ".css-w9nhgi img.css-158t7i4"]:
                    for img in sec4.query_selector_all(sel):
                        src = (img.get_attribute("src") or img.get_attribute("data-src") or "").strip()
                        if src and "svg" not in src.lower() and src not in seen:
                            seen.add(src)
                            urls.append(src)
        for img in page.query_selector_all("img[src*='heydealer.com'], img[src*='cdn.'], .css-w9nhgi img, .css-1a3591h img, main img"):
            src = (img.get_attribute("src") or img.get_attribute("data-src") or "").strip()
            if not src or "svg" in src.lower() or src in seen:
                continue
            seen.add(src)
            urls.append(src)
        page.wait_for_timeout(800)
        for i in range(1, 12):
            page.evaluate(f"window.scrollTo(0, {i * 600})")
            time.sleep(0.2)
        for img in page.query_selector_all("img[src], img[data-src]"):
            src = (img.get_attribute("src") or img.get_attribute("data-src") or "").strip()
            if not src or "svg" in src.lower() or src in seen:
                continue
            if "heydealer" in src or "cdn." in src or len(src) > 20:
                seen.add(src)
                urls.append(src)
    except Exception as e:
        print(f"      ❌ 이미지 수집 오류: {str(e)[:60]}")
    return urls


def _query_list_car_cards_snapshot(page) -> list[dict[str, str]]:
    """
    목록 카드의 필요한 값만 브라우저에서 한 번에 추출한다.
    대량 목록에서 ElementHandle 순회 비용을 줄여 수집 속도를 개선한다.
    """
    try:
        rows = page.evaluate(
            """
            () => {
              const isVisible = (el) => {
                if (!el) return false;
                const rect = el.getBoundingClientRect();
                const style = window.getComputedStyle(el);
                return rect.width > 0
                  && rect.height > 0
                  && style.visibility !== 'hidden'
                  && style.display !== 'none';
              };

              const anchors = Array.from(document.querySelectorAll('a[href^="/market/cars/"]'));
              return anchors
                .filter((el) => isVisible(el) && isVisible(el.querySelector('.css-9j6363')))
                .map((el) => {
                  const href = ((el.getAttribute('href') || '').split('?')[0] || '').trim();
                  const modelBox = el.querySelector('.css-9j6363');
                  const names = modelBox
                    ? Array.from(modelBox.querySelectorAll('.css-jk6asd')).map((node) => (node.textContent || '').trim())
                    : [];
                  const gradeEl = modelBox ? modelBox.querySelector('.css-13wylk3') : null;
                  const ykEl = el.querySelector('.css-6bza35');
                  const priceArea = el.querySelector('.css-105xtr1 .css-1066lcq .css-dbu2tk');
                  const saleEl = priceArea ? priceArea.querySelector('.css-8sjynn') : null;

                  let listImageUrl = '';
                  for (const img of Array.from(el.querySelectorAll('img'))) {
                    const src = ((img.getAttribute('src') || img.getAttribute('data-src') || '') + '').trim();
                    if (!src || src.toLowerCase().includes('svg')) continue;
                    if (!listImageUrl) listImageUrl = src;
                    if (src.includes('image.heydealer.com') || src.includes('heydealer.com')) {
                      listImageUrl = src;
                      break;
                    }
                  }

                  return {
                    href,
                    model_name: names[0] || '',
                    model_second_name: names[1] || '',
                    grade_name: gradeEl ? (gradeEl.textContent || '').trim() : '',
                    year_km: ykEl ? (ykEl.textContent || '').trim() : '',
                    sale_price: saleEl
                      ? (saleEl.textContent || '').trim()
                      : (priceArea ? (priceArea.textContent || '').trim() : ''),
                    list_image_url: listImageUrl,
                  };
                })
                .filter((row) => row.href && row.model_name);
            }
            """
        )
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def _normalize_list_href(href: str) -> str:
    h = (href or "").strip().split("?")[0].rstrip("/")
    return h


def _collect_images_from_detail_page(page, product_id):
    """상세 페이지에서 이미지 URL 수집 후 detail 폴더에 product_id_1.png, product_id_2.png ... 로 저장. car_imgs에는 첫 번째 상대 경로 반환."""
    car_imgs_path = ""
    urls = _collect_image_urls_from_detail_page(page)
    for idx, src in enumerate(urls, 1):
        path = download_detail_image(src, product_id, idx)
        if path and not car_imgs_path:
            car_imgs_path = path
    return car_imgs_path


def _build_card_data_from_snapshot(
    card: dict[str, Any],
    idx: int,
    brand_map: dict[str, Any],
    *,
    car_type: str = "",
    brand_by_name: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = {
        "model_sn": idx,
        "brand_id": "",
        "brand_name": "",
        "car_type": car_type,
        "car_list": "",
        "car_imgs": "",
        "list_image_url": (card.get("list_image_url") or "").strip(),
        "car_name": "",
    }
    try:
        href = card.get("href") or ""
        full_url = (href if str(href).startswith("http") else f"https://www.heydealer.com{href}").split("?")[0]
        data["product_id"] = full_url.split("/")[-1]
        data["detail_url"] = full_url

        raw_model_name = (card.get("model_name") or "").strip()
        data["model_name"] = raw_model_name
        data["model_list"] = raw_model_name

        second_name = (card.get("model_second_name") or "").strip()
        data["model_second_name"] = second_name
        if second_name:
            data["model_list_1"] = second_name

        matched = brand_map.get(raw_model_name)
        if not matched and " " in raw_model_name:
            matched = brand_map.get(raw_model_name.split(" ", 1)[1].strip())
        if not matched and brand_by_name:
            for word in raw_model_name.replace("·", " ").split():
                word = word.strip()
                if word and brand_by_name.get(word):
                    matched = brand_by_name[word]
                    break
        if matched:
            data["brand_id"], data["brand_name"] = matched["brand_id"], matched["brand_name"]
            data["brand_list"] = matched["brand_name"]

        grade_name = (card.get("grade_name") or "").strip()
        data["grade_name"] = grade_name
        if grade_name:
            data["model_list_2"] = grade_name

        if grade_name and raw_model_name.endswith(" " + grade_name):
            data["car_name"] = raw_model_name
        elif grade_name and raw_model_name.endswith(grade_name):
            base = raw_model_name[: -len(grade_name)].rstrip()
            data["car_name"] = f"{base} {grade_name}" if base else grade_name
        else:
            data["car_name"] = " ".join(p for p in [raw_model_name, grade_name] if p).strip()

        year_km = (card.get("year_km") or "").strip()
        if "ㆍ" in year_km:
            left, right = year_km.split("ㆍ", 1)
            data["year"], data["km"] = left.strip(), right.strip()
        else:
            data["year"], data["km"] = year_km, ""

        data["sale_price"] = (card.get("sale_price") or "").strip()
        d_pnttm, c_dt = get_now_times()
        data["date_crtr_pnttm"], data["create_dt"] = d_pnttm, c_dt
    except Exception:
        pass
    return data

def _extract_card_heydealer(elem, idx, brand_map, car_type="", brand_by_name=None) -> dict:
    data = {"model_sn": idx, "brand_id": "", "brand_name": "", "car_type": car_type, "car_list": "", "car_imgs": "", "list_image_url": "", "car_name": ""}
    try:
        href = elem.get_attribute("href") or ""
        full_url = (href if href.startswith("http") else f"https://www.heydealer.com{href}").split("?")[0]
        data["product_id"] = full_url.split("/")[-1]
        data["detail_url"] = full_url
        # 목록 카드 썸네일 이미지 URL (image.heydealer.com 등) → 이 URL로 저장해야 상세페이지 이미지가 아닌 리스트 이미지가 저장됨
        for img in elem.query_selector_all("img"):
            src = (img.get_attribute("src") or img.get_attribute("data-src") or "").strip()
            if not src or "svg" in src.lower():
                continue
            if "image.heydealer.com" in src or "heydealer.com" in src:
                data["list_image_url"] = src
                break
        if not data["list_image_url"] and elem.query_selector("img"):
            first_img = elem.query_selector("img")
            src = (first_img.get_attribute("src") or first_img.get_attribute("data-src") or "").strip()
            if src and "svg" not in src.lower():
                data["list_image_url"] = src
        m_box = elem.query_selector(".css-9j6363")
        if m_box:
            names = m_box.query_selector_all(".css-jk6asd")
            raw_model_name = names[0].inner_text().strip() if len(names) > 0 else ""
            data["model_name"] = raw_model_name
            data["model_list"] = raw_model_name
            data["model_second_name"] = names[1].inner_text().strip() if len(names) > 1 else ""
            if len(names) > 1:
                data["model_list_1"] = data["model_second_name"]
            matched = brand_map.get(raw_model_name)
            if not matched and " " in raw_model_name:
                sub_name = raw_model_name.split(" ", 1)[1].strip()
                matched = brand_map.get(sub_name)
            if not matched and brand_by_name:
                for word in raw_model_name.replace("·", " ").split():
                    w = word.strip()
                    if w and brand_by_name.get(w):
                        matched = brand_by_name[w]
                        break
            if matched:
                data["brand_id"], data["brand_name"] = matched["brand_id"], matched["brand_name"]
                data["brand_list"] = matched["brand_name"]
            grade = m_box.query_selector(".css-13wylk3")
            data["grade_name"] = grade.inner_text().strip() if grade else ""
            if data["grade_name"]:
                data["model_list_2"] = data["grade_name"]
            # 차량 풀네임: 모델명 + 띄어쓰기 + 등급 (예: "더 뉴 레이 시그니처"). 한 span에 "더 뉴 레이시그니처"처럼 붙어 나오면 등급 앞에 공백 삽입
            grade_name = data.get("grade_name", "")
            if grade_name and raw_model_name.endswith(" " + grade_name):
                data["car_name"] = raw_model_name  # 이미 "더 뉴 레이 시그니처" 형태
            elif grade_name and raw_model_name.endswith(grade_name):
                base = raw_model_name[: -len(grade_name)].rstrip()
                data["car_name"] = f"{base} {grade_name}" if base else grade_name
            else:
                data["car_name"] = " ".join(p for p in [raw_model_name, grade_name] if p).strip()
        yk_el = elem.query_selector(".css-6bza35")
        if yk_el:
            txt = yk_el.inner_text().strip()
            if "ㆍ" in txt:
                p = txt.split("ㆍ")
                data["year"], data["km"] = p[0].strip(), p[1].strip()
            else: data["year"], data["km"] = txt, ""
        price_area = elem.query_selector(".css-105xtr1 .css-1066lcq .css-dbu2tk")
        if price_area:
            sale = price_area.query_selector(".css-8sjynn")
            data["sale_price"] = sale.inner_text().strip() if sale else price_area.inner_text().strip()
        d_pnttm, c_dt = get_now_times()
        data["date_crtr_pnttm"], data["create_dt"] = d_pnttm, c_dt
    except: pass
    return data

def run_heydealer_job(
    bsc: TnDataBscInfo,
    *,
    brand_list_csv_path: str | None = None,
    car_type_csv_path: str | None = None,
    kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    DB 메타(`std.tn_data_bsc_info`) 1건을 기준으로 실행되는 본 작업.
    - bsc.link_data_clct_url: 기본 목록 URL
    - bsc.pvsn_site_cd / bsc.datst_cd: 실행 식별에 사용 가능
    - brand_list_csv_path: run_brand_csv 태스크에서 만든 브랜드 CSV 절대경로 (필수 권장)
    - car_type_csv_path: run_car_type_csv 결과 경로 (검증용, 선택)
    """
    global BRAND_LIST_FILE

    activate_paths_for_datst((bsc.datst_cd or HEYDEALER_DATST_LIST).lower() or HEYDEALER_DATST_LIST, kwargs=kwargs)

    # 브랜드 CSV는 앞 단계 Task에서 생성한 파일을 그대로 쓴다 (RUN_TS 불일치로 경로가 달라지는 문제 방지)
    if brand_list_csv_path:
        BRAND_LIST_FILE = Path(brand_list_csv_path)

    # 런타임에만 디렉터리/로그 준비 (DAG import-time 부작용 방지)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    IMG_BASE.mkdir(parents=True, exist_ok=True)
    (IMG_BASE / "list").mkdir(parents=True, exist_ok=True)
    _clear_directory_contents(IMG_BASE / "list")

    run_logger = _build_logger_for_file(LOG_FILE, "heydealer_run")
    today_img_dir_list = IMG_BASE / "list"
    run_logger.info("🏁 헤이딜러 수집 시작")
    run_logger.info(f"- pvsn_site_cd={bsc.pvsn_site_cd}, datst_cd={bsc.datst_cd}, datst_nm={bsc.datst_nm}")
    run_logger.info(f"- link_url={bsc.link_data_clct_url}")
    run_logger.info(f"- 이미지(list)={today_img_dir_list}")

    # 0) 브랜드 CSV는 run_brand_csv 태스크에서만 수집. 여기서는 API 재호출 없음.
    if not BRAND_LIST_FILE.exists():
        raise FileNotFoundError(
            f"브랜드 CSV가 없습니다. run_brand_csv를 먼저 성공시키고, "
            f"brand_list_csv_path를 넘겼는지 확인하세요: {BRAND_LIST_FILE}"
        )
    if car_type_csv_path and not Path(car_type_csv_path).exists():
        run_logger.warning(f"차종 CSV 경로 없음(무시하고 진행): {car_type_csv_path}")

    run_logger.info(f"[0단계] 브랜드 매핑 로드 (기존 CSV): {BRAND_LIST_FILE}")
    brand_map, brand_by_name = load_brand_mapping()
    list_fields = [
        "model_sn",
        "product_id",
        "car_type",
        "brand_list",
        "car_list",
        "model_list",
        "model_list_1",
        "model_list_2",
        "car_name",
        "year",
        "km",
        "sale_price",
        "detail_url",
        "car_imgs",
        "date_crtr_pnttm",
        "create_dt",
    ]

    if LIST_FILE.exists():
        LIST_FILE.unlink()

    # 차종 CSV 역시 별도 Task(run_car_type_csv)에서 수집.
    # 목록 수집에서는 filters API만 호출해서 (value, name)만 사용.
    run_logger.info("[0단계] 차종 목록 로딩 (filters API)")
    car_type_entries = fetch_filters_car_type_entries()
    if not car_type_entries:
        car_type_entries = [(0, "")]
        run_logger.warning("filters API 실패 → 필터 없이 전체만 수집합니다.")

    list_url = _build_heydealer_list_url(bsc.link_data_clct_url)

    run_logger.info("[1단계] 목록 수집 (playwright)")
    raw_list: list[dict[str, Any]] = []
    seen: set[str] = set()
    brand_rows = load_brand_rows()
    brand_matcher = build_brand_row_matcher(brand_rows)

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
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        for nav_try in range(3):
            try:
                page.goto(list_url, wait_until="commit", timeout=60000)
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                break
            except Exception as e:
                if nav_try < 2:
                    run_logger.warning(f"목록 페이지 재시도 ({nav_try + 2}/3): {e}")
                    time.sleep(3)
                else:
                    raise RuntimeError(f"목록 페이지 접속 실패: {list_url}") from e
        page.wait_for_timeout(2000)
        current_car_type_trigger_name = "차체"

        for car_type_value, car_type_name in car_type_entries:
            display_name = car_type_name or "전체"
            collected_this_type = 0
            no_new_rounds = 0
            prev_snapshot_count = 0
            expected_count = 0

            if car_type_value:
                try:
                    expected_count = _apply_car_type_filter(
                        page,
                        display_name,
                        current_trigger_name=current_car_type_trigger_name,
                    )
                    current_car_type_trigger_name = display_name
                    run_logger.info(
                        f"차종 적용: {display_name} (car-shape={car_type_value}, expected_count={expected_count})"
                    )
                except Exception as e:
                    run_logger.warning(f"[{display_name}] 차체 필터 적용 실패, 건너뜀: {e}")
                    continue
            else:
                run_logger.info("차종 없음(전체) → 수집 시작")

            while True:
                if TARGET_COUNT is not None and collected_this_type >= TARGET_COUNT:
                    run_logger.info(f"[{display_name}] 목표 {TARGET_COUNT}개 수집 완료")
                    break
                if expected_count and collected_this_type >= expected_count:
                    run_logger.info(f"[{display_name}] 보기 버튼 기준 {expected_count}개 수집 완료")
                    break

                prev_collected_this_type = collected_this_type
                last_height = page.evaluate("document.body.scrollHeight")
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1200)

                cards = _query_list_car_cards_snapshot(page)
                if car_type_value and not cards and collected_this_type == 0:
                    page.wait_for_timeout(1500)
                    cards = _query_list_car_cards_snapshot(page)
                current_snapshot_count = len(cards)
                new_card_count = 0
                for card in cards:
                    if TARGET_COUNT is not None and collected_this_type >= TARGET_COUNT:
                        break
                    href = _normalize_list_href(card.get("href") or "")
                    if href and href not in seen:
                        seen.add(href)
                        item = _build_card_data_from_snapshot(
                            card,
                            len(raw_list) + 1,
                            brand_map,
                            car_type=car_type_name,
                            brand_by_name=brand_by_name,
                        )
                        br = _find_matching_brand_row(item, brand_matcher)
                        if br:
                            item["brand_list"] = (br.get("brand_list") or "").strip()
                            item["car_list"] = (br.get("car_list") or "").strip()
                            item["model_list"] = (br.get("model_list") or "").strip()
                            item["model_list_1"] = (br.get("model_list_1") or "").strip()
                            item["model_list_2"] = (br.get("model_list_2") or "").strip()
                        raw_list.append(item)
                        save_to_csv_append(LIST_FILE, list_fields, item)
                        collected_this_type += 1
                        new_card_count += 1

                if collected_this_type == prev_collected_this_type and current_snapshot_count <= prev_snapshot_count:
                    no_new_rounds += 1
                else:
                    no_new_rounds = 0
                prev_snapshot_count = current_snapshot_count

                if TARGET_COUNT is not None:
                    run_logger.info(
                        f"목록 수집 [{display_name}]: {collected_this_type}/{TARGET_COUNT}대 "
                        f"(총 {len(raw_list)}대, snapshot={current_snapshot_count}, new={new_card_count})"
                    )
                else:
                    run_logger.info(
                        f"목록 수집 [{display_name}]: {collected_this_type}대 "
                        f"(총 {len(raw_list)}대, expected={expected_count}, snapshot={current_snapshot_count}, new={new_card_count})"
                    )

                if car_type_value and collected_this_type == 0 and prev_collected_this_type == 0:
                    run_logger.info(f"[{display_name}] 매물 0대 → 수집 종료")
                    break

                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    page.wait_for_timeout(1500)
                    if page.evaluate("document.body.scrollHeight") == last_height:
                        if expected_count and collected_this_type < expected_count and no_new_rounds < 3:
                            run_logger.info(
                                f"[{display_name}] 추가 로딩 대기 "
                                f"(collected={collected_this_type}, expected={expected_count}, snapshot={current_snapshot_count})"
                            )
                            continue
                        run_logger.info(f"[{display_name}] 페이지 끝 도달")
                        break
                if no_new_rounds >= 3 and new_card_count == 0:
                    run_logger.info(f"[{display_name}] 새 매물 없음 → 수집 종료")
                    break

        run_logger.info(f"[1단계] 목록 CSV 생성 완료: {LIST_FILE} ({len(raw_list)}건)")

        if raw_list:
            run_logger.info("[2단계] 목록 이미지 수집 시작 (%d건)", len(raw_list))
            for idx, item in enumerate(raw_list, 1):
                product_id = item.get("product_id", "")
                list_image_url = (item.get("list_image_url") or "").strip()

                if list_image_url:
                    car_imgs_path = download_list_image(list_image_url, product_id)
                    if car_imgs_path:
                        item["car_imgs"] = car_imgs_path

        rewrite_csv_atomic(LIST_FILE, list_fields, raw_list)
        browser.close()

    return {
        "brand_csv": str(BRAND_LIST_FILE),
        "car_type_csv": str(CAR_TYPE_LIST_FILE),
        "list_csv": str(LIST_FILE),
        "count": len(raw_list),
        "log_file": str(LOG_FILE),
    }


def _sync_tmp_and_source_register_flag(
    hook: PostgresHook,
    *,
    current_rows: list[dict[str, Any]],
    tmp_table: str,
    source_table: str,
    key_cols: tuple[str, ...] = ("product_id", "detail_url"),
    flag_col: str = "register_flag",
):
    tmp_count = CommonUtil.get_table_row_count(hook, tmp_table)
    if not current_rows:
        return {"tmp_count": tmp_count, "source_count": CommonUtil.get_table_row_count(hook, source_table)}

    src_cols = _get_table_columns(hook, source_table)
    src_col_set = set(src_cols)
    src_has_flag = flag_col in src_col_set
    missing_key_cols = [col for col in key_cols if col not in src_col_set]
    if missing_key_cols:
        raise ValueError(f"source 테이블 키 컬럼 누락: table={source_table}, cols={missing_key_cols}")

    current_rows = _dedupe_heydealer_list_rows(current_rows)
    date_crtr_pnttm, create_dt = _pick_snapshot_audit_values(current_rows)
    order_cols = [col for col in ("create_dt", "date_crtr_pnttm", "data_crtr_pnttm") if col in src_col_set]
    update_key_col = key_cols[0]
    latest_source_map = _fetch_latest_source_rows(hook, source_table, (update_key_col,), order_cols)
    compare_exclude_cols = set(key_cols) | {"model_sn", "date_crtr_pnttm", "data_crtr_pnttm", "create_dt", flag_col, "_row_ctid"}
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
            rows_to_mark_existing.append({
                update_key_col: current_row.get(update_key_col),
                flag_col: "Y",
            })

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
        _bulk_insert_rows(hook, source_table, rows_to_insert, truncate=False, allow_only_table_cols=True)

    if missing_product_ids and src_has_flag:
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                set_parts = [f'"{flag_col}" = %s']
                params: list[Any] = ["N"]
                if "date_crtr_pnttm" in src_col_set and date_crtr_pnttm:
                    set_parts.append('"date_crtr_pnttm" = %s')
                    params.append(date_crtr_pnttm)
                if "data_crtr_pnttm" in src_col_set and date_crtr_pnttm:
                    set_parts.append('"data_crtr_pnttm" = %s')
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


def _run_heydealer_brand_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    activate_paths_for_datst((datst_cd or HEYDEALER_DATST_BRAND).lower() or HEYDEALER_DATST_BRAND, kwargs=kwargs)
    fetch_and_save_brand_csv()
    return str(BRAND_LIST_FILE)


def _run_heydealer_car_type_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    activate_paths_for_datst((datst_cd or HEYDEALER_DATST_CAR_TYPE).lower() or HEYDEALER_DATST_CAR_TYPE, kwargs=kwargs)
    fetch_filters_and_save_car_type_list()
    return str(CAR_TYPE_LIST_FILE)


dag_object = heydealer_crawler()