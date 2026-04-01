# -*- coding: utf-8 -*-
"""
K Car 검색 페이지에서 차종/브랜드/목록 수집.

Airflow DAG:
- DB 메타(std.tn_data_bsc_info, ps00004, data10/11/12) 조회
- 차종 CSV -> 브랜드 CSV -> 목록 CSV 생성
- 수집 메타(std.tn_data_clct_dtl_info) 등록 후 ODS 적재
- list는 임시 테이블 적재 후 원천 테이블과 동기화
"""
import csv
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

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
from util.playwright_util import images_enabled, install_route_blocking


@dag(
    dag_id="sdag_kcar_crawler",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["used_car", "kcar", "crawler", "day"],
)
def kcar_crawler_dag():
    

    @task
    def insert_collect_data_info(**kwargs) -> dict[str, dict[str, Any]]:
        
        pg_hook = PostgresHook(postgres_conn_id="car_db_conn")

        select_bsc_info_stmt = f"""
        SELECT * FROM std.tn_data_bsc_info tdbi
        WHERE 1=1
          AND LOWER(clct_yn) = 'y'
          AND LOWER(link_yn) = 'y'
          AND LOWER(pvsn_site_cd) = '{KCAR_PVSN_SITE_CD}'
          AND LOWER(datst_cd) IN ('{KCAR_DATST_BRAND}','{KCAR_DATST_CAR_TYPE}','{KCAR_DATST_LIST}')
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
            key
            for key in (KCAR_DATST_BRAND, KCAR_DATST_CAR_TYPE, KCAR_DATST_LIST)
            if key not in out
        ]
        if missing:
            raise ValueError(f"std.tn_data_bsc_info 조회 결과 누락: {missing} (pvsn_site_cd={KCAR_PVSN_SITE_CD})")
        return out

    @task
    def run_car_type_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        datst_cd = str((infos.get(KCAR_DATST_CAR_TYPE) or {}).get("datst_cd") or KCAR_DATST_CAR_TYPE).lower()
        return _run_kcar_car_type_csv(datst_cd, kwargs=kwargs)

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        datst_cd = str((infos.get(KCAR_DATST_BRAND) or {}).get("datst_cd") or KCAR_DATST_BRAND).lower()
        return _run_kcar_brand_csv(datst_cd, kwargs=kwargs)

    @task
    def run_list_csv(
        bsc_infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        car_type_csv_path: str,
        **kwargs,
    ) -> dict[str, Any]:
        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[KCAR_DATST_LIST])
        return run_kcar_list_job(
            tn_data_bsc_info,
            brand_list_csv_path=brand_csv_path,
            car_type_csv_path=car_type_csv_path or None,
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
            KCAR_COLLECT_DETAIL_TABLE,
            tn_data_bsc_info.datst_cd,
            csv_file_path,
        )
        registered_file_path = str(CommonUtil.build_collect_detail_file_path(tn_data_clct_dtl_info))
        logging.info(
            "K Car 수집 메타 등록: datst_cd=%s, file=%s, clct_pnttm=%s, status=%s",
            datst_cd,
            registered_file_path,
            tn_data_clct_dtl_info.clct_pnttm,
            getattr(tn_data_clct_dtl_info, "status", ""),
        )
        info = tn_data_clct_dtl_info.as_dict()
        info["file_path"] = registered_file_path
        info["status"] = getattr(tn_data_clct_dtl_info, "status", "")
        return info

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
            KCAR_COLLECT_DETAIL_TABLE,
            datst_cd,
        )
        if not tn_data_clct_dtl_info:
            raise ValueError(f"최신 수집 메타 정보가 없습니다: datst_cd={datst_cd}")

        latest_csv_path = CommonUtil.build_collect_detail_file_path(tn_data_clct_dtl_info)
        if not latest_csv_path.is_file():
            raise ValueError(f"CSV 수집 완료 조건 미충족: datst_cd={datst_cd}, path={latest_csv_path}")

        target_table = _resolve_target_table_for_datst(datst_cd, tn_data_bsc_info.ods_tbl_phys_nm)
        rows = _read_csv_rows(latest_csv_path)
        if not rows:
            raise ValueError(f"적재할 CSV 데이터가 없습니다: datst_cd={datst_cd}, path={latest_csv_path}")

        if datst_cd == KCAR_DATST_LIST:
            original_count = len(rows)
            rows = _dedupe_kcar_list_rows(rows)
            if len(rows) != original_count:
                logging.info(
                    "K Car list 중복 제거: before=%d, after=%d, removed=%d",
                    original_count,
                    len(rows),
                    original_count - len(rows),
                )
            target_table, _ = _resolve_list_table_targets(tn_data_bsc_info)

        _bulk_insert_rows(hook, target_table, rows, truncate=True, allow_only_table_cols=True)
        table_count = CommonUtil.get_table_row_count(hook, target_table)
        logging.info(
            "K Car CSV 적재 완료: datst_cd=%s, table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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

        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[KCAR_DATST_LIST])
        tmp_table, source_table = _resolve_list_table_targets(tn_data_bsc_info)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        sync_result = _sync_kcar_tmp_to_source(hook, tmp_table=tmp_table, source_table=source_table)
        logging.info(
            "K Car list tmp->source 반영 완료: tmp_table=%s, source_table=%s, tmp_count=%d, source_count=%d",
            tmp_table,
            source_table,
            sync_result["tmp_count"],
            sync_result["source_count"],
        )
        return {
            "done": True,
            "tmp_table": tmp_table,
            "source_table": source_table,
            "tmp_count": sync_result["tmp_count"],
            "source_count": sync_result["source_count"],
        }

    @task_group(group_id="create_csv_process")
    def create_csv_process(bsc_infos: dict[str, dict[str, Any]]) -> dict[str, Any]:
        car_type_path = run_car_type_csv(bsc_infos)
        car_type_collect_info = register_csv_collect_log_info.override(
            task_id="register_car_type_collect_log_info"
        )(bsc_infos, KCAR_DATST_CAR_TYPE, car_type_path)

        brand_path = run_brand_csv(bsc_infos)
        brand_collect_info = register_csv_collect_log_info.override(
            task_id="register_brand_collect_log_info"
        )(bsc_infos, KCAR_DATST_BRAND, brand_path)

        list_result = run_list_csv(bsc_infos, brand_path, car_type_path)
        list_collect_info = register_csv_collect_log_info.override(
            task_id="register_list_collect_log_info"
        )(bsc_infos, KCAR_DATST_LIST, list_result["list_csv"])

        return {
            KCAR_DATST_BRAND: brand_collect_info,
            KCAR_DATST_CAR_TYPE: car_type_collect_info,
            KCAR_DATST_LIST: list_collect_info,
        }

    @task_group(group_id="insert_csv_process")
    def insert_csv_process(
        bsc_infos: dict[str, dict[str, Any]],
        tn_data_clct_dtl_info_map: dict[str, dict[str, Any]],
    ) -> None:
        brand_load_result = load_csv_to_ods.override(task_id="load_brand_csv_to_ods")(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            KCAR_DATST_BRAND,
        )
        car_type_load_result = load_csv_to_ods.override(task_id="load_car_type_csv_to_ods")(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            KCAR_DATST_CAR_TYPE,
        )
        list_load_result = load_csv_to_ods.override(task_id="load_list_csv_to_ods")(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            KCAR_DATST_LIST,
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


USED_CAR_SITE_NAMES_VAR = "used_car_site_names"
CRAWL_BASE_PATH_VAR = "crawl_base_path"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
COLLECT_LOG_FILE_PATH_VAR = "used_car_collect_log_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"

KCAR_PVSN_SITE_CD = "ps00004"
KCAR_DATST_BRAND = "data10"
KCAR_DATST_CAR_TYPE = "data11"
KCAR_DATST_LIST = "data12"
KCAR_SITE_NAME = "케이카"
KCAR_BRAND_TABLE = "ods.ods_brand_list_kcar"
KCAR_CAR_TYPE_TABLE = "ods.ods_car_type_list_kcar"
KCAR_TMP_LIST_TABLE = "ods.ods_tmp_car_list_kcar"
KCAR_SOURCE_LIST_TABLE = "ods.ods_car_list_kcar"
KCAR_COLLECT_DETAIL_TABLE = "std.tn_data_clct_dtl_info"

URL = "https://www.kcar.com/bc/search"
DETAIL_URL_TEMPLATE = "https://www.kcar.com/bc/detail/carInfoDtl?i_sCarCd={}"
CAR_LIST_BOX_SELECTOR = ".resultCnt .carListWrap .carListBox"
HEADLESS_MODE = os.environ.get("KCAR_HEADED", "0").strip().lower() not in ("1", "true", "yes")
USE_CHROME_CHANNEL = os.environ.get("KCAR_CHROME_CHANNEL", "0").strip().lower() in ("1", "true", "yes")

YEAR_STR = ""
DATE_STR = ""
RUN_TS = ""
RESULT_DIR = Path("/tmp")
LOG_DIR = Path("/tmp")
IMG_BASE = Path("/tmp")


def _norm(value: str) -> str:
    return " ".join((value or "").strip().split())


def _normalize_target_table(raw: str | None) -> str | None:
    if not raw:
        return None
    s = str(raw).strip().strip('"').strip("'").strip()
    s = s.replace('"ods."', "ods.").replace("'ods.'", "ods.")
    s = s.replace('"', "").replace("'", "").strip()
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
    if key == KCAR_DATST_BRAND:
        return normalized or KCAR_BRAND_TABLE
    if key == KCAR_DATST_CAR_TYPE:
        return normalized or KCAR_CAR_TYPE_TABLE
    if key == KCAR_DATST_LIST:
        return normalized or KCAR_TMP_LIST_TABLE
    if normalized:
        return normalized
    raise ValueError(f"적재 대상 테이블을 확인할 수 없습니다: datst_cd={datst_cd}")


def _resolve_list_table_targets(tn_data_bsc_info: TnDataBscInfo) -> tuple[str, str]:
    tmp_table = _normalize_target_table(getattr(tn_data_bsc_info, "tmpr_tbl_phys_nm", None)) or KCAR_TMP_LIST_TABLE
    source_table = _normalize_target_table(getattr(tn_data_bsc_info, "ods_tbl_phys_nm", None)) or KCAR_SOURCE_LIST_TABLE
    return tmp_table, source_table


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
    result_root = _get_result_root_path(kwargs=kwargs)
    if result_root.name.lower() == "crawl":
        return result_root.parent / "img"
    return _get_crawl_base_path(kwargs=kwargs) / "img"


def get_site_name_by_datst(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    key = (datst_cd or "").lower().strip()
    mapping: dict[str, Any] = {}
    raw = _get_context_var_json(kwargs, USED_CAR_SITE_NAMES_VAR)
    try:
        if raw is None:
            raw = Variable.get(USED_CAR_SITE_NAMES_VAR, default_var="{}", deserialize_json=True)
        if isinstance(raw, dict):
            mapping = {str(k).lower(): v for k, v in raw.items()}
    except Exception:
        mapping = {}
    name = mapping.get(key)
    if name is not None and str(name).strip():
        return str(name).strip()
    return KCAR_SITE_NAME


def get_kcar_site_name(kwargs: dict[str, Any] | None = None) -> str:
    for datst_cd in (KCAR_DATST_LIST, KCAR_DATST_BRAND, KCAR_DATST_CAR_TYPE):
        site_name = get_site_name_by_datst(datst_cd, kwargs=kwargs)
        if site_name and site_name.strip():
            return site_name
    return KCAR_SITE_NAME


def activate_paths_for_datst(datst_cd: str, kwargs: dict[str, Any] | None = None) -> None:
    global RESULT_DIR, LOG_DIR, IMG_BASE, YEAR_STR, DATE_STR, RUN_TS

    result_root = _get_result_root_path(kwargs=kwargs)
    log_root = _get_log_root_path(kwargs=kwargs)
    img_root = _get_img_root_path(kwargs=kwargs)
    site = get_kcar_site_name(kwargs=kwargs)
    now = datetime.now()
    YEAR_STR = now.strftime("%Y년")
    DATE_STR = now.strftime("%Y%m%d")
    RUN_TS = now.strftime("%Y%m%d%H%M")

    RESULT_DIR = CommonUtil.build_dated_site_path(result_root, site, now)
    LOG_DIR = CommonUtil.build_dated_site_path(log_root, site, now)
    IMG_BASE = CommonUtil.build_year_site_path(img_root, site, now)


try:
    activate_paths_for_datst(KCAR_DATST_LIST)
except Exception:
    pass


def _split_schema_table(full_name: str) -> tuple[str, str]:
    if "." in full_name:
        schema, table = full_name.split(".", 1)
        return schema.strip(), table.strip()
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


def _read_csv_rows(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _bulk_insert_rows(
    hook: PostgresHook,
    full_table_name: str,
    rows: list[dict[str, Any]],
    *,
    truncate: bool = False,
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
    insert_cols = [c for c in candidate_cols if c in table_col_set] if table_cols else candidate_cols
    if not insert_cols:
        raise ValueError(f"insert 가능한 컬럼이 없습니다. table={full_table_name}")

    values = [tuple(row.get(col) for col in insert_cols) for row in rows]
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            if truncate:
                cur.execute(f"TRUNCATE TABLE {full_table_name}")
            from psycopg2.extras import execute_values

            cols_sql = ", ".join([f'"{col}"' for col in insert_cols])
            sql = f"INSERT INTO {full_table_name} ({cols_sql}) VALUES %s"
            execute_values(cur, sql, values, page_size=2000)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _sync_kcar_tmp_to_source(
    hook: PostgresHook,
    *,
    tmp_table: str,
    source_table: str,
    key_cols: tuple[str, str] = ("product_id", "detail_url"),
    flag_col: str = "register_flag",
) -> dict[str, int]:
    tmp_count = CommonUtil.get_table_row_count(hook, tmp_table)
    if tmp_count == 0:
        return {"tmp_count": 0, "source_count": CommonUtil.get_table_row_count(hook, source_table)}

    tmp_cols = _get_table_columns(hook, tmp_table)
    src_cols = _get_table_columns(hook, source_table)
    tmp_has_flag = flag_col in set(tmp_cols)
    src_has_flag = flag_col in set(src_cols)
    join_condition = " AND ".join([f'COALESCE(src."{col}", \'\') = COALESCE(tmp."{col}", \'\')' for col in key_cols])
    not_exists_condition = " AND ".join([f'COALESCE(tmp."{col}", \'\') = COALESCE(src."{col}", \'\')' for col in key_cols])

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            if tmp_has_flag:
                cur.execute(
                    f"""
                    UPDATE {tmp_table} AS tmp
                    SET "{flag_col}" = CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM {source_table} AS src
                            WHERE {join_condition}
                        ) THEN 'Y'
                        ELSE 'A'
                    END
                    """
                )

            update_cols = [c for c in tmp_cols if c in set(src_cols) and c not in set(key_cols)]
            set_expr = ", ".join([f'"{c}" = tmp."{c}"' for c in update_cols if c != flag_col])
            if src_has_flag:
                set_expr = (set_expr + ", " if set_expr else "") + f'"{flag_col}" = \'Y\''
            if set_expr:
                cur.execute(
                    f"""
                    UPDATE {source_table} AS src
                    SET {set_expr}
                    FROM {tmp_table} AS tmp
                    WHERE {join_condition}
                    """
                )

            insert_cols = [c for c in tmp_cols if c in set(src_cols)]
            if src_has_flag and flag_col not in insert_cols:
                insert_cols.append(flag_col)
            cols_sql = ", ".join([f'"{c}"' for c in insert_cols])
            select_cols_sql = ", ".join(
                [
                    ("tmp." + f'"{c}"') if c != flag_col else ("tmp." + f'"{c}"' if tmp_has_flag else "'A'")
                    for c in insert_cols
                ]
            )
            cur.execute(
                f"""
                INSERT INTO {source_table} ({cols_sql})
                SELECT {select_cols_sql}
                FROM {tmp_table} tmp
                LEFT JOIN {source_table} src
                  ON {join_condition}
                WHERE src."{key_cols[0]}" IS NULL
                  AND src."{key_cols[1]}" IS NULL
                """
            )

            if src_has_flag:
                cur.execute(
                    f"""
                    UPDATE {source_table} src
                    SET "{flag_col}" = 'N'
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {tmp_table} tmp
                        WHERE {not_exists_condition}
                    )
                    """
                )
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {
        "tmp_count": CommonUtil.get_table_row_count(hook, tmp_table),
        "source_count": CommonUtil.get_table_row_count(hook, source_table),
    }


def _get_file_logger(run_ts: str) -> logging.Logger:
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


def _dismiss_kcar_popups(page) -> None:
    for _ in range(3):
        clicked = False
        try:
            btn = page.locator("button:has-text('확인')").first
            if btn.count() > 0 and btn.is_visible(timeout=700):
                btn.click()
                page.wait_for_timeout(250)
                clicked = True
        except Exception:
            clicked = False
        if not clicked:
            break


def _ensure_kcar_search_page(page, logger) -> None:
    if URL not in (page.url or ""):
        page.goto(URL, wait_until="domcontentloaded", timeout=90000)
    page.wait_for_selector(".kcarSnb, .kcarSnbWrap, .menuItemList", timeout=30000)
    page.wait_for_timeout(600)
    _dismiss_kcar_popups(page)
    try:
        page.wait_for_load_state("networkidle", timeout=2500)
    except Exception:
        pass
    logger.debug("K Car 검색 페이지 준비 완료")


def _ensure_filter_section_open(page, aria_label: str) -> None:
    try:
        page.evaluate(
            """
            target => {
                const header = document.querySelector(`[aria-label="${target}"]`);
                if (header) header.click();
            }
            """,
            aria_label,
        )
        page.wait_for_timeout(250)
    except Exception:
        pass


def _get_kcar_car_type_labels(page):
    try:
        header = page.locator('[aria-label="차종"]').first
        if header.is_visible(timeout=2000):
            header.click()
            page.wait_for_timeout(1000)
    except Exception:
        pass

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

    try:
        first_menu = page.locator(".kcarSnb .menuItemList").first
        first_menu.wait_for(state="visible", timeout=3000)
        labels = first_menu.locator(".el-checkbox .el-checkbox__label")
        if labels.count() > 0:
            return labels
    except Exception:
        pass

    try:
        first_group = page.locator(".kcarSnb .el-checkbox-group.chkGroup").first
        first_group.wait_for(state="visible", timeout=3000)
        labels = first_group.locator(".el-checkbox .el-checkbox__label")
        if labels.count() > 0:
            return labels
    except Exception:
        pass
    return None


def _get_kcar_car_type_names(page) -> list[str]:
    labels = _get_kcar_car_type_labels(page)
    if not labels:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for i in range(labels.count()):
        try:
            name = _norm(labels.nth(i).inner_text())
        except Exception:
            continue
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _normalize_kcar_car_type_label(text: str) -> str:
    """
    차종 라벨(체크박스) 텍스트는 종종 '(123)' 같은 건수가 붙습니다.
    선택/비교는 건수 제거 후 정확히 매칭되도록 정규화합니다.
    """
    s = _norm(text or "")
    if not s:
        return ""
    # 예: "준중형차(1,234)" -> "준중형차"
    s = re.sub(r"\([^)]*\)\s*$", "", s).strip()
    return s


def _set_kcar_exact_car_type_filter(page, target_label_text: str, logger) -> bool:
    """
    차종 체크박스를 '정확히 1개만' 선택 상태로 만듭니다.
    - '승합차' vs '경승합차' 같이 포함 관계가 있어도, normalize 후 '완전 일치'로만 선택합니다.
    """
    desired = _normalize_kcar_car_type_label(target_label_text)
    if not desired:
        return False

    try:
        changed = page.evaluate(
            """
            desiredRaw => {
                const desired = (desiredRaw || '').trim();
                const normalize = (t) => {
                    t = (t || '').replace(/\\s+/g, ' ').trim();
                    // trailing "(...)" 제거
                    t = t.replace(/\\([^)]*\\)\\s*$/, '').trim();
                    return t;
                };

                const header = document.querySelector('[aria-label="차종"]');
                if (header) {
                    try { header.click(); } catch (e) {}
                }

                const root =
                    document.querySelector('.kcarSnb .el-collapse-item:has([aria-label="차종"])') ||
                    document.querySelector('.kcarSnb .menuItemList') ||
                    document.querySelector('.kcarSnb .el-checkbox-group.chkGroup');
                if (!root) return { ok: false, changed: 0, reason: 'no-root' };

                const boxes = Array.from(root.querySelectorAll('.el-checkbox'));
                const getLabel = (box) => {
                    const lbl = box.querySelector('.el-checkbox__label') || box.querySelector('label');
                    return normalize(lbl ? (lbl.textContent || '') : '');
                };
                const isChecked = (box) =>
                    box.classList.contains('is-checked') || box.getAttribute('aria-checked') === 'true';
                const clickBox = (box) => {
                    const clickable = box.querySelector('label') || box.querySelector('.el-checkbox__label') || box;
                    if (!clickable) return false;
                    try { clickable.click(); return true; } catch (e) { return false; }
                };

                let targetBox = null;
                for (const box of boxes) {
                    if (getLabel(box) === desired) { targetBox = box; break; }
                }
                if (!targetBox) return { ok: false, changed: 0, reason: 'not-found' };

                let changed = 0;
                // 1) 타깃 외에 체크된 건 전부 해제
                for (const box of boxes) {
                    if (box === targetBox) continue;
                    if (isChecked(box)) {
                        if (clickBox(box)) changed += 1;
                    }
                }
                // 2) 타깃이 미체크면 체크
                if (!isChecked(targetBox)) {
                    if (clickBox(targetBox)) changed += 1;
                }
                return { ok: true, changed, reason: 'ok' };
            }
            """,
            desired,
        )
        ok = bool((changed or {}).get("ok"))
        if not ok:
            logger.warning("차종 단일 선택 실패: target=%s, result=%s", desired, changed)
            return False
        page.wait_for_timeout(1200)
        return True
    except Exception as e:
        logger.warning("차종 단일 선택 예외: target=%s, err=%s", desired, e)
        return False


def _reset_kcar_car_type_filters(page, logger) -> bool:
    try:
        reset_count = page.evaluate(
            """
            () => {
                const header = document.querySelector('[aria-label="차종"]');
                if (header) {
                    try { header.click(); } catch (e) {}
                }

                const root =
                    document.querySelector('.kcarSnb .el-collapse-item:has([aria-label="차종"])') ||
                    document.querySelector('.kcarSnb .menuItemList') ||
                    document.querySelector('.kcarSnb .el-checkbox-group.chkGroup');
                if (!root) return 0;

                const checkedNodes = Array.from(
                    root.querySelectorAll('.el-checkbox.is-checked, .el-checkbox[aria-checked="true"]')
                );

                let count = 0;
                checkedNodes.forEach(node => {
                    const clickable =
                        node.querySelector('label') ||
                        node.querySelector('.el-checkbox__label') ||
                        node;
                    if (!clickable) return;
                    try {
                        clickable.click();
                        count += 1;
                    } catch (e) {}
                });
                return count;
            }
            """
        )
        if reset_count:
            page.wait_for_timeout(1200)
            logger.info("차종 필터 초기화: %d건 해제", reset_count)
            return True
    except Exception:
        pass
    return False


def run_kcar_car_type_list(page, result_dir: Path, logger, csv_path: Path | None = None) -> None:
    csv_path = csv_path or result_dir / "kcar_car_type_list.csv"
    if csv_path.exists():
        csv_path.unlink()
    headers = ["car_type_sn", "car_type_name", "date_crtr_pnttm", "create_dt"]

    try:
        page.goto(URL, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(3000)
        _dismiss_kcar_popups(page)
        page.wait_for_selector(".kcarSnb, .kcarSnbWrap, .menuItemList", timeout=20000)
        page.wait_for_timeout(1500)

        labels = _get_kcar_car_type_labels(page)
        if not labels:
            logger.warning("차종 라벨 요소를 찾지 못했습니다.")
            return

        n = labels.count()
        now = datetime.now()
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for idx in range(n):
                try:
                    name = labels.nth(idx).inner_text().strip()
                except Exception:
                    continue
                if not name:
                    continue
                w.writerow(
                    {
                        "car_type_sn": idx + 1,
                        "car_type_name": name,
                        "date_crtr_pnttm": now.strftime("%Y%m%d"),
                        "create_dt": now.strftime("%Y%m%d%H%M"),
                    }
                )
        logger.info("K Car 차종 목록 저장 완료: %s (총 %d건)", csv_path, n)
    except Exception as e:
        logger.error("차종 수집 오류: %s", e, exc_info=True)


def _model_list_format(text: str) -> str:
    if not text or "(" not in text:
        return (text or "").strip()
    last_open = text.rfind("(")
    prefix = text[:last_open].strip()
    suffix = text[last_open:].strip()
    if not prefix:
        return text.strip()
    return f"{prefix}|{suffix}"


def _collect_brand_depth1_block(
    page,
    depth1_block,
    depth_1_value: str,
    csv_path: Path,
    headers: list[str],
    logger,
    model_sn_ref: list[int],
    *,
    start_index: int = 0,
    total_brands: int = 0,
) -> int:
    def append_row(row: dict[str, Any]) -> None:
        row["model_sn"] = model_sn_ref[0]
        model_sn_ref[0] += 1

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

        if original_ml:
            row["model_list"] = original_ml.split("|", 1)[0].strip() if "|" in original_ml else original_ml.strip()

        now = datetime.now()
        row.setdefault("date_crtr_pnttm", now.strftime("%Y%m%d"))
        row.setdefault("create_dt", now.strftime("%Y%m%d%H%M"))

        normalized: dict[str, Any] = {}
        for key in headers:
            val = row.get(key, "")
            if key == "model_sn":
                normalized[key] = val
                continue
            if isinstance(val, str):
                val = val.strip()
            normalized[key] = val if val else "-"

        with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=headers).writerow(normalized)

    def _wait_depth3_visible(d2_li) -> bool:
        try:
            d2_li.locator(".depth3").first.wait_for(state="visible", timeout=6000)
            return True
        except Exception:
            return False

    def _count_direct_depth2(block) -> int:
        c = block.locator(".depth2 > ul > li").count()
        if c > 0:
            return c
        c = block.locator(".depth2 > li").count()
        if c > 0:
            return c
        return block.locator(".depth2 li").count()

    try:
        depth1_block.scroll_into_view_if_needed()
        page.wait_for_timeout(500)
    except Exception:
        pass

    depth2_lis = depth1_block.locator(".depth2 > ul > li")
    if depth2_lis.count() == 0:
        depth2_lis = depth1_block.locator(".depth2 > li")
    if depth2_lis.count() == 0:
        depth2_lis = depth1_block.locator(".depth2 li")
    if depth2_lis.count() == 0:
        return 0

    try:
        depth2_lis.first.wait_for(state="visible", timeout=10000)
    except Exception:
        return 0

    row_count = 0
    depth2_count = _count_direct_depth2(depth1_block)
    for i in range(depth2_count):
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
            continue

        current_num = start_index + i + 1
        if total_brands > 0:
            logger.info("[%d/%d] 브랜드 처리 중 [%s]: %s", current_num, total_brands, depth_1_value, depth_2)

        expanded = False
        for click_selector in ("label", ".el-checkbox"):
            try:
                d2_li.locator(click_selector).first.click()
                page.wait_for_timeout(1200)
                if _wait_depth3_visible(d2_li):
                    expanded = True
                    break
            except Exception:
                continue
        if not expanded:
            try:
                d2_li.evaluate("el => el.querySelector('label')?.click()")
                page.wait_for_timeout(1200)
                expanded = _wait_depth3_visible(d2_li)
            except Exception:
                expanded = False
        if not expanded:
            continue

        depth3_container = d2_li.locator("ul.depth3").first
        depth3_lis = depth3_container.locator("> li")
        if depth3_lis.count() == 0:
            depth3_lis = d2_li.locator(".depth3 > li")
        if depth3_lis.count() == 0:
            continue

        row_count_this_brand = 0
        for j in range(depth3_lis.count()):
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
                    depth_3 = (el.inner_text(timeout=1000) or el.text_content() or "").strip()
                    if depth_3:
                        break
                except Exception:
                    continue
            if not depth_3:
                continue

            expanded_d3 = False
            for click_selector in ("label", ".el-checkbox"):
                try:
                    d3_li.locator(click_selector).first.click()
                    page.wait_for_timeout(1200)
                    d3_li.locator("ul.depth4 > li .el-checkbox__label").first.wait_for(
                        state="visible", timeout=10000
                    )
                    expanded_d3 = True
                    break
                except Exception:
                    continue

            if not expanded_d3:
                append_row(
                    {
                        "depth_1": depth_1_value,
                        "brand_list": depth_2,
                        "car_list": depth_3,
                        "model_list": "-",
                        "model_list_1": "-",
                        "model_list_2": "-",
                    }
                )
                row_count += 1
                row_count_this_brand += 1
                continue

            depth4_lis = d3_li.locator("ul.depth4 > li")
            depth4_added = 0
            for k in range(depth4_lis.count()):
                d4_li = depth4_lis.nth(k)
                depth_4 = ""
                for sel in (".el-checkbox__label", "label.el-checkbox__label", "label", ".el-checkbox label"):
                    try:
                        el = d4_li.locator(sel).first
                        depth_4 = (el.inner_text(timeout=800) or el.text_content() or "").strip()
                        if depth_4:
                            break
                    except Exception:
                        continue
                if not depth_4:
                    continue

                depth_5_values: list[tuple[str, Any | None]] = []
                try:
                    d4_li.locator("label").first.click()
                    page.wait_for_timeout(800)
                except Exception:
                    pass

                depth5_lis = d4_li.locator("ul.depth5 > li")
                if depth5_lis.count() > 0:
                    for m in range(depth5_lis.count()):
                        d5_li = depth5_lis.nth(m)
                        depth_5 = ""
                        for sel in (".el-checkbox__label", "label.el-checkbox__label", "label", ".el-checkbox label"):
                            try:
                                el = d5_li.locator(sel).first
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
                    depth_6_values = ["-"]
                    if d5_li is not None:
                        try:
                            d5_li.locator("label").first.click()
                            page.wait_for_timeout(600)
                        except Exception:
                            pass
                        depth6_lis = d5_li.locator("ul.depth6 > li")
                        if depth6_lis.count() > 0:
                            depth_6_values = []
                            for n in range(depth6_lis.count()):
                                d6_li = depth6_lis.nth(n)
                                depth_6 = ""
                                for sel in (
                                    ".el-checkbox__label",
                                    "label.el-checkbox__label",
                                    "label",
                                    ".el-checkbox label",
                                ):
                                    try:
                                        el = d6_li.locator(sel).first
                                        depth_6 = (el.inner_text(timeout=500) or el.text_content() or "").strip()
                                        if depth_6:
                                            break
                                    except Exception:
                                        continue
                                if depth_6:
                                    depth_6_values.append(depth_6)
                            if not depth_6_values:
                                depth_6_values = ["-"]

                    for depth_6 in depth_6_values:
                        append_row(
                            {
                                "depth_1": depth_1_value,
                                "brand_list": depth_2,
                                "car_list": depth_3,
                                "model_list": _model_list_format(depth_4),
                                "model_list_1": depth_5 if depth_5 != "-" else "-",
                                "model_list_2": depth_6 if depth_6 != "-" else "-",
                            }
                        )
                        row_count += 1
                        row_count_this_brand += 1
                        depth4_added += 1

            if depth4_added == 0 and depth_3:
                append_row(
                    {
                        "depth_1": depth_1_value,
                        "brand_list": depth_2,
                        "car_list": depth_3,
                        "model_list": "-",
                        "model_list_1": "-",
                        "model_list_2": "-",
                    }
                )
                row_count += 1
                row_count_this_brand += 1

            try:
                d3_li.locator("label").first.click()
                page.wait_for_timeout(300)
            except Exception:
                pass

        logger.info("  → [%s] '%s' 수집 완료 (%d건)", depth_1_value, depth_2, row_count_this_brand)
        try:
            d2_li.locator("label").first.click()
            page.wait_for_timeout(400)
        except Exception:
            pass

    return row_count


def run_kcar_brand_list(page, result_dir: Path, logger, csv_path: Path | None = None) -> None:
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

    try:
        page.goto(URL, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(5000)
        _dismiss_kcar_popups(page)

        for snb_sel in (".kcarSnb", ".kcarSnbWrap", ".menuItemList", "[class*='kcarSnb']", ".el-collapse-item__content"):
            try:
                page.wait_for_selector(snb_sel, timeout=15000)
                page.wait_for_timeout(1500)
                break
            except Exception:
                continue

        try:
            header = page.locator('[aria-label="제조사/모델"]').first
            header.wait_for(state="visible", timeout=8000)
            header.click()
            page.wait_for_timeout(2500)
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
            logger.warning("제조사/모델 .modelList 영역을 찾을 수 없습니다.")
            return

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

        if depth1_count >= 2:
            try:
                imported_block = depth1_blocks.nth(1)
                imported_block.scroll_into_view_if_needed()
                page.wait_for_timeout(500)
                for _ in range(3):
                    cnt = _count_depth2_direct(imported_block)
                    if cnt > 0:
                        break
                    try:
                        imported_block.locator("label").first.click()
                    except Exception:
                        try:
                            page.locator("text=수입").first.click()
                        except Exception:
                            pass
                    page.wait_for_timeout(1500)
            except Exception:
                pass

        depth2_counts = []
        for idx in range(min(depth1_count, len(depth_1_labels))):
            depth2_counts.append(_count_depth2_direct(depth1_blocks.nth(idx)))
        total_brands = sum(depth2_counts)

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=headers).writeheader()

        total_rows = 0
        start_index = 0
        model_sn_ref = [1]
        for idx in range(min(depth1_count, len(depth_1_labels))):
            depth_1_value = depth_1_labels[idx]
            depth1_block = depth1_blocks.nth(idx)
            if depth2_counts[idx] == 0:
                continue
            n = _collect_brand_depth1_block(
                page,
                depth1_block,
                depth_1_value,
                csv_path,
                headers,
                logger,
                model_sn_ref,
                start_index=start_index,
                total_brands=total_brands,
            )
            total_rows += n
            start_index += depth2_counts[idx]

        logger.info("K Car 브랜드 목록 저장 완료: %s (총 %d건)", csv_path, total_rows)
    except Exception as e:
        logger.error("브랜드 목록 수집 오류: %s", e, exc_info=True)


def _extract_product_id_from_img_src(src: str) -> str:
    if not src:
        return ""
    m = re.search(r"/(\d+)_\d+/main/", src)
    if m:
        return m.group(1)
    m = re.search(r"kcarM_(\d+)_", src)
    if m:
        return m.group(1)
    return ""


def _extract_product_id_from_href(href: str) -> str:
    if not href:
        return ""
    m = re.search(r"/detail/(\d+)(?:\?|$|/)", href)
    if m:
        return m.group(1)
    m = re.search(r"i_sCarCd=EC?(\d+)", href, re.I)
    if m:
        return m.group(1)
    return ""


def _normalize_key(s: str) -> str:
    return " ".join((s or "").split()) if s else ""


def _load_brand_list_lookup(result_dir: Path, brand_path: Path | None = None) -> dict[str, tuple[str, str, str, str, str]]:
    path = brand_path or result_dir / "kcar_brand_list.csv"
    lookup: dict[str, tuple[str, str, str, str, str]] = {}
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
                parts_full = [p for p in [brand, car, model, m1] if p and p != "-"]
                if m2 and m2 != "-":
                    parts_full.append(m2)
                parts_no_car = [p for p in [brand, model, m1] if p and p != "-"]
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
    if not images_enabled():
        return False
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


def _dedupe_kcar_list_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def _get_kcar_list_signature(page) -> str:
    try:
        return page.evaluate(
            """
            () => {
                const boxes = Array.from(document.querySelectorAll('.resultCnt .carListWrap .carListBox'));
                const valid = boxes.find(box => !box.querySelector('.adBannerBox'));
                if (!valid) return '';
                const link = valid.querySelector('a[href*="/detail/"], a[href*="i_sCarCd"]');
                const img = valid.querySelector('.carListImg img');
                const href = (link && link.getAttribute('href')) || '';
                const src = (img && img.getAttribute('src')) || '';
                return `${boxes.length}|${href}|${src}`;
            }
            """
        ) or ""
    except Exception:
        return ""


def _wait_kcar_list_refresh(page, previous_signature: str, timeout_ms: int = 12000) -> None:
    elapsed = 0
    while elapsed < timeout_ms:
        page.wait_for_timeout(500)
        elapsed += 500
        current_signature = _get_kcar_list_signature(page)
        if current_signature and current_signature != previous_signature:
            return


def _goto_next_kcar_page(page) -> bool:
    try:
        clicked = page.evaluate(
            """
            () => {
                const root =
                    document.querySelector('.pagination.-sm .paging') ||
                    document.querySelector('.pagination .paging') ||
                    document.querySelector('.paging');
                if (!root) return false;

                const pageNums = Array.from(root.querySelectorAll('.pagingNum'));
                const current =
                    root.querySelector('.pagingNum.on') ||
                    root.querySelector('.pagingNum.active') ||
                    root.querySelector('.pagingNum[aria-current="page"]') ||
                    pageNums.find(node => node.getAttribute('aria-current') === 'page');

                if (current) {
                    let nextNode = current.nextElementSibling;
                    while (nextNode) {
                        if (nextNode.classList && nextNode.classList.contains('pagingNum')) {
                            nextNode.click();
                            return true;
                        }
                        nextNode = nextNode.nextElementSibling;
                    }
                }

                const nextImg = root.querySelector('img[alt="다음"]');
                if (nextImg) {
                    const clickable = nextImg.closest('a, button, span, li, div');
                    if (clickable) {
                        clickable.click();
                        return true;
                    }
                }

                const nextText = Array.from(root.querySelectorAll('a, button, span, li, div')).find(node => {
                    const text = (node.textContent || '').replace(/\\s+/g, ' ').trim();
                    return text === '다음';
                });
                if (nextText) {
                    nextText.click();
                    return true;
                }

                return false;
            }
            """
        )
        return bool(clicked)
    except Exception:
        return False


def _get_kcar_expected_count(page) -> int:
    """
    상단 h2 타이틀에 표시되는 직영중고차 대수를 읽습니다(초기 목록 / 차종 필터 적용 후 공통).
    예)
      K Car 직영중고차 9,356대
      <h2 class="subTitle mt64 ft22">... <span class="textRed">9,356</span>대
    """
    try:
        raw = page.evaluate(
            """
            () => {
                const selectors = [
                    'h2.subTitle.mt64.ft22 .textRed',
                    'h2.subTitle.mt64.ft22 span.textRed',
                    'h2.subTitle .textRed',
                    'h2.subTitle span.textRed',
                    '.subTitbox .subTitle .textRed',
                    '.subTitbox .subTitle span.textRed',
                    '.subTitbox h2 .textRed',
                    '.subTitbox h2 span.textRed',
                ];
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (el) {
                        const t = (el.textContent || '').trim();
                        if (t) return t;
                    }
                }
                return '';
            }
            """
        )
        s = (raw or "").strip()
        if not s:
            return 0
        s = re.sub(r"[^0-9]", "", s)
        return int(s) if s else 0
    except Exception:
        return 0


def run_kcar_list(
    page,
    result_dir: Path,
    logger,
    csv_path: Path | None = None,
    brand_path: Path | None = None,
    save_dir: Path | None = None,
) -> None:
    csv_path = csv_path or result_dir / "kcar_list.csv"
    headers = [
        "model_sn",
        "product_id",
        "car_type_name",
        "brand_list",
        "car_list",
        "model_list",
        "model_list_1",
        "model_list_2",
        "car_name",
        "car_exp",
        "car_pay_meth",
        "release_dt",
        "car_navi",
        "car_fuel",
        "local_dos",
        "detail_url",
        "car_imgs",
        "date_crtr_pnttm",
        "create_dt",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow(headers)

    list_save_dir = Path(save_dir) if save_dir is not None else (result_dir / "imgs" / "list")
    list_save_dir.mkdir(parents=True, exist_ok=True)
    brand_lookup = _load_brand_list_lookup(result_dir, brand_path=brand_path)

    model_sn = 0
    now = datetime.now()
    base_date_crtr_pnttm = now.strftime("%Y%m%d")
    base_create_dt = now.strftime("%Y%m%d%H%M")

    try:
        logger.info("K Car 페이지 접속 중...")
        last_err: Exception | None = None
        for attempt in range(3):
            try:
                _ensure_kcar_search_page(page, logger)
                break
            except Exception as e:
                last_err = e
                logger.warning("K Car 페이지 접속 재시도(%d/3): %s", attempt + 2, e)
                try:
                    page.wait_for_timeout(1500)
                    page.reload(wait_until="domcontentloaded", timeout=90000)
                    page.wait_for_timeout(1200)
                except Exception:
                    pass
                if attempt >= 1:
                    time.sleep(2)
        else:
            raise last_err if last_err is not None else RuntimeError("K Car 페이지 접속 실패")

        direct_site_total = _get_kcar_expected_count(page)
        if direct_site_total:
            if brand_lookup:
                logger.info(
                    "K Car 직영중고차 %d대 표시 ",
                    direct_site_total,
                )
            else:
                logger.info("K Car 직영중고차 %d대 표시", direct_site_total)
        elif brand_lookup:
            logger.info(
                "brand_list 매칭용 %d건 로드됨 (직영중고차 표시 대수: 읽기 실패)",
                len(brand_lookup),
            )

        labels_locator = _get_kcar_car_type_labels(page)
        if not labels_locator:
            logger.error("차종 목록을 찾을 수 없습니다.")
            return

        n_car_types = labels_locator.count()
        logger.info("총 %d개 차종 대상 수집 시작", n_car_types)
        seen_product_ids: set[str] = set()

        for idx in range(n_car_types):
            _reset_kcar_car_type_filters(page, logger)
            try:
                current_labels = _get_kcar_car_type_labels(page)
                target_label = current_labels.nth(idx)
                raw_label_text = target_label.inner_text().strip()
                car_type_name = _normalize_kcar_car_type_label(raw_label_text) or raw_label_text
                if not _set_kcar_exact_car_type_filter(page, raw_label_text, logger):
                    # fallback: 기존 방식
                    target_label.scroll_into_view_if_needed()
                    target_label.click()
                    page.wait_for_timeout(3000)
            except Exception as e:
                logger.warning("%d번째 차종 선택 실패: %s", idx, e)
                continue

            try:
                page.wait_for_selector(CAR_LIST_BOX_SELECTOR, timeout=10000)
                page.wait_for_timeout(500)
            except Exception:
                logger.warning("[%s] 매물이 없거나 로딩되지 않았습니다.", car_type_name)
                continue

            expected_count = _get_kcar_expected_count(page)
            if expected_count:
                logger.info("차종 적용: %s (expected_count=%d)", car_type_name, expected_count)
            else:
                logger.info("차종 적용: %s (expected_count=unknown)", car_type_name)

            collected_this_type = 0
            type_seen_product_ids: set[str] = set()
            no_growth_rounds = 0

            boxes = page.locator(".resultCnt").first.locator(".carListWrap .carListBox")
            page_no = 1
            while True:
                count = boxes.count()
                logger.info("   ㄴ [%s] page=%d 데이터 추출 중: %d개 발견", car_type_name, page_no, count)
                wrote_this_page = 0

                with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
                    writer = csv.writer(f)
                    for i in range(count):
                        try:
                            box = boxes.nth(i)
                            if box.locator(".adBannerBox").count() > 0:
                                continue
                            box.scroll_into_view_if_needed()

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
                            if not product_id:
                                link_el = box.locator("a[href*='i_sCarCd']").first
                                if link_el.count() > 0:
                                    product_id = _extract_product_id_from_href(link_el.get_attribute("href") or "")

                            product_id_out = (
                                "EC" + product_id if product_id and not str(product_id).strip().upper().startswith("EC") else (product_id or "-")
                            )
                            if product_id_out != "-" and product_id_out in seen_product_ids:
                                continue
                            if product_id_out != "-":
                                # 차종별 집계/중복 스킵(정상이라면 type 간 중복은 없어야 함)
                                if product_id_out in type_seen_product_ids:
                                    continue
                                type_seen_product_ids.add(product_id_out)
                                seen_product_ids.add(product_id_out)

                            car_name = ""
                            name_el = box.locator(".detailInfo.srchTimedeal .carName .carTit").first
                            if name_el.count() == 0:
                                name_el = box.locator(".carName .carTit, .carTit").first
                            if name_el.count() > 0:
                                car_name = name_el.inner_text().strip()

                            car_exp = ""
                            exp_el = box.locator(".carListFlex .carExpIn .carExp").first
                            if exp_el.count() > 0:
                                car_exp = exp_el.inner_text().strip()

                            car_pay_meth = ""
                            pay_el = box.locator(".carPayMeth li .el-link .el-link--inner").first
                            if pay_el.count() > 0:
                                car_pay_meth = pay_el.inner_text().strip()

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

                            brand_list_val = car_list_val = model_list_val = model_list_1_val = model_list_2_val = "-"
                            if car_name and brand_lookup:
                                matched = brand_lookup.get(_normalize_key(car_name))
                                if matched:
                                    brand_list_val, car_list_val, model_list_val, model_list_1_val, model_list_2_val = matched

                            detail_url = DETAIL_URL_TEMPLATE.format(product_id_out) if product_id_out != "-" else "-"
                            car_imgs_val = "-"
                            if product_id_out != "-" and img_src:
                                list_img_path = list_save_dir / f"{product_id_out}_list.png"
                                if _download_list_image(page, img_src, list_img_path, logger):
                                    car_imgs_val = str(list_img_path)

                            if product_id_out != "-" or car_name:
                                model_sn += 1
                                writer.writerow(
                                    [
                                        model_sn,
                                        product_id_out,
                                        car_type_name,
                                        brand_list_val,
                                        car_list_val,
                                        model_list_val,
                                        model_list_1_val,
                                        model_list_2_val,
                                        car_name,
                                        car_exp,
                                        car_pay_meth,
                                        release_dt,
                                        car_navi,
                                        car_fuel,
                                        local_dos,
                                        detail_url,
                                        car_imgs_val,
                                        base_date_crtr_pnttm,
                                        base_create_dt,
                                    ]
                                )
                                wrote_this_page += 1
                                collected_this_type += 1
                        except Exception:
                            continue
                    f.flush()

                if wrote_this_page == 0:
                    no_growth_rounds += 1
                else:
                    no_growth_rounds = 0

                if expected_count and collected_this_type >= expected_count:
                    logger.info("   ㄴ [%s] expected_count=%d 도달(수집=%d) -> 차종 종료", car_type_name, expected_count, collected_this_type)
                    break

                previous_signature = _get_kcar_list_signature(page)
                if not _goto_next_kcar_page(page):
                    # 페이지 끝으로 판단했지만 expected_count가 남아있으면 약간 더 재시도
                    if expected_count and collected_this_type < expected_count and no_growth_rounds < 4:
                        try:
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        except Exception:
                            pass
                        page.wait_for_timeout(1200)
                        # pagination이 늦게 붙는 케이스 대비 1회 더 시도
                        if _goto_next_kcar_page(page):
                            _wait_kcar_list_refresh(page, previous_signature)
                            page.wait_for_timeout(800)
                            boxes = page.locator(".resultCnt").first.locator(".carListWrap .carListBox")
                            page_no += 1
                            continue
                    logger.info("   ㄴ [%s] 페이지 끝 도달 (수집=%d, expected=%d)", car_type_name, collected_this_type, expected_count or 0)
                    break
                _wait_kcar_list_refresh(page, previous_signature)
                page.wait_for_timeout(800)
                boxes = page.locator(".resultCnt").first.locator(".carListWrap .carListBox")
                page_no += 1

    except Exception as e:
        logger.error("전체 공정 중 치명적 오류: %s", e, exc_info=True)

    logger.info("최종 수집 완료. 총 %d건이 %s에 저장되었습니다.", model_sn, csv_path)


def _run_kcar_car_type_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    activate_paths_for_datst((datst_cd or KCAR_DATST_CAR_TYPE).lower() or KCAR_DATST_CAR_TYPE, kwargs=kwargs)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    csv_path = RESULT_DIR / f"kcar_car_type_list_{run_ts}.csv"
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE, prefer_chrome=USE_CHROME_CHANNEL)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        install_route_blocking(context)
        page = context.new_page()
        try:
            run_kcar_car_type_list(page, RESULT_DIR, logger, csv_path=csv_path)
        finally:
            browser.close()
    return str(csv_path)


def _run_kcar_brand_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    activate_paths_for_datst((datst_cd or KCAR_DATST_BRAND).lower() or KCAR_DATST_BRAND, kwargs=kwargs)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    csv_path = RESULT_DIR / f"kcar_brand_list_{run_ts}.csv"
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE, prefer_chrome=USE_CHROME_CHANNEL)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        install_route_blocking(context)
        page = context.new_page()
        try:
            run_kcar_brand_list(page, RESULT_DIR, logger, csv_path=csv_path)
        finally:
            browser.close()
    return str(csv_path)


def run_kcar_list_job(
    bsc: TnDataBscInfo,
    *,
    brand_list_csv_path: str,
    car_type_csv_path: str | None = None,
    kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    activate_paths_for_datst((bsc.datst_cd or KCAR_DATST_LIST).lower() or KCAR_DATST_LIST, kwargs=kwargs)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (IMG_BASE / "list").mkdir(parents=True, exist_ok=True)

    logger = _get_file_logger(run_ts)
    brand_path = Path(brand_list_csv_path)
    if not brand_path.is_file():
        raise FileNotFoundError(f"브랜드 CSV가 없습니다: {brand_path}")
    if car_type_csv_path and not Path(car_type_csv_path).is_file():
        logger.warning("차종 CSV 경로 없음(무시하고 진행): %s", car_type_csv_path)

    logger.info("🏁 K Car 목록 수집 시작")
    logger.info("- pvsn_site_cd=%s, datst_cd=%s, datst_nm=%s", bsc.pvsn_site_cd, bsc.datst_cd, bsc.datst_nm)
    logger.info("- link_url=%s", bsc.link_data_clct_url)
    logger.info("- 브랜드 CSV=%s", brand_path)

    list_path = RESULT_DIR / f"kcar_list_{run_ts}.csv"
    if list_path.exists():
        list_path.unlink()

    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE, prefer_chrome=USE_CHROME_CHANNEL)
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
                save_dir=IMG_BASE / "list",
            )
        finally:
            browser.close()

    count = 0
    if list_path.exists():
        with open(list_path, "r", encoding="utf-8-sig", newline="") as f:
            count = sum(1 for _ in csv.DictReader(f))

    logger.info("✅ K Car 목록 수집 완료: 총 %d건", count)
    return {
        "brand_csv": str(brand_path),
        "car_type_csv": car_type_csv_path or "",
        "list_csv": str(list_path),
        "count": count,
        "done": list_path.is_file() and count > 0,
        "log_dir": str(LOG_DIR),
        "img_base": str(IMG_BASE),
    }


dag_object = kcar_crawler_dag()


if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2026, 3, 24, 8, 0),
        conn_file_path=conn_path,
        run_conf={"dtst_cd": dtst_cd},
    )
