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
    dag_id="sdag_kiacar_crawler",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["used_car", "kiacar", "crawler", "day"],
)
def kiacar_crawler_dag():
    pg_hook = PostgresHook(postgres_conn_id="car_db_conn")

    @task
    def insert_collect_data_info(**kwargs) -> dict[str, dict[str, Any]]:
        select_bsc_info_stmt = f"""
        SELECT * FROM std.tn_data_bsc_info tdbi
        WHERE 1=1
          AND LOWER(clct_yn) = 'y'
          AND LOWER(link_yn) = 'y'
          AND LOWER(pvsn_site_cd) = '{KIACAR_PVSN_SITE_CD}'
          AND LOWER(datst_cd) IN ('{KIACAR_DATST_BRAND}','{KIACAR_DATST_LIST}')
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

        missing = [key for key in (KIACAR_DATST_BRAND, KIACAR_DATST_LIST) if key not in out]
        if missing:
            raise ValueError(f"std.tn_data_bsc_info 조회 결과 누락: {missing} (pvsn_site_cd={KIACAR_PVSN_SITE_CD})")
        return out

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        datst_cd = str((infos.get(KIACAR_DATST_BRAND) or {}).get("datst_cd") or KIACAR_DATST_BRAND).lower()
        return _run_kiacar_brand_csv(datst_cd, kwargs=kwargs)

    @task
    def run_list_csv(
        bsc_infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        **kwargs,
    ) -> dict[str, Any]:
        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[KIACAR_DATST_LIST])
        return run_kiacar_list_job(
            tn_data_bsc_info,
            brand_list_csv_path=brand_csv_path,
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
            KIACAR_COLLECT_DETAIL_TABLE,
            tn_data_bsc_info.datst_cd,
            csv_file_path,
        )
        registered_file_path = str(CommonUtil.build_collect_detail_file_path(tn_data_clct_dtl_info))
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
            KIACAR_COLLECT_DETAIL_TABLE,
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

        if datst_cd == KIACAR_DATST_LIST:
            rows = _dedupe_kiacar_list_rows(rows)
            target_table, _ = _resolve_list_table_targets(tn_data_bsc_info)

        if datst_cd in (KIACAR_DATST_BRAND, KIACAR_DATST_LIST):
            _delete_snapshot_rows(hook, target_table, rows)

        _bulk_insert_rows(hook, target_table, rows, truncate=False, allow_only_table_cols=True)
        table_count = CommonUtil.get_table_row_count(hook, target_table)
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
        list_load_result: dict[str, Any],
    ) -> dict[str, Any]:
        for load_result in (brand_load_result, list_load_result):
            if not load_result.get("done"):
                raise ValueError(f"CSV 적재 완료 조건 미충족: {load_result}")

        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[KIACAR_DATST_LIST])
        tmp_table, source_table = _resolve_list_table_targets(tn_data_bsc_info)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        current_rows = _read_csv_rows(Path(str(list_load_result.get("csv_path") or "")))
        sync_result = _sync_kiacar_tmp_to_source(
            hook,
            current_rows=current_rows,
            tmp_table=tmp_table,
            source_table=source_table,
        )
        logging.info(
            "기아차 list tmp->source 반영 완료: tmp_table=%s, source_table=%s, tmp_count=%d, current_row_count=%d, inserted_count=%d, marked_existing_count=%d, marked_missing_count=%d, source_count=%d",
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
        brand_path = run_brand_csv(bsc_infos)
        brand_collect_info = register_csv_collect_log_info.override(
            task_id="register_brand_collect_log_info"
        )(bsc_infos, KIACAR_DATST_BRAND, brand_path)

        list_result = run_list_csv(bsc_infos, brand_path)
        list_collect_info = register_csv_collect_log_info.override(
            task_id="register_list_collect_log_info"
        )(bsc_infos, KIACAR_DATST_LIST, list_result["list_csv"])

        return {
            KIACAR_DATST_BRAND: brand_collect_info,
            KIACAR_DATST_LIST: list_collect_info,
        }

    @task_group(group_id="insert_csv_process")
    def insert_csv_process(
        bsc_infos: dict[str, dict[str, Any]],
        tn_data_clct_dtl_info_map: dict[str, dict[str, Any]],
    ) -> None:
        brand_load_result = load_csv_to_ods.override(task_id="load_brand_csv_to_ods")(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            KIACAR_DATST_BRAND,
        )
        list_load_result = load_csv_to_ods.override(task_id="load_list_csv_to_ods")(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            KIACAR_DATST_LIST,
        )
        sync_list_tmp_to_source.override(task_id="sync_list_tmp_to_source")(
            bsc_infos,
            brand_load_result,
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

KIACAR_PVSN_SITE_CD = "ps00005"
KIACAR_DATST_BRAND = "data13"
KIACAR_DATST_LIST = "data15"
KIACAR_SITE_NAME = "기아차"
KIACAR_BRAND_TABLE = "ods.ods_brand_list_kiacar"
KIACAR_TMP_LIST_TABLE = "ods.ods_tmp_car_list_kiacar"
KIACAR_SOURCE_LIST_TABLE = "ods.ods_car_list_kiacar"
KIACAR_COLLECT_DETAIL_TABLE = "std.tn_data_clct_dtl_info"

URL = "https://cpo.kia.com/products/"
DETAIL_URL_TEMPLATE = "https://cpo.kia.com/products/detail/?id={}"
SELECTOR_BASE = (
    ".buy-carlist__left-side.show-pc .filter-info.js-select-scrollbar "
    ".filter-info__item:first-child .filter-info__content .css-hu289v"
)
SELECTOR_BASE_FALLBACK = ".filter-info__content .css-hu289v"
SELECTOR_CAR_LIST_ROOT = ".carcard-list"
SELECTOR_CAR_ITEM = f"{SELECTOR_CAR_LIST_ROOT} li.item-card.is-pc"
EXCLUDE_ITEM_CLASS = "css-1759cna"
HEADLESS_MODE = os.environ.get("KIACAR_HEADED", "0").strip().lower() not in ("1", "true", "yes")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

YEAR_STR = ""
DATE_STR = ""
RUN_TS = ""
RESULT_DIR = Path("/tmp")
LOG_DIR = Path("/tmp")
IMG_BASE = Path("/tmp")


def _norm(s: str) -> str:
    return " ".join((s or "").strip().split())


def _strip_dot(s: str) -> str:
    return _norm((s or "").replace("ㆍ", ""))


def _timestamps() -> tuple[str, str]:
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%Y%m%d%H%M")


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
    if (datst_cd or "").lower().strip() == KIACAR_DATST_BRAND:
        return normalized or KIACAR_BRAND_TABLE
    if (datst_cd or "").lower().strip() == KIACAR_DATST_LIST:
        return normalized or KIACAR_TMP_LIST_TABLE
    if normalized:
        return normalized
    raise ValueError(f"적재 대상 테이블을 확인할 수 없습니다: datst_cd={datst_cd}")


def _resolve_list_table_targets(tn_data_bsc_info: TnDataBscInfo) -> tuple[str, str]:
    tmp_table = _normalize_target_table(getattr(tn_data_bsc_info, "tmpr_tbl_phys_nm", None)) or KIACAR_TMP_LIST_TABLE
    source_table = _normalize_target_table(getattr(tn_data_bsc_info, "ods_tbl_phys_nm", None)) or KIACAR_SOURCE_LIST_TABLE
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
    return KIACAR_SITE_NAME


def activate_paths_for_datst(datst_cd: str, kwargs: dict[str, Any] | None = None) -> None:
    global RESULT_DIR, LOG_DIR, IMG_BASE, YEAR_STR, DATE_STR, RUN_TS

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
    IMG_BASE = CommonUtil.build_year_site_path(img_root, site, now)


try:
    activate_paths_for_datst(KIACAR_DATST_LIST)
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


def _sync_kiacar_tmp_to_source(
    hook: PostgresHook,
    *,
    current_rows: list[dict[str, Any]],
    tmp_table: str,
    source_table: str,
    key_cols: tuple[str, ...] = ("product_id", "detail_url"),
    flag_col: str = "register_flag",
) -> dict[str, int]:
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

    src_cols = _get_table_columns(hook, source_table)
    src_col_set = set(src_cols)
    src_has_flag = flag_col in src_col_set
    missing_key_cols = [col for col in key_cols if col not in src_col_set]
    if missing_key_cols:
        raise ValueError(f"source 테이블 키 컬럼 누락: table={source_table}, cols={missing_key_cols}")

    current_rows = _dedupe_kiacar_list_rows(current_rows)
    date_crtr_pnttm, create_dt = _pick_snapshot_audit_values(current_rows)
    order_cols = [col for col in ("create_dt", "date_crtr_pnttm") if col in src_col_set]
    update_key_col = key_cols[0]
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


def _get_file_logger(run_ts: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("kiacar_crawler")
    logger.setLevel(logging.INFO)
    log_path = str(LOG_DIR / f"kiacar_crawler_{run_ts}.log")
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == log_path:
            return logger
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    return logger


def _launch_browser(playwright, headless: bool):
    return playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--no-sandbox",
        ],
    )


def _get_name(locator) -> str:
    try:
        el = locator.locator(".item-txt__name").first
        if el.count() == 0:
            return ""
        return _norm(el.inner_text())
    except Exception:
        return ""


def _parse_product_id(href: str) -> str:
    if not href:
        return ""
    m = re.search(r"[?&]id=(\d+)", href)
    return (m.group(1) or "").strip() if m else ""


def _dismiss_popups_kiacar(page, logger) -> None:
    try:
        for _ in range(4):
            clicked = False
            for txt in ["오늘 그만 보기", "오늘만 보기", "닫기", "닫 기"]:
                try:
                    btn = page.locator(f"button:has-text('{txt}'), a:has-text('{txt}')").first
                    if btn.count() > 0 and btn.is_visible(timeout=800):
                        btn.click()
                        page.wait_for_timeout(800)
                        clicked = True
                        break
                except Exception:
                    continue
            if clicked:
                continue
            try:
                close_icon = page.locator("button[aria-label*='닫기'], button[aria-label*='close']").first
                if close_icon.count() > 0 and close_icon.is_visible(timeout=800):
                    close_icon.click()
                    page.wait_for_timeout(800)
                    clicked = True
            except Exception:
                pass
            if not clicked:
                break
    except Exception as e:
        logger.debug("팝업 닫기 중 오류: %s", e)


def _collect_brand_rows_in_page(page, base_selector: str, logger) -> list[dict[str, str]]:
    js = r"""
    (baseSel) => {
      const base = document.querySelector(baseSel) || document.querySelector('.css-hu289v');
      if (!base) return { ok: false, reason: 'base_not_found', rows: [] };
      const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
      const nameOf = (btn) => norm(btn?.querySelector?.('.item-txt__name')?.textContent || '');
      const clickAndMark = (btn) => {
        if (!btn) return false;
        try { btn.scrollIntoView({ block: 'center' }); } catch(e) {}
        try { btn.click(); } catch(e) {}
        return true;
      };
      const nextChild = (btn) => {
        const sib = btn?.nextElementSibling;
        if (sib && sib.classList && sib.classList.contains('child')) return sib;
        return null;
      };
      const rows = [];
      const depth1Btns = Array.from(base.querySelectorAll('.model-select__btn.depth1'));
      for (const d1 of depth1Btns) {
        clickAndMark(d1);
        const car_list = nameOf(d1);
        const c1 = nextChild(d1);
        if (!c1) {
          rows.push({ car_list, model_list: '', model_list_1: '', model_list_2: '' });
          continue;
        }
        const depth2Btns = Array.from(c1.querySelectorAll(':scope > .model-select__btn.depth2'));
        if (depth2Btns.length === 0) {
          rows.push({ car_list, model_list: '', model_list_1: '', model_list_2: '' });
          continue;
        }
        for (const d2 of depth2Btns) {
          clickAndMark(d2);
          const model_list = nameOf(d2);
          const c2 = nextChild(d2);
          if (!c2) {
            rows.push({ car_list, model_list, model_list_1: '', model_list_2: '' });
            continue;
          }
          const depth3Btns = Array.from(c2.querySelectorAll(':scope > .model-select__btn.depth3'));
          if (depth3Btns.length === 0) {
            rows.push({ car_list, model_list, model_list_1: '', model_list_2: '' });
            continue;
          }
          for (const d3 of depth3Btns) {
            clickAndMark(d3);
            const model_list_1 = nameOf(d3);
            const c3 = nextChild(d3);
            if (!c3) {
              rows.push({ car_list, model_list, model_list_1, model_list_2: '' });
              continue;
            }
            const depth4Btns = Array.from(c3.querySelectorAll(':scope > .model-select__btn.depth4'));
            if (depth4Btns.length === 0) {
              rows.push({ car_list, model_list, model_list_1, model_list_2: '' });
              continue;
            }
            for (const d4 of depth4Btns) {
              clickAndMark(d4);
              rows.push({
                car_list,
                model_list,
                model_list_1,
                model_list_2: nameOf(d4)
              });
            }
          }
        }
      }
      return { ok: true, rows };
    }
    """
    try:
        res = page.evaluate(js, base_selector)
    except Exception as e:
        logger.warning("페이지 내 트리 수집(evaluate) 실패: %s", e)
        return []
    if not res or not res.get("ok"):
        return []
    return res.get("rows") or []


def run_kiacar_brand_list(page, result_dir: Path, logger, csv_path: Path | None = None) -> None:
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_path or (result_dir / "kiacar_brand_list.csv")
    if csv_path.exists():
        csv_path.unlink()

    headers = [
        "model_sn",
        "brand_list",
        "car_type",
        "car_list",
        "model_list",
        "model_list_1",
        "model_list_2",
        "date_crtr_pnttm",
        "create_dt",
    ]

    try:
        if URL not in (page.url or ""):
            goto_with_retry(
                page,
                GotoSpec(
                    URL,
                    wait_until="commit",
                    timeout_ms=90_000,
                    ready_selectors=(SELECTOR_CONTAINER, SELECTOR_BASE, SELECTOR_BASE_FALLBACK),
                    ready_timeout_ms=20_000,
                ),
                logger=logger,
                attempts=3,
            )

        rows = _collect_brand_rows_in_page(page, SELECTOR_BASE, logger)
        if not rows:
            rows = _collect_brand_rows_in_page(page, SELECTOR_BASE_FALLBACK, logger)
        if not rows:
            logger.warning("기아 브랜드 트리 수집 결과가 0건입니다.")
            return

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for idx, row in enumerate(rows, start=1):
                date_crtr_pnttm, create_dt = _timestamps()
                w.writerow(
                    {
                        "model_sn": idx,
                        "brand_list": "기아",
                        "car_type": (row.get("car_list") or "").strip(),
                        "car_list": (row.get("model_list") or "").strip(),
                        "model_list": (row.get("model_list_1") or "").strip(),
                        "model_list_1": (row.get("model_list_2") or "").strip(),
                        "model_list_2": "",
                        "date_crtr_pnttm": date_crtr_pnttm,
                        "create_dt": create_dt,
                    }
                )
        logger.info("기아 브랜드 목록 저장 완료: %s (총 %d건)", csv_path, len(rows))
    except Exception as e:
        logger.error("기아 브랜드 목록 수집 오류: %s", e, exc_info=True)


def _extract_row_from_kiacar_card(li) -> dict[str, str]:
    row = {
        "product_id": "",
        "car_name_1": "",
        "car_name_2": "",
        "line_up": "",
        "release_dt": "",
        "car_navi": "",
        "price": "",
        "discount": "",
        "badge_discount": "",
        "reserve": "",
    }
    try:
        cls = li.get_attribute("class") or ""
        if "reserved" in cls and "css-ozhxoy" in cls:
            row["reserve"] = "예약중인 차량"
    except Exception:
        pass
    try:
        a = li.locator("a[rel='noopener noreferrer']").first
        if a.count() > 0:
            row["product_id"] = _parse_product_id(a.get_attribute("href") or "")
    except Exception:
        pass
    try:
        el = li.locator(".css-1laq3le > div .css-1lk1ywa").first
        if el.count() > 0:
            row["car_name_1"] = _norm(el.inner_text())
    except Exception:
        pass
    try:
        spans = li.locator(".css-1laq3le > div span")
        if spans.count() > 0:
            row["line_up"] = _norm(spans.nth(0).inner_text())
            if row["line_up"] and row["car_name_1"] and row["line_up"] in row["car_name_1"]:
                row["car_name_1"] = _norm(row["car_name_1"].replace(row["line_up"], "", 1))
    except Exception:
        pass
    try:
        el = li.locator(".css-jk1y7a").first
        if el.count() > 0:
            row["car_name_2"] = _norm(el.inner_text())
    except Exception:
        pass
    try:
        el = li.locator(".css-8lid69 .year").first
        if el.count() > 0:
            row["release_dt"] = _strip_dot(el.inner_text())
    except Exception:
        pass
    try:
        el = li.locator(".css-8lid69 .km").first
        if el.count() > 0:
            row["car_navi"] = _strip_dot(el.inner_text())
    except Exception:
        pass
    try:
        el = li.locator(".css-1sdsv7u .price").first
        if el.count() > 0:
            row["price"] = _norm(el.inner_text())
    except Exception:
        pass
    try:
        el = li.locator(".css-1sdsv7u .discount").first
        if el.count() > 0:
            row["discount"] = _norm(el.inner_text())
    except Exception:
        pass
    try:
        if li.locator("[class*='img-wrap__discount']").count() > 0:
            el = li.locator(".css-1u214re strong").first
            if el.count() > 0:
                row["badge_discount"] = _norm(el.inner_text())
    except Exception:
        pass
    return row


def _download_kiacar_card_first_img(page, li, save_dir: Path, file_stem: str, logger) -> str:
    save_dir.mkdir(parents=True, exist_ok=True)
    out_path = save_dir / f"{file_stem}.png"
    selectors = [
        "div.img-wrap__discount.css-kn97bq a[rel='noopener noreferrer'] img.item-card__img",
        "div.css-kn97bq a[rel='noopener noreferrer'] img.item-card__img",
        "div.img-wrap__discount.css-kn97bq a[rel='noopener noreferrer'] img",
        "div.css-kn97bq a[rel='noopener noreferrer'] img",
        "a[rel='noopener noreferrer'] img.item-card__img",
        "a[rel='noopener noreferrer'] img",
    ]

    img_src = ""
    for sel in selectors:
        try:
            img = li.locator(sel).first
            if img.count() == 0:
                continue
            img_src = (img.get_attribute("src") or "").strip() or (img.get_attribute("data-src") or "").strip()
            if img_src:
                break
        except Exception:
            continue
    if not img_src:
        return ""

    img_url = img_src if img_src.startswith("http") else urljoin(page.url or URL, img_src)
    try:
        resp = page.request.get(img_url, timeout=20000)
        if not resp or not resp.ok:
            return ""
        out_path.write_bytes(resp.body())
        return str(out_path.resolve())
    except Exception as e:
        logger.debug("기아 리스트 이미지 다운로드 실패: %s (%s)", img_url, e)
        return ""


def _load_kiacar_brand_lookup(brand_path: Path) -> dict[str, dict[str, str]]:
    key_to_brand: dict[str, dict[str, str]] = {}
    if not brand_path.exists():
        return key_to_brand
    with open(brand_path, "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            ml = (row.get("model_list") or "").strip()
            ml1 = (row.get("model_list_1") or "").strip()
            key = ml if (ml and ml[:1].isdigit()) else ml1
            if not key:
                key = ml or ml1
            if not key or key in key_to_brand:
                continue
            has_car_type = bool((row.get("car_type") or "").strip())
            key_to_brand[key] = {
                "brand_list": (row.get("brand_list") or "").strip() or "기아",
                "car_type": (row.get("car_type") or row.get("car_list") or "").strip(),
                "car_list": (
                    (row.get("car_list") or row.get("model_list") or "").strip()
                    if has_car_type
                    else (row.get("model_list") or "").strip()
                ),
                "model_list": (row.get("model_list") or row.get("model_list_1") or "").strip(),
            }
    return key_to_brand


def _kiacar_car_name_to_match_key(car_name_val: str) -> str:
    s = (car_name_val or "").strip()
    if not s:
        return ""
    parts = s.split()
    if len(parts) >= 2 and len(parts[0]) == 4 and parts[0].isdigit():
        return (parts[0] + " " + parts[1]).strip()
    return parts[0].strip() if parts else ""


def _kiacar_find_brand(match_key: str, key_to_brand: dict[str, dict[str, str]]) -> dict[str, str] | None:
    c = (match_key or "").strip()
    if not c or not key_to_brand:
        return None
    if c in key_to_brand:
        return key_to_brand[c]

    candidates = [k for k in key_to_brand if k.startswith(c) or c.startswith(k)]
    if candidates:
        return key_to_brand[max(candidates, key=len)]

    parts = c.split()
    year = parts[0] if (parts and len(parts[0]) == 4 and parts[0].isdigit()) else ""
    model = parts[1] if len(parts) >= 2 else (parts[0] if parts else "")
    if not model:
        return None
    candidates = [k for k in key_to_brand if (model in k or k in model)]
    if not candidates:
        return None
    with_year = [k for k in candidates if year and k.startswith(year)]
    pool = with_year if with_year else candidates
    return key_to_brand[max(pool, key=len)]


def run_kiacar_list(
    page,
    result_dir: Path,
    logger,
    csv_path: Path | None = None,
    brand_path: Path | None = None,
    save_dir: Path | None = None,
) -> None:
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_path or (result_dir / "kiacar_list.csv")
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
        "line_up",
        "release_dt",
        "car_navi",
        "price",
        "discount",
        "badge_discount",
        "reserve",
        "detail_url",
        "car_imgs",
        "date_crtr_pnttm",
        "create_dt",
    ]

    brand_map = _load_kiacar_brand_lookup(Path(brand_path) if brand_path else result_dir / "kiacar_brand_list.csv")
    list_save_dir = Path(save_dir) if save_dir is not None else (IMG_BASE / "list")
    list_save_dir.mkdir(parents=True, exist_ok=True)

    try:
        if URL not in (page.url or ""):
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3500)
        _dismiss_popups_kiacar(page, logger)
        page.wait_for_timeout(1000)
        page.wait_for_selector(SELECTOR_CAR_LIST_ROOT, timeout=20000)
        page.wait_for_timeout(800)

        prev_n = -1
        stable = 0
        for _ in range(260):
            items_loc = page.locator(SELECTOR_CAR_ITEM)
            n = items_loc.count()
            if n == prev_n:
                stable += 1
                if stable >= 4 and n > 0:
                    break
            else:
                stable = 0
            prev_n = n
            if n == 0:
                page.wait_for_timeout(800)
                continue
            try:
                items_loc.nth(n - 1).scroll_into_view_if_needed(timeout=3000)
            except Exception:
                pass
            page.wait_for_timeout(900)

        items_loc = page.locator(SELECTOR_CAR_ITEM)
        total_li = items_loc.count()
        logger.info("기아 리스트 최종 카드 수: %d", total_li)

        seen: set[str] = set()
        model_sn = 0
        progress_every = 100
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for idx in range(total_li):
                cur = idx + 1
                if idx == 0 or cur % progress_every == 0 or cur == total_li:
                    logger.info("기아 리스트 수집 진행: %d/%d (전체 카드 대비 스캔)", cur, total_li)
                li = items_loc.nth(idx)
                try:
                    cls = li.get_attribute("class") or ""
                    if EXCLUDE_ITEM_CLASS in cls:
                        continue
                except Exception:
                    continue

                row = _extract_row_from_kiacar_card(li)
                pid = (row.get("product_id") or "").strip()
                if not pid or pid in seen:
                    continue
                seen.add(pid)
                model_sn += 1

                date_crtr_pnttm, create_dt = _timestamps()
                car_name = ((row.get("car_name_1") or "").strip() + " " + (row.get("car_name_2") or "").strip()).strip()
                match_key = _kiacar_car_name_to_match_key(row.get("car_name_1") or car_name)
                matched = _kiacar_find_brand(match_key, brand_map) if match_key else None

                car_imgs = _download_kiacar_card_first_img(page, li, list_save_dir, f"{pid}_list", logger) or ""
                w.writerow(
                    {
                        "model_sn": model_sn,
                        "product_id": pid,
                        "car_type": (matched.get("car_type") if matched else "") or "",
                        "brand_list": (matched.get("brand_list") if matched else "") or "기아",
                        "car_list": (matched.get("car_list") if matched else "") or "",
                        "model_list": (matched.get("model_list") if matched else "") or "",
                        "car_name": car_name,
                        "line_up": row.get("line_up") or "",
                        "release_dt": row.get("release_dt") or "",
                        "car_navi": row.get("car_navi") or "",
                        "price": row.get("price") or "",
                        "discount": row.get("discount") or "",
                        "badge_discount": row.get("badge_discount") or "",
                        "reserve": row.get("reserve") or "",
                        "detail_url": DETAIL_URL_TEMPLATE.format(pid),
                        "car_imgs": car_imgs,
                        "date_crtr_pnttm": date_crtr_pnttm,
                        "create_dt": create_dt,
                    }
                )
        logger.info("기아 리스트 저장 완료: %s (총 %d건)", csv_path, model_sn)
    except Exception as e:
        logger.error("기아 리스트 수집 오류: %s", e, exc_info=True)


def _dedupe_kiacar_list_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        product_id = str(row.get("product_id") or "").strip()
        if product_id:
            if product_id in seen:
                continue
            seen.add(product_id)
        deduped.append(row)
    return deduped


def _run_kiacar_brand_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    activate_paths_for_datst((datst_cd or KIACAR_DATST_BRAND).lower() or KIACAR_DATST_BRAND, kwargs=kwargs)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    csv_path = RESULT_DIR / f"kiacar_brand_list_{run_ts}.csv"
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        context = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1280, "height": 800})
        install_route_blocking(context)
        page = context.new_page()
        try:
            run_kiacar_brand_list(page, RESULT_DIR, logger, csv_path=csv_path)
        finally:
            browser.close()
    return str(csv_path)


def run_kiacar_list_job(
    bsc: TnDataBscInfo,
    *,
    brand_list_csv_path: str,
    kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    activate_paths_for_datst((bsc.datst_cd or KIACAR_DATST_LIST).lower() or KIACAR_DATST_LIST, kwargs=kwargs)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (IMG_BASE / "list").mkdir(parents=True, exist_ok=True)

    logger = _get_file_logger(run_ts)
    brand_path = Path(brand_list_csv_path)
    if not brand_path.is_file():
        raise FileNotFoundError(f"브랜드 CSV가 없습니다: {brand_path}")

    list_path = RESULT_DIR / f"kiacar_list_{run_ts}.csv"
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        context = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1280, "height": 800})
        install_route_blocking(context)
        page = context.new_page()
        try:
            run_kiacar_list(
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

    return {
        "brand_csv": str(brand_path),
        "list_csv": str(list_path),
        "count": count,
        "done": list_path.is_file() and count > 0,
        "log_dir": str(LOG_DIR),
        "img_base": str(IMG_BASE),
    }


dag_object = kiacar_crawler_dag()


if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    dtst_cd = ""
    dag_object.test(
        execution_date=datetime(2026, 3, 24, 8, 0),
        conn_file_path=conn_path,
        run_conf={"dtst_cd": dtst_cd},
    )
