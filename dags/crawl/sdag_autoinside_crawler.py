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
import shutil
import sys
from datetime import datetime
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

from dto.tn_data_bsc_info import TnDataBscInfo
from util.common_util import CommonUtil


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
                    dto = CommonUtil.build_bsc_info_dto(raw)
                    k = str(dto.datst_cd or "").lower().strip()
                    if k and k not in out:
                        out[k] = CommonUtil.bsc_info_to_dict(dto)
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
    def run_car_type_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        dc = str((infos.get(AUTOINSIDE_DATST_CAR_TYPE) or {}).get("datst_cd") or AUTOINSIDE_DATST_CAR_TYPE).lower()
        return _run_autoinside_car_type_csv(dc, kwargs=kwargs)

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        dc = str((infos.get(AUTOINSIDE_DATST_BRAND) or {}).get("datst_cd") or AUTOINSIDE_DATST_BRAND).lower()
        return _run_autoinside_brand_csv(dc, kwargs=kwargs)

    @task
    def run_list_csv(
        bsc_infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        car_type_csv_path: str,
        **kwargs,
    ) -> dict[str, Any]:
        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[AUTOINSIDE_DATST_LIST])
        return run_autoinside_list_job(
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
            AUTOINSIDE_COLLECT_DETAIL_TABLE,
            tn_data_bsc_info.datst_cd,
            csv_file_path,
        )
        registered_file_path = str(CommonUtil.build_collect_detail_file_path(tn_data_clct_dtl_info))
        logging.info(
            "오토인사이드 수집 메타 등록: datst_cd=%s, file=%s, clct_pnttm=%s, status=%s",
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
            AUTOINSIDE_COLLECT_DETAIL_TABLE,
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
            "오토인사이드 최신 CSV 메타 선택: datst_cd=%s, selected=%s, clct_pnttm=%s, file_nm=%s",
            datst_cd,
            latest_csv_path,
            tn_data_clct_dtl_info.clct_pnttm or "",
            tn_data_clct_dtl_info.clct_data_file_nm or "",
        )
        rows = _read_csv_rows(latest_csv_path)
        if not rows:
            raise ValueError(f"적재할 CSV 데이터가 없습니다: datst_cd={datst_cd}, path={latest_csv_path}")
        if datst_cd == AUTOINSIDE_DATST_LIST:
            original_count = len(rows)
            rows = _dedupe_autoinside_list_rows(rows)
            if len(rows) != original_count:
                logging.info(
                    "오토인사이드 list 중복 제거: before=%d, after=%d, removed=%d",
                    original_count,
                    len(rows),
                    original_count - len(rows),
                )

        if datst_cd == AUTOINSIDE_DATST_LIST:
            target_table, _ = _resolve_list_table_targets(tn_data_bsc_info)

        if datst_cd in (AUTOINSIDE_DATST_BRAND, AUTOINSIDE_DATST_CAR_TYPE, AUTOINSIDE_DATST_LIST):
            _delete_snapshot_rows(hook, target_table, rows)

        should_truncate = False
        _bulk_insert_rows(hook, target_table, rows, truncate=should_truncate, allow_only_table_cols=True)
        table_count = CommonUtil.get_table_row_count(hook, target_table)
        logging.info(
            "오토인사이드 CSV 적재 완료: datst_cd=%s, table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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

        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[AUTOINSIDE_DATST_LIST])
        tmp_table, source_table = _resolve_list_table_targets(tn_data_bsc_info)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        current_rows = _read_csv_rows(Path(str(list_load_result.get("csv_path") or "")))
        sync_result = _sync_autoinside_tmp_to_source(
            hook,
            current_rows=current_rows,
            tmp_table=tmp_table,
            source_table=source_table,
        )
        logging.info(
            "오토인사이드 list tmp->source 반영 완료: tmp_table=%s, source_table=%s, tmp_count=%d, current_row_count=%d, inserted_count=%d, marked_existing_count=%d, marked_missing_count=%d, source_count=%d",
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
        car_type_path = run_car_type_csv(bsc_infos)
        car_type_collect_info = register_csv_collect_log_info.override(
            task_id="register_car_type_collect_log_info"
        )(bsc_infos, AUTOINSIDE_DATST_CAR_TYPE, car_type_path)

        brand_path = run_brand_csv(bsc_infos)
        brand_collect_info = register_csv_collect_log_info.override(
            task_id="register_brand_collect_log_info"
        )(bsc_infos, AUTOINSIDE_DATST_BRAND, brand_path)

        list_result = run_list_csv(bsc_infos, brand_path, car_type_path)
        list_collect_info = register_csv_collect_log_info.override(
            task_id="register_list_collect_log_info"
        )(bsc_infos, AUTOINSIDE_DATST_LIST, list_result["list_csv"])

        return {
            AUTOINSIDE_DATST_BRAND: brand_collect_info,
            AUTOINSIDE_DATST_CAR_TYPE: car_type_collect_info,
            AUTOINSIDE_DATST_LIST: list_collect_info,
        }

    @task_group(group_id="insert_csv_process")
    def insert_csv_process(
        bsc_infos: dict[str, dict[str, Any]],
        tn_data_clct_dtl_info_map: dict[str, dict[str, Any]],
    ) -> None:
        brand_load_result = load_csv_to_ods.override(task_id="load_brand_csv_to_ods")(bsc_infos, tn_data_clct_dtl_info_map, AUTOINSIDE_DATST_BRAND)
        car_type_load_result = load_csv_to_ods.override(task_id="load_car_type_csv_to_ods")(bsc_infos, tn_data_clct_dtl_info_map, AUTOINSIDE_DATST_CAR_TYPE)
        list_load_result = load_csv_to_ods.override(task_id="load_list_csv_to_ods")(bsc_infos, tn_data_clct_dtl_info_map, AUTOINSIDE_DATST_LIST)
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
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
COLLECT_LOG_FILE_PATH_VAR = "used_car_collect_log_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"

# DB `std.tn_data_bsc_info`: 오토인사이드 제공사이트 코드
AUTOINSIDE_PVSN_SITE_CD = "ps00001"
AUTOINSIDE_DATST_BRAND = "data1"
AUTOINSIDE_DATST_CAR_TYPE = "data2"
AUTOINSIDE_DATST_LIST = "data3"
AUTOINSIDE_SITE_NAME = "오토인사이드"
AUTOINSIDE_BRAND_TABLE = "ods.ods_brand_list_autoinside"
AUTOINSIDE_CAR_TYPE_TABLE = "ods.ods_car_type_list_autoinside"
AUTOINSIDE_TMP_LIST_TABLE = "ods.ods_tmp_car_list_autoinside"
AUTOINSIDE_SOURCE_LIST_TABLE = "ods.ods_car_list_autoinside"
AUTOINSIDE_COLLECT_DETAIL_TABLE = "std.tn_data_clct_dtl_info"


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
    key = (datst_cd or "").lower().strip()
    mapping: dict[str, Any] = {}
    raw = _get_context_var_json(kwargs, USED_CAR_SITE_NAMES_VAR)
    try:
        if raw is None:
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


def get_autoinside_site_name(kwargs: dict[str, Any] | None = None) -> str:
    for datst_cd in (
        AUTOINSIDE_DATST_LIST,
        AUTOINSIDE_DATST_BRAND,
        AUTOINSIDE_DATST_CAR_TYPE,
    ):
        site_name = get_site_name_by_datst(datst_cd, kwargs=kwargs)
        if site_name and site_name.strip():
            return site_name
    return AUTOINSIDE_SITE_NAME


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
        return parts[0] + "." + parts[1]
    return s


def _resolve_target_table_for_datst(datst_cd: str, raw: str | None) -> str:
    normalized = _normalize_target_table(raw)
    key = (datst_cd or "").lower().strip()

    if key == AUTOINSIDE_DATST_BRAND:
        return normalized or AUTOINSIDE_BRAND_TABLE
    if key == AUTOINSIDE_DATST_CAR_TYPE:
        return normalized or AUTOINSIDE_CAR_TYPE_TABLE
    if key == AUTOINSIDE_DATST_LIST:
        return normalized or AUTOINSIDE_TMP_LIST_TABLE
    if normalized:
        return normalized
    raise ValueError(f"적재 대상 테이블을 확인할 수 없습니다: datst_cd={datst_cd}")


def _resolve_list_table_targets(tn_data_bsc_info: TnDataBscInfo) -> tuple[str, str]:
    tmp_table = _normalize_target_table(getattr(tn_data_bsc_info, "tmpr_tbl_phys_nm", None)) or AUTOINSIDE_TMP_LIST_TABLE
    source_table = _normalize_target_table(getattr(tn_data_bsc_info, "ods_tbl_phys_nm", None)) or AUTOINSIDE_SOURCE_LIST_TABLE
    return tmp_table, source_table


def _dedupe_autoinside_list_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def _sync_autoinside_tmp_to_source(
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
        return {"tmp_count": tmp_count, "source_count": CommonUtil.get_table_row_count(hook, source_table)}

    src_cols = _get_table_columns(hook, source_table)
    src_col_set = set(src_cols)
    src_has_flag = flag_col in src_col_set
    missing_key_cols = [col for col in key_cols if col not in src_col_set]
    if missing_key_cols:
        raise ValueError(f"source 테이블 키 컬럼 누락: table={source_table}, cols={missing_key_cols}")

    current_rows = _dedupe_autoinside_list_rows(current_rows)
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


def activate_paths_for_datst(datst_cd: str, kwargs: dict[str, Any] | None = None) -> None:
    global RESULT_DIR, LOG_DIR, IMG_BASE, YEAR_STR, DATE_STR, RUN_TS

    result_root = _get_result_root_path(kwargs=kwargs)
    log_root = _get_log_root_path(kwargs=kwargs)
    img_root = _get_img_root_path(kwargs=kwargs)
    site = get_autoinside_site_name(kwargs=kwargs)
    now = datetime.now()
    YEAR_STR = now.strftime("%Y년")
    DATE_STR = now.strftime("%Y%m%d")
    RUN_TS = now.strftime("%Y%m%d%H%M")

    RESULT_DIR = CommonUtil.build_dated_site_path(result_root, site, now)
    LOG_DIR = CommonUtil.build_dated_site_path(log_root, site, now)
    IMG_BASE = _build_year_site_path(img_root, site, now)


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
    return f"imgs/{now.strftime('%Y년')}/{AUTOINSIDE_SITE_NAME}/list"


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
    seen_product_id_to_type: dict[str, str] = {}

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
                    btn = page.get_by_text(car_type_name, exact=True).first
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

                    if product_id and product_id in seen_product_id_to_type:
                        logger.info(
                            "[차종] 중복 product_id 스킵: current=%s, previous=%s, product_id=%s",
                            car_type_name,
                            seen_product_id_to_type[product_id],
                            product_id,
                        )
                        continue

                    if product_id:
                        seen_product_id_to_type[product_id] = car_type_name
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


def _run_autoinside_car_type_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    activate_paths_for_datst((datst_cd or AUTOINSIDE_DATST_CAR_TYPE).lower() or AUTOINSIDE_DATST_CAR_TYPE, kwargs=kwargs)
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


def _run_autoinside_brand_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    activate_paths_for_datst((datst_cd or AUTOINSIDE_DATST_BRAND).lower() or AUTOINSIDE_DATST_BRAND, kwargs=kwargs)
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
    bsc: TnDataBscInfo,
    *,
    brand_list_csv_path: str,
    car_type_csv_path: str | None = None,
    kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    activate_paths_for_datst((bsc.datst_cd or AUTOINSIDE_DATST_LIST).lower() or AUTOINSIDE_DATST_LIST, kwargs=kwargs)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    list_img_dir = IMG_BASE / "list"
    list_img_dir.mkdir(parents=True, exist_ok=True)
    _clear_directory_contents(list_img_dir)

    logger = _get_file_logger(run_ts)
    brand_path = Path(brand_list_csv_path)
    if not brand_path.is_file():
        raise FileNotFoundError(
            f"브랜드 CSV가 없습니다. run_brand_csv 태스크를 먼저 성공시키고 brand_list_csv_path를 확인하세요: {brand_path}"
        )
    if car_type_csv_path and not Path(car_type_csv_path).is_file():
        logger.warning("차종 CSV 경로 없음(무시하고 진행): %s", car_type_csv_path)

    logger.info("🏁 오토인사이드 목록 수집 시작")
    logger.info("- pvsn_site_cd=%s, datst_cd=%s, datst_nm=%s", bsc.pvsn_site_cd, bsc.datst_cd, bsc.datst_nm)
    logger.info("- link_url=%s", bsc.link_data_clct_url)
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
                img_dir=list_img_dir,
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
        "done": list_path.is_file() and count > 0,
        "log_dir": str(LOG_DIR),
        "img_base": str(IMG_BASE),
    }


dag_object = autoinside_crawler_dag()


# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2025, 10, 10, 8, 0),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )
