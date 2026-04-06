# -*- coding: utf-8 -*-
"""
리본카 스마트바이 목록 수집 → ODS·tmp→원천 동기화 후 sdag_reborncar_detail_crawl 트리거.
목록 썸네일: {연도}년/리본카/list/{product_id}/{product_id}_list.png (Variable used_car_image_file_path).
"""
import csv
import logging
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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from dto.tn_data_bsc_info import TnDataBscInfo
from util.common_util import CommonUtil

# Airflow Variable
USED_CAR_SITE_NAMES_VAR = "used_car_site_names"
CRAWL_BASE_PATH_VAR = "crawl_base_path"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
COLLECT_LOG_FILE_PATH_VAR = "used_car_collect_log_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"

REBORNCAR_PVSN_SITE_CD = "ps00007"
REBORNCAR_DATST_BRAND = "data19"
REBORNCAR_DATST_CAR_TYPE = "data20"
REBORNCAR_DATST_LIST = "data21"
REBORNCAR_SITE_NAME = "리본카"
REBORNCAR_BRAND_TABLE = "ods.ods_brand_list_reborncar"
REBORNCAR_CAR_TYPE_TABLE = "ods.ods_car_type_list_reborncar"
REBORNCAR_TMP_LIST_TABLE = "ods.ods_tmp_car_list_reborncar"
REBORNCAR_SOURCE_LIST_TABLE = "ods.ods_car_list_reborncar"
REBORNCAR_COLLECT_DETAIL_TABLE = "std.tn_data_clct_dtl_info"


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


def get_site_name_by_datst(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    """Variable USED_CAR_SITE_NAMES(JSON)에서 datst_cd에 해당하는 한글 폴더명 조회."""
    key = (datst_cd or "").lower().strip()
    mapping: dict[str, Any] = {}
    raw_mapping = _get_context_var_json(kwargs, USED_CAR_SITE_NAMES_VAR)
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
    return REBORNCAR_SITE_NAME


def activate_paths_for_datst(datst_cd: str, kwargs: dict[str, Any] | None = None) -> None:
    """datst_cd 기준 crawl / log / img 루트 경로 설정 (각 Task 시작 시 호출)."""
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


def _reborncar_list_image_relpath(product_id: str) -> str:
    """Variable 이미지 루트 기준: {연도}년/리본카/list/{product_id}/{product_id}_list.png."""
    site = get_site_name_by_datst(REBORNCAR_DATST_LIST)
    return f"{YEAR_STR}/{site}/list/{product_id}/{product_id}_list.png"


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
    if key == REBORNCAR_DATST_BRAND:
        return normalized or REBORNCAR_BRAND_TABLE
    if key == REBORNCAR_DATST_CAR_TYPE:
        return normalized or REBORNCAR_CAR_TYPE_TABLE
    if key == REBORNCAR_DATST_LIST:
        return normalized or REBORNCAR_TMP_LIST_TABLE
    if normalized:
        return normalized
    raise ValueError(f"적재 대상 테이블을 확인할 수 없습니다: datst_cd={datst_cd}")


def _resolve_list_table_targets(tn_data_bsc_info: TnDataBscInfo) -> tuple[str, str]:
    tmp_table = _normalize_target_table(getattr(tn_data_bsc_info, "tmpr_tbl_phys_nm", None)) or REBORNCAR_TMP_LIST_TABLE
    source_table = _normalize_target_table(getattr(tn_data_bsc_info, "ods_tbl_phys_nm", None)) or REBORNCAR_SOURCE_LIST_TABLE
    return tmp_table, source_table


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


def _delete_snapshot_rows(
    hook: PostgresHook,
    full_table_name: str,
    rows: list[dict[str, Any]],
) -> None:
    """
    스냅샷 기준 컬럼(create_dt/date_crtr_pnttm)이 있으면
    해당 일자의 기존 데이터를 먼저 삭제하고 적재한다.
    """
    if not rows:
        return

    table_cols = set(_get_table_columns(hook, full_table_name))
    create_dt_values = sorted(
        {str(row.get("create_dt") or "").strip() for row in rows if str(row.get("create_dt") or "").strip()}
    )
    date_values = sorted(
        {str(row.get("date_crtr_pnttm") or "").strip() for row in rows if str(row.get("date_crtr_pnttm") or "").strip()}
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


def _dedupe_reborncar_list_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def _sync_reborncar_tmp_to_source(
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
                            SELECT 1 FROM {source_table} AS src
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
                        SELECT 1 FROM {tmp_table} tmp
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

def _normalize_compact_text(text: str) -> str:
    return re.sub(r"\s+", "", (text or "").strip()).lower()


def _build_auto_model_from_name(
    lp_car_name: str,
    lp_car_trim: str,
    row_5: tuple[str, str, str, str, str],
) -> tuple[str, str, str, str, str]:
    brand, car, model, model_1, model_2 = row_5
    full = _normalize_composite_key(lp_car_name, lp_car_trim)
    compact_full = _normalize_compact_text(full)
    compact_model = _normalize_compact_text(model)
    if compact_model and compact_model in compact_full:
        idx = compact_full.find(compact_model)
        rest = full[max(0, idx + len(model)) :].strip() if idx >= 0 else ""
        parts = rest.split()
        auto_model_1 = model_1 or (parts[0] if parts else "")
        auto_model_2 = model_2 or (parts[1] if len(parts) > 1 else "")
        return brand, car, model, auto_model_1, auto_model_2
    return row_5


def _get_model_key_for_lp_car_name(lp_car_name, model_keys):
    """lp_car_name으로 model_keys 중 매칭되는 키 반환. 없으면 None.
    - 전체/마지막 단어 매칭
    - 추가: car_name에 model_key가 포함된 경우 (가장 긴 매칭 우선)
    """
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
    # 포함 매칭: car_name에 model_key가 단어 단위로 포함된 경우
    name_words = set(name.split())
    for mk in sorted(model_keys, key=len, reverse=True):
        if not mk:
            continue
        if mk in name_words or (mk in name and name.strip().startswith(mk)):
            return mk
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

    # 보완: model_list가 car_name에 포함된 경우 (띄어쓰기 단위, 예: "캐스퍼" in "캐스퍼 가솔린 1.0")
    for model_list_key in sorted(model_list_to_row.keys(), key=len, reverse=True):
        if not model_list_key:
            continue
        if (
            model_list_key in car_name_full.split()
            or car_name_full.startswith(model_list_key + " ")
            or car_name_no_first.startswith(model_list_key + " ")
        ):
            return _build_auto_model_from_name(name, trim, model_list_to_row[model_list_key])

    compact_full = _normalize_compact_text(car_name_full)
    compact_no_first = _normalize_compact_text(car_name_no_first)
    for model_list_key in sorted(model_list_to_row.keys(), key=len, reverse=True):
        compact_model = _normalize_compact_text(model_list_key)
        if not compact_model:
            continue
        if compact_model in compact_full or compact_model in compact_no_first:
            return _build_auto_model_from_name(name, trim, model_list_to_row[model_list_key])

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


def _safe_click(page, locator, logger, *, timeout: int = 15000, desc: str = "") -> bool:
    """클릭 재시도 2번 + force fallback (오버레이/스크롤 등으로 타임아웃 시 사용)."""
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            locator.click(timeout=timeout)
            return True
        except Exception as e:
            if attempt < max_retries:
                logger.warning(f"클릭 재시도 ({attempt + 1}/{max_retries}) {desc}: {e}")
                page.wait_for_timeout(500 * (attempt + 1))
            else:
                try:
                    locator.click(force=True, timeout=5000)
                    return True
                except Exception as e2:
                    logger.warning(f"force 클릭 실패 {desc}: {e2}")
                    return False
    return False


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
        page.wait_for_timeout(700)
        brand_selectors = page.locator(".filter-brand .brand-list")
        brand_count = brand_selectors.count()

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            for i in range(brand_count):
                brand_box = brand_selectors.nth(i)
                brand_list = brand_box.locator(".brand-name label span").inner_text().strip()
                logger.info(f"[{brand_list}] 처리 중...")
                if not _safe_click(page, brand_box.locator(".brand-name label"), logger, desc="brand"):
                    continue
                page.wait_for_timeout(150)

                car_items = brand_box.locator(".car-list .check-box[class*='car-']")
                car_count = car_items.count()

                for j in range(car_count):
                    car_box = car_items.nth(j)
                    car_list = car_box.locator("label span").first.inner_text().strip()
                    if not _safe_click(page, car_box.locator("label").first, logger, desc=f"car {car_list}"):
                        continue
                    page.wait_for_timeout(120)

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
                            if not _safe_click(page, model_box.locator("label").first, logger, desc=f"model {model_list_val}", timeout=20000):
                                continue
                            page.wait_for_timeout(80)

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
                                if not _safe_click(page, trim_el.locator("label"), logger, desc=f"trim {trim_name}", timeout=20000):
                                    continue
                                page.wait_for_timeout(100)
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
    """목록 .lp-thumnail > img → list/{product_id}/{product_id}_list.png. 상대경로(imgs 기준) 반환."""
    if not product_id or not save_dir:
        return ""
    save_dir = Path(save_dir)
    product_dir = save_dir / product_id
    product_dir.mkdir(parents=True, exist_ok=True)
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
            path = product_dir / f"{product_id}_list.png"
            path.write_bytes(resp.body())
            return _reborncar_list_image_relpath(product_id)
    except Exception as e:
        logger.warning(f"리스트 썸네일 저장 실패 ({product_id}_list): {e}")
    return ""


def save_detail_images(page, product_id, save_dir, detail_url, logger):
    """상세 vip-visual 이미지를 detail/{product_id}/{product_id}_1.png … 로 저장."""
    if not product_id or not save_dir:
        return
    save_dir = Path(save_dir)
    product_dir = save_dir / product_id
    product_dir.mkdir(parents=True, exist_ok=True)
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
    # 3) fallback: vip-visual 내 모든 img
    if not urls:
        fallback_imgs = page.locator("#wrap .vip-section .vip-visual img[src]")
        for i in range(fallback_imgs.count()):
            src = fallback_imgs.nth(i).get_attribute("src")
            if src and src not in urls:
                urls.append(src)
    saved_count = 0
    for idx, src in enumerate(urls, start=1):
        try:
            full_url = urljoin(base_url, src) if not (src.startswith("http") or src.startswith("//")) else ("https:" + src if src.startswith("//") else src)
            resp = page.request.get(full_url)
            if resp.ok:
                path = product_dir / f"{product_id}_{idx}.png"
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
        page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_selector(".vip-section .vip-visual", state="attached", timeout=15000)
        page.wait_for_timeout(1500)  # lazy 로드 이미지 대기
        save_detail_images(page, product_id, img_save_dir, detail_url, logger)
    except Exception as e:
        logger.warning(f"이미지 저장 스킵 ({product_id}): {e}")


def _run_reborncar_brand_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    from playwright.sync_api import sync_playwright

    activate_paths_for_datst((datst_cd or REBORNCAR_DATST_BRAND).lower() or REBORNCAR_DATST_BRAND, kwargs=kwargs)
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


def _run_reborncar_car_type_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    from playwright.sync_api import sync_playwright

    activate_paths_for_datst((datst_cd or REBORNCAR_DATST_CAR_TYPE).lower() or REBORNCAR_DATST_CAR_TYPE, kwargs=kwargs)
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
    bsc: TnDataBscInfo,
    *,
    brand_list_csv_path: str,
    car_type_csv_path: str | None = None,
    kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    DB 메타 1건(목록 datst) 기준 리본카 목록·이미지 수집.
    브랜드 CSV는 앞단 Task에서 생성한 절대경로를 사용한다 (RUN_TS 불일치 방지).
    """
    from playwright.sync_api import sync_playwright

    activate_paths_for_datst((bsc.datst_cd or REBORNCAR_DATST_LIST).lower() or REBORNCAR_DATST_LIST, kwargs=kwargs)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (IMG_BASE / "list").mkdir(parents=True, exist_ok=True)

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
    logger.info(f"- pvsn_site_cd={bsc.pvsn_site_cd}, datst_cd={bsc.datst_cd}, datst_nm={bsc.datst_nm}")
    logger.info(f"- link_url={bsc.link_data_clct_url}")
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

    TEST_PAGE_LIMIT = None
    list_url = (bsc.link_data_clct_url or "").strip() or "https://www.reborncar.co.kr/smartbuy/SB1001.rb"
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
        page.add_init_script(
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
                        page.wait_for_timeout(1200)
                        page.wait_for_selector('.lp-filter-list .lp-filter-choice span[data-cls="cate-cb"]', timeout=8000)
                        page.wait_for_timeout(300)
                        logger.info(f"차종 필터 선택 (칩 생성): {current_car_type}")
                    except Exception as e:
                        logger.warning(f"차종 필터 클릭 실패 ({current_car_type}): {e}")
                        continue

                while True:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(1200)

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
                                            pdir = list_img_save_dir / v_product_id
                                            pdir.mkdir(parents=True, exist_ok=True)
                                            path = pdir / f"{v_product_id}_list.png"
                                            path.write_bytes(resp.body())
                                            car_imgs_val = _reborncar_list_image_relpath(
                                                v_product_id
                                            )

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
                        page.wait_for_timeout(1200)
                    else:
                        next_grp = page.locator("li.pagination-con.next:not(.disabled) a").first
                        if next_grp.count() > 0:
                            next_grp.evaluate("el => el.click()")
                            page.wait_for_timeout(1500)
                            page.wait_for_selector("li.pagination-con.page-num.active", timeout=8000)
                        else:
                            break
                    page.wait_for_timeout(500)

                    try:
                        new_active = page.locator("li.pagination-con.page-num.active")
                        new_page = int(new_active.inner_text() or "0")
                        if new_page == prev_page:
                            page.wait_for_timeout(500)
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
                            page.wait_for_timeout(700)
                            logger.info(f"차종 필터 칩 제거: {current_car_type}")
                    except Exception as e:
                        logger.warning(f"차종 칩 제거 실패 ({current_car_type}): {e}")
        finally:
            browser.close()

    total_count = car_counter - 1
    logger.info(f"✅ 리본카 목록 수집 완료: 총 {total_count:,}건")
    return {
        "brand_csv": str(brand_path),
        "car_type_csv": car_type_csv_path or "",
        "list_csv": str(list_path),
        "count": total_count,
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

    - DB 메타(std.tn_data_bsc_info, ps00007, data19/20/21) 조회
    - 브랜드 CSV → 차종 CSV → 목록/이미지 순, 태스크별 파일 생성
    """

    @task
    def insert_collect_data_info(**kwargs) -> dict[str, dict[str, Any]]:
        
        hook = PostgresHook(postgres_conn_id="car_db_conn")

        site = REBORNCAR_PVSN_SITE_CD.lower().strip()
        sql = f"""
        SELECT * FROM std.tn_data_bsc_info
        WHERE 1=1
          AND LOWER(clct_yn) = 'y'
          AND LOWER(link_yn) = 'y'
          AND LOWER(pvsn_site_cd) = '{site}'
          AND LOWER(datst_cd) IN ('{REBORNCAR_DATST_BRAND}','{REBORNCAR_DATST_CAR_TYPE}','{REBORNCAR_DATST_LIST}')
        ORDER BY data_sn
        """
        logging.info("select_bsc_info_stmt ::: %s", sql)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
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
            k for k in (
                REBORNCAR_DATST_BRAND,
                REBORNCAR_DATST_CAR_TYPE,
                REBORNCAR_DATST_LIST,
            ) if k not in out
        ]
        if missing:
            raise ValueError(
                f"std.tn_data_bsc_info 조회 결과 누락: {missing} (pvsn_site_cd={REBORNCAR_PVSN_SITE_CD})"
            )
        return out

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        dc = str((infos.get(REBORNCAR_DATST_BRAND) or {}).get("datst_cd") or REBORNCAR_DATST_BRAND).lower()
        return _run_reborncar_brand_csv(dc, kwargs=kwargs)

    @task
    def run_car_type_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        dc = str((infos.get(REBORNCAR_DATST_CAR_TYPE) or {}).get("datst_cd") or REBORNCAR_DATST_CAR_TYPE).lower()
        return _run_reborncar_car_type_csv(dc, kwargs=kwargs)

    @task
    def run_list_csv(
        bsc_infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        car_type_csv_path: str,
        **kwargs,
    ) -> dict[str, Any]:
        b_list = CommonUtil.build_bsc_info_dto(bsc_infos[REBORNCAR_DATST_LIST])
        return run_reborncar_list_job(
            b_list,
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
            REBORNCAR_COLLECT_DETAIL_TABLE,
            tn_data_bsc_info.datst_cd,
            csv_file_path,
        )
        registered_file_path = str(CommonUtil.build_collect_detail_file_path(tn_data_clct_dtl_info))
        logging.info(
            "리본카 수집 메타 등록: datst_cd=%s, file=%s, clct_pnttm=%s, status=%s",
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
            REBORNCAR_COLLECT_DETAIL_TABLE,
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
            "리본카 최신 CSV 메타 선택: datst_cd=%s, selected=%s, clct_pnttm=%s, file_nm=%s",
            datst_cd,
            latest_csv_path,
            tn_data_clct_dtl_info.clct_pnttm or "",
            tn_data_clct_dtl_info.clct_data_file_nm or "",
        )
        rows = _read_csv_rows(latest_csv_path)
        if not rows:
            raise ValueError(f"적재할 CSV 데이터가 없습니다: datst_cd={datst_cd}, path={latest_csv_path}")

        if datst_cd == REBORNCAR_DATST_LIST:
            original_count = len(rows)
            rows = _dedupe_reborncar_list_rows(rows)
            if len(rows) != original_count:
                logging.info(
                    "리본카 list 중복 제거: before=%d, after=%d, removed=%d",
                    original_count,
                    len(rows),
                    original_count - len(rows),
                )
            target_table, _ = _resolve_list_table_targets(tn_data_bsc_info)

        if datst_cd in (REBORNCAR_DATST_BRAND, REBORNCAR_DATST_CAR_TYPE, REBORNCAR_DATST_LIST):
            _delete_snapshot_rows(hook, target_table, rows)

        should_truncate = False
        _bulk_insert_rows(hook, target_table, rows, truncate=should_truncate, allow_only_table_cols=True)
        table_count = CommonUtil.get_table_row_count(hook, target_table)
        logging.info(
            "리본카 CSV 적재 완료: datst_cd=%s, table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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

        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[REBORNCAR_DATST_LIST])
        tmp_table, source_table = _resolve_list_table_targets(tn_data_bsc_info)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        sync_result = _sync_reborncar_tmp_to_source(
            hook,
            tmp_table=tmp_table,
            source_table=source_table,
        )
        logging.info(
            "리본카 list tmp->source 반영 완료: tmp_table=%s, source_table=%s, tmp_count=%d, source_count=%d",
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
        brand_path = run_brand_csv(bsc_infos)
        brand_collect_info = register_csv_collect_log_info.override(
            task_id="register_brand_collect_log_info"
        )(bsc_infos, REBORNCAR_DATST_BRAND, brand_path)

        car_type_path = run_car_type_csv(bsc_infos)
        car_type_collect_info = register_csv_collect_log_info.override(
            task_id="register_car_type_collect_log_info"
        )(bsc_infos, REBORNCAR_DATST_CAR_TYPE, car_type_path)

        list_result = run_list_csv(bsc_infos, brand_path, car_type_path)
        list_collect_info = register_csv_collect_log_info.override(
            task_id="register_list_collect_log_info"
        )(bsc_infos, REBORNCAR_DATST_LIST, list_result["list_csv"])

        return {
            REBORNCAR_DATST_BRAND: brand_collect_info,
            REBORNCAR_DATST_CAR_TYPE: car_type_collect_info,
            REBORNCAR_DATST_LIST: list_collect_info,
        }

    @task_group(group_id="insert_csv_process")
    def insert_csv_process(
        bsc_infos: dict[str, dict[str, Any]],
        tn_data_clct_dtl_info_map: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        brand_load_result = load_csv_to_ods.override(task_id="load_brand_csv_to_ods")(bsc_infos, tn_data_clct_dtl_info_map, REBORNCAR_DATST_BRAND)
        car_type_load_result = load_csv_to_ods.override(task_id="load_car_type_csv_to_ods")(bsc_infos, tn_data_clct_dtl_info_map, REBORNCAR_DATST_CAR_TYPE)
        list_load_result = load_csv_to_ods.override(task_id="load_list_csv_to_ods")(bsc_infos, tn_data_clct_dtl_info_map, REBORNCAR_DATST_LIST)
        sync_result = sync_list_tmp_to_source.override(task_id="sync_list_tmp_to_source")(
            bsc_infos,
            brand_load_result,
            car_type_load_result,
            list_load_result,
        )
        return sync_result

    infos = insert_collect_data_info()
    tn_data_clct_dtl_info_map = create_csv_process(infos)
    insert_csv_done = insert_csv_process(infos, tn_data_clct_dtl_info_map)

    trigger_reborncar_detail_crawl = TriggerDagRunOperator(
        task_id="trigger_reborncar_detail_crawl",
        trigger_dag_id="sdag_reborncar_detail_crawl",
        wait_for_completion=False,
        reset_dag_run=True,
        conf={
            "source_dag_id": "sdag_reborncar_crawler",
            "source_run_id": "{{ run_id }}",
            "source_logical_date": "{{ ds }}",
        },
    )
    insert_csv_done >> trigger_reborncar_detail_crawl


reborncar_dag = reborncar_crawler_dag()