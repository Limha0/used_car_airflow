# -*- coding: utf-8 -*-
"""
현대 인증중고차 목록 페이지에서 브랜드/차종/목록 수집.

Airflow DAG:
- DB 메타(std.tn_data_bsc_info, ps00003, data7/8/9) 조회
- 차종 CSV -> 브랜드 CSV -> 목록 CSV 생성
- 수집 메타(std.tn_data_clct_dtl_info) 등록 후 ODS 적재
- list는 임시 테이블 적재 후 원천 테이블과 동기화
"""
import csv
import logging
import os
import re
import shutil
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


USED_CAR_SITE_NAMES_VAR = "used_car_site_names"
CRAWL_BASE_PATH_VAR = "crawl_base_path"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
COLLECT_LOG_FILE_PATH_VAR = "used_car_collect_log_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"

HYUNDAICAR_PVSN_SITE_CD = "ps00003"
HYUNDAICAR_DATST_BRAND = "data7"
HYUNDAICAR_DATST_CAR_TYPE = "data8"
HYUNDAICAR_DATST_LIST = "data9"
HYUNDAICAR_SITE_NAME = "현대차"
HYUNDAICAR_BRAND_TABLE = "ods.ods_brand_list_hyundaicar"
HYUNDAICAR_CAR_TYPE_TABLE = "ods.ods_car_type_list_hyundaicar"
HYUNDAICAR_TMP_LIST_TABLE = "ods.ods_tmp_car_list_hyundaicar"
HYUNDAICAR_SOURCE_LIST_TABLE = "ods.ods_car_list_hyundaicar"
HYUNDAICAR_COLLECT_DETAIL_TABLE = "std.tn_data_clct_dtl_info"

URL = "https://certified.hyundai.com/p/search/vehicle"
DETAIL_URL_TEMPLATE = "https://certified.hyundai.com/p/goods/goodsDetail.do?goodsNo={}"
LIST_SELECTOR = ".search_result .resultlist .product.productlist li.type02:not(.banner)"
LIST_SELECTOR_FALLBACK = ".resultlist .product.productlist li.type02:not(.banner), .resultlist .productlist li.type02:not(.banner)"
LIST_MORE_BUTTON = "#btnSeeMore"
LIST_PER_CAR_TYPE = 0
USE_CHROME_CHANNEL = os.environ.get("HYUNDAICAR_CHROME_CHANNEL", "0").strip().lower() in ("1", "true", "yes")
HEADLESS_MODE = os.environ.get("HYUNDAICAR_HEADED", "0").strip().lower() not in ("1", "true", "yes")

YEAR_STR = ""
DATE_STR = ""
RUN_TS = ""
RESULT_DIR = Path("/tmp")
LOG_DIR = Path("/tmp")
IMG_BASE = Path("/tmp")


def _norm(value: str) -> str:
    return " ".join((value or "").strip().split())


def _normalize_production_period(value: str) -> str:
    return _norm((value or "").replace("년", ""))


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
    if key == HYUNDAICAR_DATST_BRAND:
        return normalized or HYUNDAICAR_BRAND_TABLE
    if key == HYUNDAICAR_DATST_CAR_TYPE:
        return normalized or HYUNDAICAR_CAR_TYPE_TABLE
    if key == HYUNDAICAR_DATST_LIST:
        return normalized or HYUNDAICAR_TMP_LIST_TABLE
    if normalized:
        return normalized
    raise ValueError(f"적재 대상 테이블을 확인할 수 없습니다: datst_cd={datst_cd}")


def _resolve_list_table_targets(tn_data_bsc_info: TnDataBscInfo) -> tuple[str, str]:
    tmp_table = _normalize_target_table(getattr(tn_data_bsc_info, "tmpr_tbl_phys_nm", None)) or HYUNDAICAR_TMP_LIST_TABLE
    source_table = _normalize_target_table(getattr(tn_data_bsc_info, "ods_tbl_phys_nm", None)) or HYUNDAICAR_SOURCE_LIST_TABLE
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
            raw = Variable.get(USED_CAR_SITE_NAMES_VAR, default_var="{}", deserialize_json=True)
        if isinstance(raw, dict):
            mapping = {str(k).lower(): v for k, v in raw.items()}
    except Exception:
        mapping = {}
    name = mapping.get(key)
    if name is not None and str(name).strip():
        return str(name).strip()
    return HYUNDAICAR_SITE_NAME


def get_hyundaicar_site_name(kwargs: dict[str, Any] | None = None) -> str:
    for datst_cd in (HYUNDAICAR_DATST_LIST, HYUNDAICAR_DATST_BRAND, HYUNDAICAR_DATST_CAR_TYPE):
        site_name = get_site_name_by_datst(datst_cd, kwargs=kwargs)
        if site_name and site_name.strip():
            return site_name
    return HYUNDAICAR_SITE_NAME


def activate_paths_for_datst(datst_cd: str, kwargs: dict[str, Any] | None = None) -> None:
    global RESULT_DIR, LOG_DIR, IMG_BASE, YEAR_STR, DATE_STR, RUN_TS

    result_root = _get_result_root_path(kwargs=kwargs)
    log_root = _get_log_root_path(kwargs=kwargs)
    img_root = _get_img_root_path(kwargs=kwargs)
    site = get_hyundaicar_site_name(kwargs=kwargs)
    now = datetime.now()
    YEAR_STR = now.strftime("%Y년")
    DATE_STR = now.strftime("%Y%m%d")
    RUN_TS = now.strftime("%Y%m%d%H%M")

    RESULT_DIR = CommonUtil.build_dated_site_path(result_root, site, now)
    LOG_DIR = CommonUtil.build_dated_site_path(log_root, site, now)
    IMG_BASE = CommonUtil.build_year_site_path(img_root, site, now)


try:
    activate_paths_for_datst(HYUNDAICAR_DATST_LIST)
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
    create_dt_values = sorted({str(row.get("create_dt") or "").strip() for row in rows if str(row.get("create_dt") or "").strip()})
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


def _sync_hyundaicar_tmp_to_source(
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

    current_rows = _dedupe_hyundaicar_list_rows(current_rows)
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


def setup_logger(log_basename: str = "hyundaicar_crawler") -> logging.Logger:
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


def _click_if_visible(page, selector: str, timeout_ms: int = 700) -> bool:
    try:
        el = page.locator(selector).first
        if el.count() > 0 and el.is_visible(timeout=timeout_ms):
            el.click()
            page.wait_for_timeout(200)
            return True
    except Exception:
        return False
    return False


def dismiss_common_popups(page, logger) -> None:
    for _ in range(4):
        clicked = False
        clicked |= _click_if_visible(page, "button:has-text('동의하기')")
        clicked |= _click_if_visible(page, "a:has-text('동의하기')")
        clicked |= _click_if_visible(page, "button:has-text('닫기')")
        clicked |= _click_if_visible(page, "a:has-text('닫기')")
        clicked |= _click_if_visible(page, "button:has-text('확인')")
        clicked |= _click_if_visible(page, "a:has-text('확인')")
        if not clicked:
            break
    logger.debug("공통 팝업 처리 완료")


def _reset_hyundaicar_filters(page, logger) -> bool:
    selectors = [
        "button:has-text('필터 초기화')",
        "a:has-text('필터 초기화')",
        "button:has-text('초기화')",
        "a:has-text('초기화')",
    ]
    for selector in selectors:
        try:
            btn = page.locator(selector).first
            if btn.count() == 0:
                continue
            btn.scroll_into_view_if_needed(timeout=3000)
            btn.click(timeout=3000)
            page.wait_for_timeout(500)
            _wait_until_list_stable(page, pause_ms=300, max_rounds=8)
            logger.info("필터 초기화 클릭 완료")
            return True
        except Exception:
            continue
    logger.debug("필터 초기화 버튼을 찾지 못해 기존 선택 상태를 유지합니다.")
    return False


def _ensure_hyundaicar_search_page(page, logger) -> None:
    if URL not in (page.url or ""):
        page.goto(URL, wait_until="domcontentloaded", timeout=90000)
    page.wait_for_selector("#CPOwrap, #saleVehicleFilter", timeout=30000)
    page.wait_for_timeout(800)
    dismiss_common_popups(page, logger)
    try:
        page.wait_for_load_state("networkidle", timeout=3000)
    except Exception:
        pass


def _get_car_type_box(page):
    box = page.locator("#saleVehicleFilter > li[data-ref='toggleBox'].isShow").first
    if box.count() == 0:
        box = page.locator("#saleVehicleFilter > li[data-ref='toggleBox']").first
    if box.count() == 0:
        box = page.locator("#saleVehicleFilter li[data-ref='toggleBox'].isShow").first
    if box.count() == 0:
        box = page.locator("#saleVehicleFilter li[data-ref='toggleBox']").first
    return box


def _extract_car_type_names(box) -> list[str]:
    try:
        names = box.locator(".cont .form_filter.model label").evaluate_all(
            """
            nodes => {
                const norm = value => (value || '').replace(/\\s+/g, ' ').trim();
                return nodes
                    .map(node => norm(node.textContent || ''))
                    .filter(Boolean);
            }
            """
        )
    except Exception:
        names = []

    ordered: list[str] = []
    seen: set[str] = set()
    for name in names or []:
        normalized = _norm(name)
        if normalized and normalized not in seen:
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def _select_car_type(page, car_type_name: str) -> bool:
    try:
        clicked = page.evaluate(
            """
            target => {
                const norm = value => (value || '').replace(/\\s+/g, ' ').trim();
                const box =
                    document.querySelector('#saleVehicleFilter > li[data-ref="toggleBox"].isShow') ||
                    document.querySelector('#saleVehicleFilter > li[data-ref="toggleBox"]') ||
                    document.querySelector('#saleVehicleFilter li[data-ref="toggleBox"].isShow') ||
                    document.querySelector('#saleVehicleFilter li[data-ref="toggleBox"]');
                if (!box) return false;
                const labels = Array.from(box.querySelectorAll('.cont .form_filter.model label'));
                const label = labels.find(node => norm(node.textContent || '') === target);
                if (!label) return false;
                label.click();
                return true;
            }
            """,
            car_type_name,
        )
        if clicked:
            return True
    except Exception:
        pass
    try:
        label = _get_car_type_box(page).locator("label").filter(has_text=car_type_name).first
        if label.count() > 0:
            label.click()
            return True
    except Exception:
        pass
    return False


def _select_brand(page, brand_name: str) -> bool:
    try:
        clicked = page.evaluate(
            """
            target => {
                const norm = value => (value || '').replace(/\\s+/g, ' ').trim();
                const root =
                    document.querySelectorAll('#saleVehicleFilter > li[data-ref="toggleBox"]')[1] ||
                    document.querySelectorAll('#saleVehicleFilter li[data-ref="toggleBox"]')[1];
                if (!root) return false;
                const radio = Array.from(root.querySelectorAll('.form_filter.brand input[type="radio"]'))
                    .find(node => (node.getAttribute('ga-tx') || '').includes(target));
                if (radio) {
                    radio.click();
                    return true;
                }
                const label = Array.from(root.querySelectorAll('.form_filter.brand label'))
                    .find(node => norm(node.textContent || '') === target);
                if (label) {
                    label.click();
                    return true;
                }
                return false;
            }
            """,
            brand_name,
        )
        return bool(clicked)
    except Exception:
        return False


def _get_brand_names(page) -> list[str]:
    try:
        brand_names = page.evaluate(
            """
            () => {
                const norm = value => (value || '').replace(/\\s+/g, ' ').trim();
                const root =
                    document.querySelectorAll('#saleVehicleFilter > li[data-ref="toggleBox"]')[1] ||
                    document.querySelectorAll('#saleVehicleFilter li[data-ref="toggleBox"]')[1];
                if (!root) return [];
                return Array.from(root.querySelectorAll('.form_filter.brand label'))
                    .map(node => norm(node.textContent || ''))
                    .filter(Boolean);
            }
            """
        )
    except Exception:
        brand_names = []

    ordered: list[str] = []
    seen: set[str] = set()
    for name in brand_names or ["현대", "제네시스"]:
        normalized = _norm(name)
        if normalized and normalized not in seen:
            seen.add(normalized)
            ordered.append(normalized)
    return ordered or ["현대", "제네시스"]


def _extract_brand_rows_for_selected_brand(page, brand_name: str) -> list[dict[str, str]]:
    try:
        page.evaluate(
            """
            target => {
                const root =
                    document.querySelectorAll('#saleVehicleFilter > li[data-ref="toggleBox"]')[1] ||
                    document.querySelectorAll('#saleVehicleFilter li[data-ref="toggleBox"]')[1];
                if (!root) return 0;
                const list = root.querySelector('.brandalllist');
                if (!list) return 0;
                const items = Array.from(list.children).filter(node => node.matches('li[data-ref="toggleBox"]'));
                const matched = items.filter(node => {
                    const cls = String(node.className || '');
                    return target === '현대' ? cls.includes('hmclist') : !cls.includes('hmclist');
                });
                matched.forEach(node => {
                    const btn = node.querySelector(':scope > .btn_toggle');
                    if (btn && !String(node.className || '').includes('open')) {
                        btn.click();
                    }
                });
                return matched.length;
            }
            """,
            brand_name,
        )
    except Exception:
        return []

    page.wait_for_timeout(500)

    try:
        return page.evaluate(
            """
            target => {
                const norm = value => (value || '').replace(/\\s+/g, ' ').trim();
                const directChildren = (root, selector) =>
                    Array.from((root && root.children) || []).filter(node => node.matches(selector));
                const readLabel = root => {
                    const label = root.querySelector('label[for^="filter"] .txt, label .txt');
                    return norm(label ? label.textContent || '' : '');
                };

                const root =
                    document.querySelectorAll('#saleVehicleFilter > li[data-ref="toggleBox"]')[1] ||
                    document.querySelectorAll('#saleVehicleFilter li[data-ref="toggleBox"]')[1];
                if (!root) return [];
                const list = root.querySelector('.brandalllist');
                if (!list) return [];

                const items = directChildren(list, 'li[data-ref="toggleBox"]').filter(node => {
                    const cls = String(node.className || '');
                    return target === '현대' ? cls.includes('hmclist') : !cls.includes('hmclist');
                });

                const rows = [];
                for (const item of items) {
                    const carUl = directChildren(item, 'ul')[0];
                    if (!carUl) continue;
                    for (const carLi of directChildren(carUl, 'li')) {
                        const carList = readLabel(carLi);
                        if (!carList) continue;
                        const modelUl = directChildren(carLi, 'ul')[0];
                        if (!modelUl) {
                            rows.push({
                                brand_list: target,
                                car_list: carList,
                                model_list: '',
                                model_list_1: '',
                                model_list_2: '',
                                production_period: '',
                            });
                            continue;
                        }

                        const modelLis = directChildren(modelUl, 'li');
                        if (!modelLis.length) {
                            rows.push({
                                brand_list: target,
                                car_list: carList,
                                model_list: '',
                                model_list_1: '',
                                model_list_2: '',
                                production_period: '',
                            });
                            continue;
                        }

                        for (const modelLi of modelLis) {
                            const txtEl = modelLi.querySelector('label[for^="filter"] .txt, label .txt');
                            const smallEl = modelLi.querySelector('small');
                            let modelList = norm(txtEl ? txtEl.textContent || '' : '');
                            const production = norm((smallEl ? smallEl.textContent || '' : '').replace(/년/g, ''));
                            if (production && modelList.includes(production)) {
                                modelList = norm(modelList.replace(production, ''));
                            }

                            const m1Uls = directChildren(modelLi, 'ul');
                            if (!m1Uls.length) {
                                rows.push({
                                    brand_list: target,
                                    car_list: carList,
                                    model_list: modelList,
                                    model_list_1: '',
                                    model_list_2: '',
                                    production_period: production,
                                });
                                continue;
                            }

                            for (const m1Ul of m1Uls) {
                                const m1Lis = directChildren(m1Ul, 'li');
                                if (!m1Lis.length) {
                                    rows.push({
                                        brand_list: target,
                                        car_list: carList,
                                        model_list: modelList,
                                        model_list_1: '',
                                        model_list_2: '',
                                        production_period: production,
                                    });
                                    continue;
                                }

                                for (const m1Li of m1Lis) {
                                    const modelList1 = readLabel(m1Li);
                                    const m2Uls = directChildren(m1Li, 'ul');
                                    if (!m2Uls.length) {
                                        rows.push({
                                            brand_list: target,
                                            car_list: carList,
                                            model_list: modelList,
                                            model_list_1: modelList1,
                                            model_list_2: '',
                                            production_period: production,
                                        });
                                        continue;
                                    }

                                    let wrote = false;
                                    for (const m2Ul of m2Uls) {
                                        const m2Lis = directChildren(m2Ul, 'li');
                                        for (const m2Li of m2Lis) {
                                            rows.push({
                                                brand_list: target,
                                                car_list: carList,
                                                model_list: modelList,
                                                model_list_1: modelList1,
                                                model_list_2: readLabel(m2Li),
                                                production_period: production,
                                            });
                                            wrote = true;
                                        }
                                    }
                                    if (!wrote) {
                                        rows.push({
                                            brand_list: target,
                                            car_list: carList,
                                            model_list: modelList,
                                            model_list_1: modelList1,
                                            model_list_2: '',
                                            production_period: production,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                return rows;
            }
            """,
            brand_name,
        )
    except Exception:
        return []


def run_hyundaicar_car_type_list(page, result_dir: Path, logger, csv_path: Path | None = None) -> None:
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_path if csv_path is not None else (result_dir / "hyundaicar_car_type_list.csv")
    if csv_path.exists():
        csv_path.unlink()

    headers = ["car_type_sn", "car_type_name", "date_crtr_pnttm", "create_dt"]

    try:
        logger.info("현대차 차종 목록 수집 시작: %s", URL)
        _ensure_hyundaicar_search_page(page, logger)
        box = _get_car_type_box(page)
        if box.count() == 0:
            logger.warning("차종 필터 박스를 찾지 못했습니다.")
            return

        rows = _extract_car_type_names(box)
        if not rows:
            logger.warning("차종 목록이 비어 있습니다.")
            return

        now = datetime.now()
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for idx, name in enumerate(rows, start=1):
                w.writerow(
                    {
                        "car_type_sn": idx,
                        "car_type_name": name,
                        "date_crtr_pnttm": now.strftime("%Y%m%d"),
                        "create_dt": now.strftime("%Y%m%d%H%M"),
                    }
                )
        logger.info("현대차 차종 목록 저장 완료: %s (총 %d건)", csv_path, len(rows))
    except Exception as e:
        logger.error("차종 수집 오류: %s", e, exc_info=True)


def run_hyundaicar_brand_list(page, result_dir: Path, logger, csv_path: Path | None = None) -> None:
    result_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_path if csv_path is not None else (result_dir / "hyundaicar_brand_list.csv")
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

    try:
        logger.info("현대차 브랜드 목록 수집 시작: %s", URL)
        _ensure_hyundaicar_search_page(page, logger)
        brand_names = _get_brand_names(page)
        now = datetime.now()
        all_rows: list[dict[str, Any]] = []
        model_sn = 1

        for brand_name in brand_names:
            if not _select_brand(page, brand_name):
                logger.warning("[%s] 브랜드 선택 실패", brand_name)
                continue
            page.wait_for_timeout(400)
            brand_rows = _extract_brand_rows_for_selected_brand(page, brand_name)
            logger.info("[%s] 추출 %d건", brand_name, len(brand_rows))
            for row in brand_rows:
                all_rows.append(
                    {
                        "model_sn": model_sn,
                        "brand_list": row.get("brand_list", "") or brand_name,
                        "car_list": row.get("car_list", ""),
                        "model_list": row.get("model_list", ""),
                        "model_list_1": row.get("model_list_1", ""),
                        "model_list_2": row.get("model_list_2", ""),
                        "production_period": _normalize_production_period(row.get("production_period", "")),
                        "date_crtr_pnttm": now.strftime("%Y%m%d"),
                        "create_dt": now.strftime("%Y%m%d%H%M"),
                    }
                )
                model_sn += 1

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            w.writerows(all_rows)

        logger.info("현대차 브랜드 목록 저장 완료: %s (총 %d건)", csv_path, len(all_rows))
    except Exception as e:
        logger.error("브랜드 목록 수집 오류: %s", e, exc_info=True)


def _extract_product_id_from_href(href: str) -> str:
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
    if not (text or "").strip():
        return ""
    return _norm(text).replace(" (", "(")


def _load_hyundaicar_brand_map(result_dir: Path, logger, brand_path: Path | None = None) -> list[dict[str, str]]:
    csv_path = Path(brand_path) if brand_path else (result_dir / "hyundaicar_brand_list.csv")
    rows: list[dict[str, str]] = []
    if not csv_path.exists():
        logger.warning("브랜드 CSV 없음: %s", csv_path)
        return rows
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                model_list = (row.get("model_list") or "").strip()
                model_list_1 = (row.get("model_list_1") or "").strip()
                car_list = (row.get("car_list") or "").strip()
                brand_list = (row.get("brand_list") or "").strip()
                combined_src = " ".join(p for p in [model_list, model_list_1] if p)
                combined_key = _build_brand_match_key(combined_src) if combined_src else ""
                model_key = _build_brand_match_key(model_list) if model_list else ""
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
    except Exception as e:
        logger.warning("브랜드 CSV 로드 실패: %s", e)
    logger.info("브랜드 매핑 로드 완료: %d건", len(rows))
    return rows


def _find_brand_for_name(name: str, brand_rows: list[dict[str, str]]):
    if not name or not brand_rows:
        return None
    nm_norm = _norm(name).replace(" (", "(")
    if not nm_norm:
        return None
    for key_name in ("combined", "model", "car"):
        best = None
        best_len = -1
        for row in brand_rows:
            key = row.get(key_name) or ""
            if key and key in nm_norm and len(key) > best_len:
                best = row
                best_len = len(key)
        if best:
            return best["brand_list"], best["car_list"], best.get("model_list", "")
    return None


def _get_list_locator(page):
    items = page.locator(LIST_SELECTOR)
    if items.count() == 0:
        items = page.locator(LIST_SELECTOR_FALLBACK)
    return items


def _wait_until_list_stable(page, pause_ms: int = 500, max_rounds: int = 10) -> int:
    prev_count = -1
    same_count_rounds = 0
    current_count = 0
    for _ in range(max_rounds):
        current_count = _get_list_locator(page).count()
        if current_count == prev_count:
            same_count_rounds += 1
            if same_count_rounds >= 2:
                break
        else:
            same_count_rounds = 0
        prev_count = current_count
        page.wait_for_timeout(pause_ms)
    return current_count


def _click_more_until_end(page, logger, needed_count: int | None = None) -> int:
    no_change_rounds = 0
    while True:
        current_count = _wait_until_list_stable(page)
        if needed_count is not None and current_count >= needed_count:
            return current_count
        btn = page.locator(LIST_MORE_BUTTON).first
        if btn.count() == 0:
            return current_count
        try:
            btn.scroll_into_view_if_needed(timeout=3000)
        except Exception:
            pass
        clicked = False
        try:
            if btn.is_visible(timeout=800):
                btn.click()
                clicked = True
        except Exception:
            clicked = False
        if not clicked:
            try:
                clicked = bool(
                    page.evaluate(
                        """
                        () => {
                            const btn = document.querySelector('#btnSeeMore');
                            if (!btn) return false;
                            btn.click();
                            return true;
                        }
                        """
                    )
                )
            except Exception:
                clicked = False
        if not clicked:
            return current_count
        page.wait_for_timeout(500)
        new_count = _wait_until_list_stable(page)
        if new_count <= current_count:
            no_change_rounds += 1
            if no_change_rounds >= 2:
                logger.info("더보기 종료: 목록 수 %d", new_count)
                return new_count
        else:
            no_change_rounds = 0


def _extract_list_rows_from_dom(page, date_crtr_pnttm: str, create_dt: str) -> list[dict[str, str]]:
    items = _get_list_locator(page)
    if items.count() == 0:
        return []
    return items.evaluate_all(
        """
        (nodes, meta) => {
            const norm = value => (value || '').replace(/\\s+/g, ' ').trim();
            const txt = (root, selector) => {
                const el = root.querySelector(selector);
                return norm(el ? el.textContent || '' : '');
            };
            const attr = (root, selector, name) => {
                const el = root.querySelector(selector);
                return norm(el ? el.getAttribute(name) || '' : '');
            };
            const productIdFromHref = href => {
                if (!href) return '';
                const m1 = href.match(/goodsDeatil\\s*\\(\\s*['"]([^'"]+)['"]/i);
                if (m1) return norm(m1[1] || '');
                const m2 = href.match(/goodsDetail\\s*\\(\\s*['"]([^'"]+)['"]/i);
                return m2 ? norm(m2[1] || '') : '';
            };

            return nodes.map(li => {
                const href =
                    attr(li, "a[href*='goodsDeatil']", 'href') ||
                    attr(li, "a[href*='goodsDetail']", 'href');
                const drive = Array.from(li.querySelectorAll('.unit_info .drive span'))
                    .map(node => norm(node.textContent || ''))
                    .filter(Boolean);
                const flags = Array.from(li.querySelectorAll('.flag span'))
                    .map(node => norm(node.textContent || ''))
                    .filter(Boolean);
                const thumb = li.querySelector('.unit_img img, [class*="unit_img"] img');
                return {
                    product_id: productIdFromHref(href),
                    car_type: '',
                    brand_list: '',
                    car_list: '',
                    model_list: '',
                    car_name: txt(li, '.unit_info .name'),
                    release_dt: drive[0] || '',
                    car_navi: drive[1] || '',
                    car_num: drive[2] || '',
                    local_dos: drive[3] || '',
                    pay: txt(li, '.price .txt.pay'),
                    del: txt(li, '.price .txt.del') || '-',
                    sale: txt(li, '.price .txt.sale'),
                    flag: flags.join('|'),
                    detail_url: '',
                    car_imgs: '',
                    date_crtr_pnttm: meta.date_crtr_pnttm,
                    create_dt: meta.create_dt,
                    thumb_src: norm(thumb ? (thumb.getAttribute('src') || thumb.getAttribute('data-src') || '') : ''),
                };
            });
        }
        """,
        {"date_crtr_pnttm": date_crtr_pnttm, "create_dt": create_dt},
    )


def _save_list_thumbnail_from_src(
    src: str,
    product_id: str,
    img_dir: Path,
    logger,
    *,
    return_absolute: bool = False,
) -> str:
    if not src or not product_id:
        return ""
    try:
        img_dir.mkdir(parents=True, exist_ok=True)
        abs_url = urljoin(URL, src)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": URL,
        }
        response = requests.get(abs_url, headers=headers, timeout=15)
        response.raise_for_status()
        out_path = img_dir / f"{product_id}_list.png"
        out_path.write_bytes(response.content)
        if return_absolute:
            return str(out_path)
        return f"imgs/{YEAR_STR}/{HYUNDAICAR_SITE_NAME}/list/{product_id}_list.png"
    except Exception as e:
        logger.debug("목록 썸네일 저장 실패 %s: %s", product_id, e)
        return ""


def _dedupe_hyundaicar_list_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def run_hyundaicar_list(
    page,
    result_dir: Path,
    logger,
    list_csv_path: Path | None = None,
    brand_csv_path: Path | None = None,
    list_img_dir: Path | None = None,
) -> None:
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
        "detail_url",
        "car_imgs",
        "date_crtr_pnttm",
        "create_dt",
    ]

    now = datetime.now()
    date_crtr_pnttm = now.strftime("%Y%m%d")
    create_dt = now.strftime("%Y%m%d%H%M")
    brand_map = _load_hyundaicar_brand_map(result_dir, logger, brand_path=brand_csv_path)
    img_save_dir = Path(list_img_dir) if list_img_dir is not None else (IMG_BASE / "list")
    img_save_dir.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("현대차 목록 수집 시작: %s", URL)
        _ensure_hyundaicar_search_page(page, logger)
        box = _get_car_type_box(page)
        if box.count() == 0:
            logger.warning("차종 필터 박스를 찾지 못했습니다.")
            return

        car_type_names = _extract_car_type_names(box)
        if not car_type_names:
            logger.warning("차종 목록을 읽지 못했습니다.")
            return

        seen_product_ids: dict[str, str] = {}
        total_written = 0
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()

            for car_type_name in car_type_names:
                _reset_hyundaicar_filters(page, logger)
                if not _select_car_type(page, car_type_name):
                    logger.warning("[%s] 차종 선택 실패", car_type_name)
                    continue
                page.wait_for_timeout(400)
                _wait_until_list_stable(page)
                need = LIST_PER_CAR_TYPE if LIST_PER_CAR_TYPE > 0 else None
                loaded_count = _click_more_until_end(page, logger, needed_count=need)
                extracted_rows = _extract_list_rows_from_dom(page, date_crtr_pnttm, create_dt)
                to_take = min(len(extracted_rows), need) if need is not None else len(extracted_rows)
                logger.info("[%s] DOM 추출 %d건, 사용 %d건", car_type_name, loaded_count, to_take)

                for row in extracted_rows[:to_take]:
                    product_id = (row.get("product_id") or "").strip()
                    if product_id and product_id in seen_product_ids:
                        logger.info(
                            "[중복 스킵] current=%s previous=%s product_id=%s",
                            car_type_name,
                            seen_product_ids[product_id],
                            product_id,
                        )
                        continue
                    if product_id:
                        seen_product_ids[product_id] = car_type_name

                    total_written += 1
                    row["model_sn"] = total_written
                    row["car_type"] = car_type_name
                    row["detail_url"] = DETAIL_URL_TEMPLATE.format(product_id) if product_id else ""

                    if row.get("car_name") and brand_map:
                        matched = _find_brand_for_name(row["car_name"], brand_map)
                        if matched:
                            row["brand_list"], row["car_list"], row["model_list"] = matched

                    thumb_src = row.pop("thumb_src", "")
                    row["car_imgs"] = _save_list_thumbnail_from_src(
                        thumb_src,
                        product_id,
                        img_save_dir,
                        logger,
                        return_absolute=list_img_dir is not None,
                    )

                    w.writerow({key: row.get(key, "") for key in headers})

                page.wait_for_timeout(200)

        logger.info("현대차 목록 저장 완료: %s (총 %d건)", csv_path, total_written)
    except Exception as e:
        logger.error("목록 수집 오류: %s", e, exc_info=True)


def _run_hyundaicar_car_type_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    activate_paths_for_datst((datst_cd or HYUNDAICAR_DATST_CAR_TYPE).lower() or HYUNDAICAR_DATST_CAR_TYPE, kwargs=kwargs)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    csv_path = RESULT_DIR / f"hyundaicar_car_type_list_{run_ts}.csv"
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE, prefer_chrome=USE_CHROME_CHANNEL)
        page = browser.new_page()
        try:
            run_hyundaicar_car_type_list(page, RESULT_DIR, logger, csv_path=csv_path)
        finally:
            browser.close()
    return str(csv_path)


def _run_hyundaicar_brand_csv(datst_cd: str, kwargs: dict[str, Any] | None = None) -> str:
    activate_paths_for_datst((datst_cd or HYUNDAICAR_DATST_BRAND).lower() or HYUNDAICAR_DATST_BRAND, kwargs=kwargs)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    csv_path = RESULT_DIR / f"hyundaicar_brand_list_{run_ts}.csv"
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE, prefer_chrome=USE_CHROME_CHANNEL)
        page = browser.new_page()
        try:
            run_hyundaicar_brand_list(page, RESULT_DIR, logger, csv_path=csv_path)
        finally:
            browser.close()
    return str(csv_path)


def run_hyundaicar_list_job(
    bsc: TnDataBscInfo,
    *,
    brand_list_csv_path: str,
    car_type_csv_path: str | None = None,
    kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    activate_paths_for_datst((bsc.datst_cd or HYUNDAICAR_DATST_LIST).lower() or HYUNDAICAR_DATST_LIST, kwargs=kwargs)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    list_img_dir = IMG_BASE / "list"
    list_img_dir.mkdir(parents=True, exist_ok=True)
    _clear_directory_contents(list_img_dir)

    logger = _get_file_logger(run_ts)
    brand_path = Path(brand_list_csv_path)
    if not brand_path.is_file():
        raise FileNotFoundError(f"브랜드 CSV가 없습니다: {brand_path}")
    if car_type_csv_path and not Path(car_type_csv_path).is_file():
        logger.warning("차종 CSV 경로 없음(무시하고 진행): %s", car_type_csv_path)

    logger.info("🏁 현대차 목록 수집 시작")
    logger.info("- pvsn_site_cd=%s, datst_cd=%s, datst_nm=%s", bsc.pvsn_site_cd, bsc.datst_cd, bsc.datst_nm)
    logger.info("- link_url=%s", bsc.link_data_clct_url)
    logger.info("- 브랜드 CSV=%s", brand_path)

    list_path = RESULT_DIR / f"hyundaicar_list_{run_ts}.csv"
    if list_path.exists():
        list_path.unlink()

    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE, prefer_chrome=USE_CHROME_CHANNEL)
        page = browser.new_page()
        try:
            run_hyundaicar_list(
                page,
                RESULT_DIR,
                logger,
                list_csv_path=list_path,
                brand_csv_path=brand_path,
                list_img_dir=list_img_dir,
            )
        finally:
            browser.close()

    count = 0
    if list_path.exists():
        with open(list_path, "r", encoding="utf-8-sig", newline="") as f:
            count = sum(1 for _ in csv.DictReader(f))

    logger.info("✅ 현대차 목록 수집 완료: 총 %d건", count)
    return {
        "brand_csv": str(brand_path),
        "car_type_csv": car_type_csv_path or "",
        "list_csv": str(list_path),
        "count": count,
        "done": list_path.is_file() and count > 0,
        "log_dir": str(LOG_DIR),
        "img_base": str(IMG_BASE),
    }


@dag(
    dag_id="sdag_hyundaicar_crawler",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["used_car", "hyundaicar", "crawler", "day"],
)
def hyundaicar_crawler_dag():
    pg_hook = PostgresHook(postgres_conn_id="car_db_conn")

    @task
    def insert_collect_data_info(**kwargs) -> dict[str, dict[str, Any]]:
        select_bsc_info_stmt = f"""
        SELECT * FROM std.tn_data_bsc_info tdbi
        WHERE 1=1
          AND LOWER(clct_yn) = 'y'
          AND LOWER(link_yn) = 'y'
          AND LOWER(pvsn_site_cd) = '{HYUNDAICAR_PVSN_SITE_CD}'
          AND LOWER(datst_cd) IN ('{HYUNDAICAR_DATST_BRAND}','{HYUNDAICAR_DATST_CAR_TYPE}','{HYUNDAICAR_DATST_LIST}')
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
            for key in (HYUNDAICAR_DATST_BRAND, HYUNDAICAR_DATST_CAR_TYPE, HYUNDAICAR_DATST_LIST)
            if key not in out
        ]
        if missing:
            raise ValueError(
                f"std.tn_data_bsc_info 조회 결과 누락: {missing} (pvsn_site_cd={HYUNDAICAR_PVSN_SITE_CD})"
            )
        return out

    @task
    def run_car_type_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        datst_cd = str((infos.get(HYUNDAICAR_DATST_CAR_TYPE) or {}).get("datst_cd") or HYUNDAICAR_DATST_CAR_TYPE).lower()
        return _run_hyundaicar_car_type_csv(datst_cd, kwargs=kwargs)

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]], **kwargs) -> str:
        datst_cd = str((infos.get(HYUNDAICAR_DATST_BRAND) or {}).get("datst_cd") or HYUNDAICAR_DATST_BRAND).lower()
        return _run_hyundaicar_brand_csv(datst_cd, kwargs=kwargs)

    @task
    def run_list_csv(
        bsc_infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        car_type_csv_path: str,
        **kwargs,
    ) -> dict[str, Any]:
        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[HYUNDAICAR_DATST_LIST])
        return run_hyundaicar_list_job(
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
            HYUNDAICAR_COLLECT_DETAIL_TABLE,
            tn_data_bsc_info.datst_cd,
            csv_file_path,
        )
        registered_file_path = str(CommonUtil.build_collect_detail_file_path(tn_data_clct_dtl_info))
        logging.info(
            "현대차 수집 메타 등록: datst_cd=%s, file=%s, clct_pnttm=%s, status=%s",
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
            HYUNDAICAR_COLLECT_DETAIL_TABLE,
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
        if datst_cd == HYUNDAICAR_DATST_LIST:
            original_count = len(rows)
            rows = _dedupe_hyundaicar_list_rows(rows)
            if len(rows) != original_count:
                logging.info(
                    "현대차 list 중복 제거: before=%d, after=%d, removed=%d",
                    original_count,
                    len(rows),
                    original_count - len(rows),
                )
            target_table, _ = _resolve_list_table_targets(tn_data_bsc_info)

        if datst_cd in (HYUNDAICAR_DATST_BRAND, HYUNDAICAR_DATST_CAR_TYPE, HYUNDAICAR_DATST_LIST):
            _delete_snapshot_rows(hook, target_table, rows)

        _bulk_insert_rows(hook, target_table, rows, truncate=False, allow_only_table_cols=True)
        table_count = CommonUtil.get_table_row_count(hook, target_table)
        logging.info(
            "현대차 CSV 적재 완료: datst_cd=%s, table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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

        tn_data_bsc_info = CommonUtil.build_bsc_info_dto(bsc_infos[HYUNDAICAR_DATST_LIST])
        tmp_table, source_table = _resolve_list_table_targets(tn_data_bsc_info)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        current_rows = _read_csv_rows(Path(str(list_load_result.get("csv_path") or "")))
        sync_result = _sync_hyundaicar_tmp_to_source(
            hook,
            current_rows=current_rows,
            tmp_table=tmp_table,
            source_table=source_table,
        )
        logging.info(
            "현대차 list tmp->source 반영 완료: tmp_table=%s, source_table=%s, tmp_count=%d, current_row_count=%d, inserted_count=%d, marked_existing_count=%d, updated_changed_count=%d, marked_missing_count=%d, source_count=%d",
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
        car_type_path = run_car_type_csv(bsc_infos)
        car_type_collect_info = register_csv_collect_log_info.override(
            task_id="register_car_type_collect_log_info"
        )(bsc_infos, HYUNDAICAR_DATST_CAR_TYPE, car_type_path)

        brand_path = run_brand_csv(bsc_infos)
        brand_collect_info = register_csv_collect_log_info.override(
            task_id="register_brand_collect_log_info"
        )(bsc_infos, HYUNDAICAR_DATST_BRAND, brand_path)

        list_result = run_list_csv(bsc_infos, brand_path, car_type_path)
        list_collect_info = register_csv_collect_log_info.override(
            task_id="register_list_collect_log_info"
        )(bsc_infos, HYUNDAICAR_DATST_LIST, list_result["list_csv"])

        return {
            HYUNDAICAR_DATST_BRAND: brand_collect_info,
            HYUNDAICAR_DATST_CAR_TYPE: car_type_collect_info,
            HYUNDAICAR_DATST_LIST: list_collect_info,
        }

    @task_group(group_id="insert_csv_process")
    def insert_csv_process(
        bsc_infos: dict[str, dict[str, Any]],
        tn_data_clct_dtl_info_map: dict[str, dict[str, Any]],
    ) -> None:
        brand_load_result = load_csv_to_ods.override(task_id="load_brand_csv_to_ods")(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            HYUNDAICAR_DATST_BRAND,
        )
        car_type_load_result = load_csv_to_ods.override(task_id="load_car_type_csv_to_ods")(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            HYUNDAICAR_DATST_CAR_TYPE,
        )
        list_load_result = load_csv_to_ods.override(task_id="load_list_csv_to_ods")(
            bsc_infos,
            tn_data_clct_dtl_info_map,
            HYUNDAICAR_DATST_LIST,
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


dag_object = hyundaicar_crawler_dag()


if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2026, 3, 24, 8, 0),
        conn_file_path=conn_path,
        run_conf={"dtst_cd": dtst_cd},
    )
