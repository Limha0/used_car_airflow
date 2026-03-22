#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
기아 인증중고(CPO) 브랜드/목록 수집 DAG.

Airflow DAG:
- DB 메타(std.tn_data_bsc_info, ps00005, data13/15) 조회
- 브랜드 CSV → 목록 CSV → enrich 순으로 단계별 Task 실행
"""
import csv
import importlib.util
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from pathlib import Path
from typing import Any

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from playwright.sync_api import sync_playwright


@dag(
    dag_id="sdag_kiacar_crawler",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["used_car", "kiacar", "crawler", "day"],
)
def kiacar_crawler_dag():
    """
    기아 인증중고 브랜드/목록 수집 DAG.

    - DB 메타(std.tn_data_bsc_info, ps00005, data13/15) 조회
    - 브랜드 CSV → 목록 CSV → enrich 순, 태스크별 파일 생성
    """

    pg_hook = PostgresHook(postgres_conn_id="car_db_conn")

    @task
    def insert_collect_data_info(**kwargs) -> dict[str, dict[str, Any]]:
        select_bsc_info_stmt = f"""
        SELECT * FROM std.tn_data_bsc_info tdbi
        WHERE 1=1
          AND LOWER(clct_yn) = 'y'
          AND LOWER(link_yn) = 'y'
          AND LOWER(pvsn_site_cd) = '{KIACAR_PVSN_SITE_CD}'
          AND LOWER(datst_cd) IN ('data13','data15')
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

        missing = [k for k in (KIACAR_DATST_BRAND, KIACAR_DATST_LIST, KIACAR_DATST_ENRICH) if k not in out]
        if missing:
            raise ValueError(
                f"std.tn_data_bsc_info 조회 결과 누락: {missing} (pvsn_site_cd={KIACAR_PVSN_SITE_CD})"
            )
        return out

    @task
    def run_brand_csv(infos: dict[str, dict[str, Any]]) -> str:
        dc = str((infos.get(KIACAR_DATST_BRAND) or {}).get("datst_cd") or KIACAR_DATST_BRAND).lower()
        return _run_kiacar_brand_csv(dc)

    @task
    def run_list_csv(infos: dict[str, dict[str, Any]], brand_csv_path: str) -> str:
        dc = str((infos.get(KIACAR_DATST_LIST) or {}).get("datst_cd") or KIACAR_DATST_LIST).lower()
        return _run_kiacar_list_csv(dc, brand_csv_path)

    @task
    def run_enrich_csv(
        infos: dict[str, dict[str, Any]],
        brand_csv_path: str,
        list_csv_path: str,
    ) -> dict[str, Any]:
        dc = str((infos.get(KIACAR_DATST_ENRICH) or {}).get("datst_cd") or KIACAR_DATST_ENRICH).lower()
        return _run_kiacar_enrich_csv(dc, brand_csv_path, list_csv_path)

    @task_group(group_id="create_csv_process")
    def create_csv_process(bsc_infos: dict[str, dict[str, Any]]) -> dict[str, Any]:
        brand_path = run_brand_csv(bsc_infos)
        list_path = run_list_csv(bsc_infos, brand_path)
        return run_enrich_csv(bsc_infos, brand_path, list_path)

    infos = insert_collect_data_info()
    create_csv_process(infos)


_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

USED_CAR_SITE_NAMES_VAR = "used_car_site_names"
CRAWL_BASE_PATH_VAR = "crawl_base_path"

KIACAR_PVSN_SITE_CD = "ps00005"
KIACAR_DATST_BRAND = "data13"
KIACAR_DATST_LIST = "data15"
KIACAR_DATST_ENRICH = "data15"
KIACAR_SITE_NAME = "기아인증중고"

LEGACY_KIACAR_SCRIPT = Path("/home/limhayoung/used_car_crawler/kiacar/crawl_kiacar_type_to_list.py")


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
        raw = Variable.get(USED_CAR_SITE_NAMES_VAR, default_var="{}", deserialize_json=True)
        if isinstance(raw, dict):
            mapping = {str(k).lower(): v for k, v in raw.items()}
    except Exception:
        mapping = {}
    name = mapping.get(key)
    if name is not None and str(name).strip():
        return str(name).strip()
    return KIACAR_SITE_NAME


def get_kiacar_site_name() -> str:
    for datst_cd in (KIACAR_DATST_LIST, KIACAR_DATST_BRAND, KIACAR_DATST_ENRICH):
        site_name = get_site_name_by_datst(datst_cd)
        if site_name and site_name.strip():
            return site_name
    return KIACAR_SITE_NAME


def activate_paths_for_datst(datst_cd: str) -> None:
    global RESULT_DIR, LOG_DIR, IMG_BASE, YEAR_STR, DATE_STR, RUN_TS

    base = _get_crawl_base_path()
    site = get_kiacar_site_name()
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
    activate_paths_for_datst(KIACAR_DATST_LIST)
except Exception:
    pass

HEADLESS_MODE = True
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


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


def _load_legacy_kiacar_module():
    if not LEGACY_KIACAR_SCRIPT.exists():
        raise FileNotFoundError(f"legacy kiacar source not found: {LEGACY_KIACAR_SCRIPT}")
    legacy_root = LEGACY_KIACAR_SCRIPT.parent.parent
    if str(legacy_root) not in sys.path:
        sys.path.insert(0, str(legacy_root))
    spec = importlib.util.spec_from_file_location("legacy_kiacar_crawler", LEGACY_KIACAR_SCRIPT)
    if spec is None or spec.loader is None:
        raise ImportError(f"legacy kiacar source load failed: {LEGACY_KIACAR_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _count_csv_rows(csv_path: Path) -> int:
    if not csv_path.exists():
        return 0
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        return sum(1 for _ in csv.DictReader(f))


def _run_kiacar_brand_csv(datst_cd: str) -> str:
    activate_paths_for_datst((datst_cd or KIACAR_DATST_BRAND).lower() or KIACAR_DATST_BRAND)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    legacy = _load_legacy_kiacar_module()

    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        context = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1280, "height": 800})
        page = context.new_page()
        try:
            legacy.run_kiacar_brand_list(page, RESULT_DIR, logger)
        finally:
            browser.close()
    return str(RESULT_DIR / "kiacar_brand_list.csv")


def _run_kiacar_list_csv(datst_cd: str, brand_csv_path: str) -> str:
    activate_paths_for_datst((datst_cd or KIACAR_DATST_LIST).lower() or KIACAR_DATST_LIST)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (IMG_BASE / "list").mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)
    brand_path = Path(brand_csv_path)
    if not brand_path.is_file():
        raise FileNotFoundError(f"브랜드 CSV가 없습니다: {brand_path}")

    legacy = _load_legacy_kiacar_module()
    with sync_playwright() as p:
        browser = _launch_browser(p, HEADLESS_MODE)
        context = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1280, "height": 800})
        page = context.new_page()
        try:
            legacy.run_kiacar_product_list(page, RESULT_DIR, logger)
        finally:
            browser.close()
    return str(RESULT_DIR / "kiacar_list.csv")


def _run_kiacar_enrich_csv(datst_cd: str, brand_csv_path: str, list_csv_path: str) -> dict[str, Any]:
    activate_paths_for_datst((datst_cd or KIACAR_DATST_ENRICH).lower() or KIACAR_DATST_ENRICH)
    run_ts = datetime.now().strftime("%Y%m%d%H%M")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = _get_file_logger(run_ts)

    brand_path = Path(brand_csv_path)
    list_path = Path(list_csv_path)
    if not brand_path.is_file():
        raise FileNotFoundError(f"브랜드 CSV가 없습니다: {brand_path}")
    if not list_path.is_file():
        raise FileNotFoundError(f"목록 CSV가 없습니다: {list_path}")

    legacy = _load_legacy_kiacar_module()
    logger.info("🏁 기아 인증중고 enrich 시작")
    logger.info("- brand_csv=%s", brand_path)
    logger.info("- list_csv=%s", list_path)
    legacy.enrich_kiacar_list_with_brand(RESULT_DIR, logger)

    return {
        "brand_csv": str(brand_path),
        "list_csv": str(list_path),
        "count": _count_csv_rows(list_path),
        "log_dir": str(LOG_DIR),
        "img_base": str(IMG_BASE),
    }


kiacar_dag = kiacar_crawler_dag()


def main():
    activate_paths_for_datst(KIACAR_DATST_LIST)
    infos = {
        KIACAR_DATST_BRAND: {"datst_cd": KIACAR_DATST_BRAND},
        KIACAR_DATST_LIST: {"datst_cd": KIACAR_DATST_LIST},
        KIACAR_DATST_ENRICH: {"datst_cd": KIACAR_DATST_ENRICH},
    }
    brand_csv = _run_kiacar_brand_csv(str(infos[KIACAR_DATST_BRAND]["datst_cd"]))
    list_csv = _run_kiacar_list_csv(str(infos[KIACAR_DATST_LIST]["datst_cd"]), brand_csv)
    _run_kiacar_enrich_csv(str(infos[KIACAR_DATST_ENRICH]["datst_cd"]), brand_csv, list_csv)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
기아 인증중고(CPO) brand CSV ↔ list CSV 매칭(enrich) DAG.

목적:
- `kiacar_brand_list_*.csv` (필터 트리)와 `kiacar_list_*.csv` (차량 카드 목록)를
  `car_name` 기준으로 매칭해 list의 `car_type`, `car_list`, `model_list`를 채운다.

입력/출력 경로(기본):
- /home/limhayoung/data/crawl/{YYYY년}/기아인증중고/{YYYYMMDD}/
"""

import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task, task_group
from pendulum import datetime as pd_datetime

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

_now = datetime.now()
YEAR_STR = _now.strftime("%Y년")
DATE_STR = _now.strftime("%Y%m%d")
BASE_DATA_PATH = Path("/home/limhayoung/data")
RESULT_DIR = BASE_DATA_PATH / "crawl" / YEAR_STR / "기아인증중고" / DATE_STR
LOG_DIR = BASE_DATA_PATH / "log" / YEAR_STR / "기아인증중고" / DATE_STR


def _norm(s: str) -> str:
    return " ".join((s or "").strip().split())


def _strip_dot(s: str) -> str:
    return _norm((s or "").replace("ㆍ", ""))


def _get_logger(name: str = "KiaCarEnrich") -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / "sdag_kiacar_crawler_enrich.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger


def kiacar_build_brand_lookup(brand_path: Path) -> dict:
    """
    brand CSV에서 list 매칭용 키를 만든다.
    - 키: `model_list`가 연도(4자리 숫자)로 시작하면 `model_list`, 아니면 `model_list_1` 우선
    """
    key_to_brand: dict[str, dict] = {}
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


def kiacar_car_name_to_match_key(car_name_val: str) -> str:
    """
    list의 `car_name`에서 brand 키와 비교할 키를 뽑는다.
    - 예: "2024 레이 Premium ..." → "2024 레이"
    """
    s = (car_name_val or "").strip()
    if not s:
        return ""
    parts = s.split()
    if len(parts) >= 2 and len(parts[0]) == 4 and parts[0].isdigit():
        return (parts[0] + " " + parts[1]).strip()
    return parts[0].strip() if parts else ""


def kiacar_find_brand(match_key: str, key_to_brand: dict) -> dict | None:
    """
    match_key로 brand 후보를 찾는다.
    - 정확 일치 → startswith/포함(가장 긴 키) 순서로 best-effort 매칭.
    """
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


def kiacar_enrich_list_with_brand(logger: logging.Logger, brand_path: Path, list_path: Path) -> dict:
    if not brand_path.exists():
        raise FileNotFoundError(f"brand csv not found: {brand_path}")
    if not list_path.exists():
        raise FileNotFoundError(f"list csv not found: {list_path}")

    key_to_brand = kiacar_build_brand_lookup(brand_path)
    if not key_to_brand:
        logger.warning("brand lookup 0건: %s", brand_path)

    with open(list_path, "r", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        headers = list(r.fieldnames or [])
        rows = [dict(row) for row in r]

    for col in ("car_type", "brand_list", "car_list", "model_list"):
        if col not in headers:
            headers.insert(2, col) if col in ("car_type", "brand_list") else headers.append(col)

    matched = 0
    for row in rows:
        car_name = row.get("car_name") or ""
        mk = kiacar_car_name_to_match_key(car_name)
        b = kiacar_find_brand(mk, key_to_brand) if mk else None
        if b:
            row["brand_list"] = b.get("brand_list") or row.get("brand_list") or "기아"
            row["car_type"] = b.get("car_type") or ""
            row["car_list"] = b.get("car_list") or ""
            row["model_list"] = b.get("model_list") or ""
            matched += 1
        else:
            row["brand_list"] = row.get("brand_list") or "기아"
            row["car_type"] = row.get("car_type") or ""
            row["car_list"] = row.get("car_list") or ""
            row["model_list"] = row.get("model_list") or ""

        # 기존 list 포맷의 year/km에 들어있는 'ㆍ' 제거만 보정(원하면 제거)
        if "year" in row:
            row["year"] = _strip_dot(row.get("year") or "")
        if "km" in row:
            row["km"] = _strip_dot(row.get("km") or "")

    with open(list_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass

    logger.info("enrich 완료: %d/%d 매칭, list=%s", matched, len(rows), list_path)
    return {"matched": matched, "total": len(rows), "list_path": str(list_path)}



