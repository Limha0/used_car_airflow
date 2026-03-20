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

from airflow.decorators import dag, task
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


def _latest_csv(dir_path: Path, prefix: str) -> Path | None:
    if not dir_path.exists():
        return None
    files = sorted(dir_path.glob(f"{prefix}_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


@dag(
    dag_id="sdag_kiacar_crawler",
    schedule=None,
    start_date=pd_datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["usedcar", "kiacar", "enrich"],
)
def kiacar_enrich_dag():
    @task
    def prepare_environment() -> dict:
        RESULT_DIR.mkdir(parents=True, exist_ok=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M%S")
        return {
            "run_ts": run_ts,
            "result_dir": str(RESULT_DIR),
            "log_dir": str(LOG_DIR),
        }

    @task
    def enrich_list(ctx: dict, brand_csv_path: str | None = None, list_csv_path: str | None = None) -> dict:
        logger = _get_logger()
        result_dir = Path(ctx["result_dir"])

        brand_path = Path(brand_csv_path) if brand_csv_path else _latest_csv(result_dir, "kiacar_brand_list")
        list_path = Path(list_csv_path) if list_csv_path else _latest_csv(result_dir, "kiacar_list")

        if not brand_path or not list_path:
            raise FileNotFoundError(
                f"입력 CSV를 찾지 못했습니다. brand={brand_path}, list={list_path}, dir={result_dir}"
            )

        logger.info("enrich 입력: brand=%s list=%s", brand_path, list_path)
        return kiacar_enrich_list_with_brand(logger, brand_path, list_path)

    ctx = prepare_environment()
    enrich_list(ctx)


kiacar_dag = kiacar_enrich_dag()

