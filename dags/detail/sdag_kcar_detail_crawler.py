# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from util.common_util import CommonUtil
from util.playwright_util import GotoSpec, goto_with_retry, images_enabled, install_route_blocking


def _is_playwright_dead_error(err: BaseException) -> bool:
    """브라우저/컨텍스트/페이지가 이미 닫힌 뒤 발생하는 오류."""
    msg = str(err).lower()
    return any(
        s in msg
        for s in (
            "has been closed",
            "target closed",
            "browser has been closed",
            "context has been closed",
            "page has been closed",
        )
    )


def _is_airflow_shutdown_error(err: BaseException) -> bool:
    """외부에서 failed/SIGTERM 등으로 태스크가 중단될 때."""
    if isinstance(err, AirflowException) and "SIGTERM" in str(err):
        return True
    if isinstance(err, AirflowException) and "SIGKILL" in str(err):
        return True
    return False


# ═══════════════════════════════════════════════════════════════════
#  상수
# ═══════════════════════════════════════════════════════════════════
SOURCE_LIST_TABLE = "ods.ods_car_list_kcar"
TARGET_DETAIL_TABLE = "ods.ods_car_detail_kcar"  # 운영 DB에 동일 네이밍이 없으면 런타임에서 에러 발생

FINAL_FILE_PATH_VAR = "used_car_final_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"
SITE_NAME = "케이카"

DETAIL_CSV_FIELDS = [
    "model_sn",
    "product_id",
    "car_name",
    "car_num",
    "accident",
    "year",
    "km",
    "fuel",
    "car_color",
    "trans",
    "car_price",
    "user_name",
    "call_guide",
    "price_lists",
    "car_select_option",
    "main_option",
    "ext_panel",
    "frame",
    "diagnosis_pass",
    "notice",
    "detail_history",
    "owner_detail",
    "detail_warranty",
    "car_imgs",
    "date_crtr_pnttm",
    "create_dt",
]


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의
# ═══════════════════════════════════════════════════════════════════
@dag(
    dag_id="sdag_kcar_detail_crawl",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "kcar", "detail", "crawler"],
)
def kcar_detail_crawl():
    """K Car 상세: register_flag='A'(최신 스냅샷)만 수집 → 행마다 List complete_yn Y/N, Detail 적재·동기화. register_flag 변경은 list 동기화 전용."""

    # ── Task 1 : DB에서 수집 대상 조회 ────────────────────────────────────
    @task
    def fetch_target_urls() -> list[dict[str, str]]:
        """
        ods.ods_car_list_kcar 에서 register_flag=A 이고
        date_crtr_pnttm 이 테이블 내 최신 적재일과 같은 행만 조회.
        """
        sql = f"""
        SELECT
            l.product_id,
            l.detail_url,
            l.register_flag
        FROM {SOURCE_LIST_TABLE} l
        WHERE TRIM(COALESCE(l.register_flag, '')) = 'A'
          AND l.detail_url IS NOT NULL
          AND TRIM(l.detail_url) != ''
          AND l."date_crtr_pnttm" IS NOT NULL
          AND l."date_crtr_pnttm" = (
              SELECT MAX(m."date_crtr_pnttm")
              FROM {SOURCE_LIST_TABLE} m
              WHERE m."date_crtr_pnttm" IS NOT NULL
          )
        ORDER BY l.model_sn
        """
        logging.info("kcar detail select_target_urls_stmt ::: %s", sql)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        conn = hook.get_conn()
        rows: list[dict[str, str]] = []
        latest_pnttm = None
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT MAX(m."date_crtr_pnttm")
                    FROM {SOURCE_LIST_TABLE} m
                    WHERE m."date_crtr_pnttm" IS NOT NULL
                    """
                )
                max_row = cur.fetchone()
                latest_pnttm = max_row[0] if max_row else None
                logging.info("kcar detail 수집 기준 date_crtr_pnttm(최신): %s", latest_pnttm)

                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall() or []:
                    rows.append(dict(zip(cols, row)))

                if not rows and latest_pnttm is not None:
                    cur.execute(
                        f"""
                        SELECT
                            COUNT(*) AS total_latest,
                            COUNT(*) FILTER (
                                WHERE TRIM(COALESCE(register_flag, '')) = 'A'
                            ) AS cnt_a,
                            COUNT(*) FILTER (
                                WHERE detail_url IS NOT NULL AND TRIM(detail_url) <> ''
                            ) AS cnt_url
                        FROM {SOURCE_LIST_TABLE}
                        WHERE "date_crtr_pnttm" = %s
                        """,
                        (latest_pnttm,),
                    )
                    diag = cur.fetchone()
                    if diag:
                        total_l, cnt_a, cnt_url = int(diag[0] or 0), int(diag[1] or 0), int(diag[2] or 0)
                        if cnt_a == 0:
                            logging.info(
                                "kcar 상세: 최신 적재일(%s) 기준 신규(register_flag=A) 0건 → "
                                "상세 크롤 생략 후 DAG 정상 완료로 진행. "
                                "(최신일 전체 행=%s, detail_url 보유 행=%s)",
                                latest_pnttm,
                                total_l,
                                cnt_url,
                            )
                        else:
                            logging.warning(
                                "kcar 상세 대상 0건: 최신일(%s)에 신규(A)=%s건 있으나 "
                                "detail_url이 있는 신규만 수집하므로 매칭 0건 (detail_url 있음=%s).",
                                latest_pnttm,
                                cnt_a,
                                cnt_url,
                            )
        finally:
            try:
                conn.close()
            except Exception:
                pass

        logging.info("수집 대상: %d건", len(rows))
        if not rows:
            logging.info("수집할 데이터가 없습니다.")
        if not rows and latest_pnttm is None:
            logging.warning(
                "수집 대상 없음: %s 에 date_crtr_pnttm 최신값이 없습니다.",
                SOURCE_LIST_TABLE,
            )
        return rows

    @task
    def summarize_targets(target_rows: list[dict[str, str]]) -> list[dict[str, str]]:
        n = len(target_rows)
        with_url = sum(1 for r in target_rows if str(r.get("detail_url") or "").strip())
        logging.info("kcar detail 준비: 총 %d건, detail_url 있음 %d건", n, with_url)
        if not target_rows:
            logging.info("상세 수집 대상 0건 — 다음 태스크에서 헤더만 CSV 생성 후 정상 완료합니다.")
        return target_rows

    # ── Task 2 : 상세 크롤링 + CSV 저장 ───────────────────────────────────
    @task
    def crawl_and_save_csv(target_rows: list[dict[str, str]]) -> str:
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"kcar_detail_{run_ts}.csv"
        logging.info("kcar detail 출력 파일: %s", csv_path)

        total = len(target_rows)
        collected = 0
        failed = 0
        skipped = 0
        recycle_every = 200
        pg_hook = PostgresHook(postgres_conn_id="car_db_conn")

        if total == 0:
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=DETAIL_CSV_FIELDS)
                w.writeheader()
            logging.info(
                "수집할 데이터가 없습니다. Playwright 생략, 헤더만 기록: %s",
                csv_path,
            )
            return str(csv_path)

        detail_img_dir = _get_detail_img_dir()
        detail_img_dir.mkdir(parents=True, exist_ok=True)
        logging.info(
            "kcar detail 이미지 상위 디렉터리(차량별 …/detail/{product_id}/): %s",
            detail_img_dir.resolve(),
        )

        with sync_playwright() as p:
            launch_args = dict(
                headless=True,
                args=[
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--no-sandbox",
                ],
            )

            def _launch_browser():
                return p.chromium.launch(**launch_args)

            browser = _launch_browser()

            def _new_context_and_page():
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1920, "height": 1080},
                )
                # 리스트(sdag_kcar_crawler)는 install_route_blocking(ctx) 기본(image 차단).
                # 상세 PDP는 lazy·Vue로 썸네일 src 가 네트워크 이후 채워지는 경우가 많아 image 만 허용.
                install_route_blocking(ctx, block_resource_types=("media", "font"))
                pg = ctx.new_page()
                # navigator.webdriver 탐지 회피용 초기 스크립트
                pg.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )
                return ctx, pg

            context, page = _new_context_and_page()

            def _ensure_live_browser_page():
                """SIGTERM 등으로 브라우저/페이지가 죽었을 때만 전체 재기동·컨텍스트 재생성."""
                nonlocal browser, context, page
                br_ok = True
                try:
                    if hasattr(browser, "is_connected"):
                        br_ok = bool(browser.is_connected())
                except Exception:
                    br_ok = False
                if not br_ok:
                    logging.warning("kcar detail: browser 비연결 → 재기동")
                    try:
                        context.close()
                    except Exception:
                        pass
                    try:
                        browser.close()
                    except Exception:
                        pass
                    browser = _launch_browser()
                    context, page = _new_context_and_page()
                    return
                try:
                    if page.is_closed():
                        logging.warning("kcar detail: page 닫힘 → context 재생성")
                        try:
                            context.close()
                        except Exception:
                            pass
                        context, page = _new_context_and_page()
                except Exception:
                    logging.warning("kcar detail: page 상태 확인 실패 → context 재생성")
                    try:
                        context.close()
                    except Exception:
                        pass
                    context, page = _new_context_and_page()

            for idx, row in enumerate(target_rows, 1):
                product_id = str(row.get("product_id") or "").strip()
                detail_url = str(row.get("detail_url") or "").strip()

                if not product_id:
                    skipped += 1
                    logging.warning(
                        "[상세수집실패] [%d/%d] product_id=(비어 있음) detail_url=%s "
                        "reason=product_id 없음 → complete_yn 갱신 불가, 스킵",
                        idx,
                        total,
                        (detail_url[:160] + "…") if len(detail_url) > 160 else detail_url or "(없음)",
                    )
                    continue

                if not detail_url:
                    skipped += 1
                    try:
                        CommonUtil.update_list_complete_yn_for_product_id(
                            pg_hook,
                            list_table=SOURCE_LIST_TABLE,
                            product_id=product_id,
                            value="N",
                            list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT,
                            register_flag_a_only=True,
                        )
                    except Exception:
                        logging.exception(
                            "[%d/%d] complete_yn=N DB 갱신 실패 (detail_url 없음) product_id=%s",
                            idx,
                            total,
                            product_id,
                        )
                    logging.warning(
                        "[상세수집실패] [%d/%d] product_id=%s detail_url=(없음) reason=detail_url 비어 있음 → complete_yn=N",
                        idx,
                        total,
                        product_id,
                    )
                    continue

                _ensure_live_browser_page()

                if idx == 1 or idx % 50 == 0 or idx == total:
                    logging.info(
                        "[%d/%d] 호출 대상 - product_id=%s, detail_url=%s",
                        idx,
                        total,
                        product_id,
                        detail_url,
                    )

                per_detail_dir = detail_img_dir / product_id
                per_detail_dir.mkdir(parents=True, exist_ok=True)

                success = False
                fail_reason: str | None = None
                try:
                    detail_data = _crawl_one(
                        page, idx, product_id, detail_url, per_detail_dir
                    )
                    if detail_data:
                        _save_to_csv_append(csv_path, DETAIL_CSV_FIELDS, detail_data)
                        success = True
                        collected += 1
                    else:
                        fail_reason = (
                            "상세 수집 결과 없음(접속 실패·파싱 오류·필수 필드 미충족)"
                        )
                        failed += 1
                except AirflowException as e:
                    if _is_airflow_shutdown_error(e):
                        logging.warning("kcar detail: Airflow 종료 신호 수신 → 즉시 중단 (%s)", e)
                        raise
                    fail_reason = f"{type(e).__name__}: {e}"
                    failed += 1
                    logging.exception(
                        "[상세수집실패] [%d/%d] product_id=%s detail_url=%s AirflowException",
                        idx,
                        total,
                        product_id,
                        detail_url,
                    )
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    if _is_airflow_shutdown_error(e):
                        raise
                    if _is_playwright_dead_error(e):
                        logging.warning(
                            "[%d/%d] playwright 세션 종료 감지 → browser 복구 후 진행: %s",
                            idx,
                            total,
                            e,
                        )
                        _ensure_live_browser_page()
                        fail_reason = f"playwright 세션 종료: {e}"
                        failed += 1
                    else:
                        fail_reason = f"{type(e).__name__}: {e}"
                        failed += 1
                        logging.exception(
                            "[상세수집실패] [%d/%d] product_id=%s detail_url=%s 예외 발생",
                            idx,
                            total,
                            product_id,
                            detail_url,
                        )
                finally:
                    yn = "Y" if success else "N"
                    try:
                        CommonUtil.update_list_complete_yn_for_product_id(
                            pg_hook,
                            list_table=SOURCE_LIST_TABLE,
                            product_id=product_id,
                            value=yn,
                            list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT,
                            register_flag_a_only=True,
                        )
                    except Exception:
                        logging.exception(
                            "[%d/%d] complete_yn=%s DB 갱신 실패 product_id=%s",
                            idx,
                            total,
                            yn,
                            product_id,
                        )
                    if not success and fail_reason:
                        logging.error(
                            "[상세수집실패] [%d/%d] product_id=%s detail_url=%s reason=%s → complete_yn=N",
                            idx,
                            total,
                            product_id,
                            detail_url,
                            fail_reason,
                        )

                if idx % 100 == 0 or idx == total:
                    logging.info(
                        "kcar detail 진행: processed=%d/%d, collected=%d, failed=%d, skipped=%d",
                        idx,
                        total,
                        collected,
                        failed,
                        skipped,
                    )

                if idx % recycle_every == 0 and idx < total:
                    try:
                        page.close()
                    except Exception:
                        pass
                    try:
                        context.close()
                    except Exception:
                        pass
                    context, page = _new_context_and_page()
                    logging.info("kcar detail browser context 재생성 완료: processed=%d/%d", idx, total)

                time.sleep(0.2)  # 서버 부하 방지(너무 짧게 하면 차단 위험)

            try:
                browser.close()
            except Exception:
                pass

        logging.info(
            "kcar detail ✅ 완료: collected=%d, failed=%d, skipped=%d, total=%d → %s",
            collected,
            failed,
            skipped,
            total,
            csv_path,
        )
        if not csv_path.exists():
            raise FileNotFoundError(f"kcar detail CSV 생성 실패(경로/권한 확인): {csv_path}")
        return str(csv_path)

    # ── Task 3 : CSV -> ODS 적재 ──────────────────────────────────────────
    @task
    def load_detail_csv_to_ods(csv_path: str) -> dict[str, Any]:
        """
        crawl_and_save_csv 결과 CSV를 ods.ods_car_detail_kcar 로 적재.
        신규 0건이면 INSERT 생략 후 원천 List complete_yn 만 동기화(register_flag 미변경).
        """
        p = Path(str(csv_path or ""))
        if not p.is_file():
            raise FileNotFoundError(f"kcar detail 적재 대상 CSV가 없습니다: {p}")

        rows = _read_csv_rows(p)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        if not rows:
            logging.info(
                "kcar 상세 DAG 정상 완료: 신규(register_flag=A) 차량 없음 → detail INSERT 생략, "
                "원천 List complete_yn(Y/N)만 최신 스냅샷 기준으로 동기화합니다. csv=%s",
                p,
            )
            CommonUtil.refresh_car_list_complete_flag_vs_detail_ods(
                hook,
                list_table=SOURCE_LIST_TABLE,
                detail_table=TARGET_DETAIL_TABLE,
                list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT,
            )
            table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
            return {
                "done": True,
                "status": "completed_no_new_cars",
                "message": "신규 없음, 상세 수집 생략, complete_yn 동기화만 수행",
                "target_table": TARGET_DETAIL_TABLE,
                "row_count": 0,
                "table_count": table_count,
                "csv_path": str(p),
                "skipped_insert": True,
            }

        CommonUtil.bulk_insert_detail_ods_rows(
            hook, TARGET_DETAIL_TABLE, rows, truncate=False, allow_only_table_cols=True
        )
        CommonUtil.refresh_car_list_complete_flag_vs_detail_ods(
            hook,
            list_table=SOURCE_LIST_TABLE,
            detail_table=TARGET_DETAIL_TABLE,
            list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT,
        )
        table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
        logging.info(
            "kcar detail CSV 적재 완료: table=%s, inserted_rows=%d, table_count=%d, csv=%s",
            TARGET_DETAIL_TABLE,
            len(rows),
            table_count,
            p,
        )
        return {
            "done": True,
            "target_table": TARGET_DETAIL_TABLE,
            "row_count": len(rows),
            "table_count": table_count,
            "csv_path": str(p),
            "skipped_insert": False,
        }

    @task_group(group_id="prepare_detail_crawl")
    def prepare_detail_crawl():
        rows = fetch_target_urls()
        return summarize_targets(rows)

    @task_group(group_id="crawl_and_persist")
    def crawl_and_persist(target_rows: list[dict[str, str]]):
        return crawl_and_save_csv(target_rows)

    prepared = prepare_detail_crawl()
    csv_path = crawl_and_persist(prepared)
    load_detail_csv_to_ods(csv_path)


dag_object = kcar_detail_crawl()


if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    dag_object.test(
        execution_date=datetime(2026, 4, 2, 8, 0),
        conn_file_path=conn_path,
    )


# ═══════════════════════════════════════════════════════════════════
#  경로 유틸
# ═══════════════════════════════════════════════════════════════════
def _get_output_dir() -> Path:
    try:
        base = Path(Variable.get(FINAL_FILE_PATH_VAR))
    except Exception:
        # 사용자가 요청한 대로 기본 하드코딩 경로(/home/limhayoung/data/crawl)를 제거한다.
        raise ValueError(f"Airflow Variable '{FINAL_FILE_PATH_VAR}' 조회 실패(기본경로 없음)")
    return CommonUtil.build_dated_site_path(base, SITE_NAME, datetime.now())


def _get_detail_img_dir() -> Path:
    """
    K Car 상세 갤러리 상위 디렉터리. 차량별 폴더는 (이 경로)/{product_id}/.

    - Variable 미설정: ~/used_car_crawler/imgs/kcar/detail
    - 값이 …/kcar/detail 로 끝나면 그 경로 그대로 사용
    - 그 외: {변수 루트}/{연도}년/케이카/detail
    """
    default_detail = Path.home() / "used_car_crawler" / "imgs" / "kcar" / "detail"
    try:
        raw_img_root = str(Variable.get(IMAGE_FILE_PATH_VAR)).strip()
    except Exception:
        return default_detail

    if not raw_img_root:
        return default_detail

    p = Path(raw_img_root)
    norm = str(p).replace("\\", "/").rstrip("/").lower()
    if norm.endswith("kcar/detail") or norm.endswith("imgs/kcar/detail"):
        return p

    img_root = Path(raw_img_root)
    year_site = CommonUtil.build_year_site_path(img_root, SITE_NAME, datetime.now())
    return year_site / "detail"


# ═══════════════════════════════════════════════════════════════════
#  CSV 유틸
# ═══════════════════════════════════════════════════════════════════
def _get_now_times() -> tuple[str, str]:
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%Y%m%d%H%M")


def _csv_cell_excel_text(val: Any) -> Any:
    if val is None:
        return ""
    s = (val if isinstance(val, str) else str(val)).strip()
    if s and re.match(r"^\d+-\d+$", s):
        return "'" + s
    return s


def _save_to_csv_append(file_path: Path, fieldnames: list[str], data: dict[str, Any]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = file_path.exists()
    row = {k: _csv_cell_excel_text(v) if (isinstance(v, str) or v is None) else v for k, v in data.items()}
    with open(file_path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _read_csv_rows(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


# ═══════════════════════════════════════════════════════════════════
#  파싱 헬퍼
# ═══════════════════════════════════════════════════════════════════
def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _safe_text(locator) -> str:
    try:
        return _norm_space(locator.first.inner_text() or "")
    except Exception:
        return ""


def _safe_attr(locator, attr: str) -> str:
    try:
        return (locator.first.get_attribute(attr) or "").strip()
    except Exception:
        return ""


def _dismiss_kcar_popups(page) -> None:
    # 상세에서도 간헐적 확인 팝업이 뜨는 경우 방어
    for _ in range(2):
        try:
            btns = page.locator("button:has-text('확인'), button:has-text('동의'), button:has-text('닫기')")
            for i in range(btns.count()):
                b = btns.nth(i)
                if b.is_visible(timeout=1000):
                    try:
                        b.click(timeout=2000)
                        page.wait_for_timeout(250)
                    except Exception:
                        pass
        except Exception:
            pass


def _format_price_lists_li_text(raw: str) -> str:
    # 요구사항 예시: "할부 월 33만원" 형태로 최대한 맞춤
    t = _norm_space(raw)
    # "월33만원" -> "월 33만원"
    t = re.sub(r"(월)(\d+)", r"\1 \2", t)
    t = t.replace(" 만원", "만원").replace("만원", "만원")
    return t


def _join_pipe_parts(parts: list[str], *, sep: str = " | ") -> str:
    out = [p for p in (_norm_space(x) for x in parts) if p]
    return sep.join(out)


def _kcar_detail_content_pages(page):
    """#carContent 와 같은 선상의 detailContent + page(두 클래스) 블록 목록."""
    for sel in (
        "#carContent ~ .detailContent.page",
        ".carInfoContainer .detailContent.page",
        "#__layout .detailContent.page",
        "#app .detailContent.page",
        # class="detailContent page" (공백 구분 두 클래스)
        ".detailContent.page",
    ):
        loc = page.locator(sel)
        try:
            if loc.count() > 0:
                return loc
        except Exception:
            continue
    return page.locator(".detailContent.page")


def _ext_frame_labels_from_scope_eval(scope_locator) -> tuple[str, str]:
    """
    threeDArea 안 #ext / #frame 의 ul.labels li 텍스트.
    셀렉터만으로 잡히지 않는 경우(Vue 동적 id/구조) 대비 evaluate 사용.
    """
    if scope_locator.count() == 0:
        return "", ""
    try:
        res = scope_locator.first.evaluate(
            """el => {
            const norm = (t) => (t || '').replace(/\\s+/g, ' ').trim();
            const pipeUl = (ul) => {
                if (!ul) return '';
                const items = [];
                for (const li of ul.querySelectorAll(':scope > li')) {
                    const t = norm(li.textContent);
                    if (t) items.push(t);
                }
                return items.join(' | ');
            };
            const root = el.querySelector('.threeDArea') || el;
            let extOut = '';
            let frameOut = '';
            const extRoot = root.querySelector('#ext');
            if (extRoot) {
                let ul = extRoot.querySelector('#index ul.labels');
                if (!ul) ul = extRoot.querySelector('ul.labels');
                if (!ul) ul = extRoot.querySelector('.labels');
                extOut = pipeUl(ul);
            }
            if (!extOut) {
                const u2 = root.querySelector('.ext #ext #index ul.labels, .ext #ext ul.labels');
                extOut = pipeUl(u2);
            }
            const frameRoot = root.querySelector('#frame');
            if (frameRoot) {
                let ul = frameRoot.querySelector('#index ul.labels');
                if (!ul) ul = frameRoot.querySelector('ul.labels');
                if (!ul) ul = frameRoot.querySelector('.labels');
                frameOut = pipeUl(ul);
            }
            if (!frameOut) {
                const u3 = root.querySelector('#frame #index ul.labels, #frame ul.labels');
                frameOut = pipeUl(u3);
            }
            return { ext: extOut, frame: frameOut };
        }"""
        )
        if isinstance(res, dict):
            return (str(res.get("ext") or "").strip(), str(res.get("frame") or "").strip())
    except Exception:
        pass
    return "", ""


def _kcar_first_option_detail_box(page):
    """#carContent 에 없을 수 있음 → carInfo 영역·전역 순으로 탐색."""
    for sel in (
        "#carContent .option-detail-box",
        ".carInfoContainer .option-detail-box",
        ".carInfoContent .option-detail-box",
        ".carInfoDetailWrap .option-detail-box",
        ".option-detail-box",
    ):
        loc = page.locator(sel).first
        try:
            if loc.count() > 0:
                return loc
        except Exception:
            continue
    return None


def _kcar_first_option_list_wrap(page):
    """
    main_option 전용: ul.option-list + li.option-list-item 이 실제로 있는 래퍼만 사용.
    페이지에 .option-list-wrap 가 여러 개이거나 첫 노드가 빈 껍데일 수 있음.
    """
    tier_sels = (
        "#carContent .option-list-wrap",
        ".carInfoContainer .option-list-wrap",
        ".carInfoContent .option-list-wrap",
        ".carInfoDetailWrap .option-list-wrap",
        ".option-list-wrap",
    )
    for sel in tier_sels:
        locs = page.locator(sel)
        try:
            for i in range(locs.count()):
                w = locs.nth(i)
                if w.locator("ul.option-list li.option-list-item").count() > 0:
                    return w
        except Exception:
            continue
    return None


def _car_select_option_from_eval(page) -> str:
    """option-detail-box 내 ul 직계 li (p.label / p.value). #carContent 밖 배치도 허용."""
    try:
        raw = page.evaluate(
            """() => {
            const box =
                document.querySelector('#carContent .option-detail-box') ||
                document.querySelector('.carInfoContainer .option-detail-box') ||
                document.querySelector('.carInfoContent .option-detail-box') ||
                document.querySelector('.carInfoDetailWrap .option-detail-box') ||
                document.querySelector('.option-detail-box');
            if (!box) return '';
            const norm = (t) => (t || '').replace(/\\s+/g, ' ').trim();
            const parts = [];
            for (const ul of box.querySelectorAll('ul')) {
                for (const li of ul.querySelectorAll(':scope > li')) {
                    const sp = li.querySelector('p.label span');
                    const pl = li.querySelector('p.label');
                    const pv = li.querySelector('p.value');
                    const label = norm(sp ? sp.textContent : (pl ? pl.textContent : ''));
                    const val = norm(pv ? pv.textContent : '');
                    if (label && val) parts.push(label + ' : ' + val);
                    else if (label) parts.push(label);
                    else if (val) parts.push(val);
                }
            }
            return parts.join(' | ');
        }"""
        )
        return _norm_space(str(raw or ""))
    except Exception:
        return ""


def _kcar_car_select_option_from_detail_box_locator(page) -> str:
    """Playwright로 option-detail-box → ul → 직계 li 만 (ul li 전체 순회로 중첩 li 오인 방지)."""
    box = _kcar_first_option_detail_box(page)
    if box is None:
        return ""
    parts: list[str] = []
    try:
        uls = box.locator("ul")
        for ui in range(uls.count()):
            ul = uls.nth(ui)
            lis = ul.locator("> li")
            n = lis.count()
            if n == 0:
                continue
            for i in range(n):
                li = lis.nth(i)
                label = _safe_text(li.locator("p.label span").first) or _safe_text(li.locator("p.label").first)
                val = _safe_text(li.locator("p.value").first)
                label2 = _norm_space(label)
                val2 = _norm_space(val)
                if label2 and val2:
                    parts.append(f"{label2} : {val2}")
                elif label2:
                    parts.append(label2)
                elif val2:
                    parts.append(val2)
    except Exception:
        return ""
    return _join_pipe_parts(parts)


def _main_option_captions_from_eval(page) -> str:
    """option-list-wrap > ul.option-list > li.option-list-item.nth-N.active > p.caption (main_option 만 있는 화면)."""
    try:
        raw = page.evaluate(
            """() => {
            const norm = (t) => (t || '').replace(/\\s+/g, ' ').trim();
            const wraps = Array.from(document.querySelectorAll('.option-list-wrap'));
            let list = null;
            for (const wrap of wraps) {
                const ul = wrap.querySelector('ul.option-list');
                if (!ul) continue;
                const hasAct = ul.querySelector('li.option-list-item.active, li.option-list-item.is-active');
                const hasItem = ul.querySelector('li.option-list-item');
                if (hasAct) { list = ul; break; }
                if (hasItem && !list) list = ul;
            }
            if (!list) return '';
            const isOn = (li) => {
                const c = li.className || '';
                const ar = li.getAttribute('aria-selected');
                return (
                    /\\bactive\\b/.test(c) ||
                    /\\bis-active\\b/.test(c) ||
                    /\\bon\\b/.test(c) ||
                    ar === 'true'
                );
            };
            const isItem = (li) => /\\boption-list-item\\b/.test(li.className || '');
            let items = Array.from(list.querySelectorAll('li.option-list-item.active, li.option-list-item.is-active'));
            if (items.length === 0) {
                items = Array.from(list.querySelectorAll('li')).filter((li) => isItem(li) && isOn(li));
            }
            const out = [];
            const seen = new Set();
            for (const li of items) {
                const cap = li.querySelector('p.caption') || li.querySelector('.caption');
                const t = norm(cap ? cap.textContent : '');
                if (t && !seen.has(t)) { seen.add(t); out.push(t); }
            }
            return out.join(' | ');
        }"""
        )
        return _norm_space(str(raw or ""))
    except Exception:
        return ""


def _kcar_main_option_from_list_wrap_locator(page) -> str:
    """ul.option-list > li.option-list-item.active p.caption (nth-N active 다건)."""
    wrap = _kcar_first_option_list_wrap(page)
    if wrap is None:
        return ""
    caps = wrap.locator(
        "ul.option-list > li.option-list-item.active > p.caption, "
        "ul.option-list > li.option-list-item.is-active > p.caption"
    )
    if caps.count() == 0:
        caps = wrap.locator("ul.option-list li.option-list-item.active p.caption")
    if caps.count() == 0:
        caps = wrap.locator(".option-list li[class*='option-list-item'][class*='active'] p.caption")
    if caps.count() == 0:
        caps = wrap.locator(".option-list li[class*='option-list-item'][class*='is-active'] p.caption")
    parts: list[str] = []
    try:
        for i in range(caps.count()):
            t = _norm_space(caps.nth(i).inner_text() or "")
            if t and t not in parts:
                parts.append(t)
    except Exception:
        return ""
    return _join_pipe_parts(parts)


def _labels_ul_to_pipe(labels_root) -> str:
    """ul.labels > li 텍스트를 ' | '로 연결."""
    try:
        lis = labels_root.locator("li")
        parts: list[str] = []
        for i in range(lis.count()):
            t = _norm_space(lis.nth(i).inner_text() or "")
            if t:
                parts.append(t)
        return _join_pipe_parts(parts)
    except Exception:
        return ""


def _join_kv_ul(ul_locator) -> str:
    """ul > li 각각 p.label / p.value → '키 : 값' 파이프 조인."""
    parts: list[str] = []
    try:
        lis = ul_locator.locator("> li")
        n = lis.count()
        if n == 0:
            lis = ul_locator.locator("li")
            n = lis.count()
        for i in range(n):
            li = lis.nth(i)
            k = _safe_text(li.locator("p.label").first)
            v = _norm_space(li.locator("p.value").first.inner_text() or "")
            if not v:
                v = _safe_text(li.locator("p.value").first)
            if k and v:
                parts.append(f"{k} : {v}")
            elif k:
                parts.append(k)
            elif v:
                parts.append(v)
    except Exception:
        pass
    return _join_pipe_parts(parts)


def _parse_detail_history_section(page_locator) -> str:
    """
    .detail-history-wrap .datail-history-box(오타) 또는 .detail-history-box ul > li.item-row
    """
    box = page_locator.locator(".detail-history-wrap .datail-history-box, .detail-history-wrap .detail-history-box").first
    if box.count() == 0:
        return ""
    ul = box.locator("ul").first
    if ul.count() == 0:
        return ""
    parts: list[str] = []
    try:
        rows = ul.locator("> li.item-row")
        if rows.count() == 0:
            rows = ul.locator("li.item-row")
        for i in range(rows.count()):
            row = rows.nth(i)
            label = _safe_text(row.locator("p.label").first)
            vbox = row.locator("ul.value-box")
            if vbox.count() > 0:
                vparts: list[str] = []
                for j in range(vbox.locator("li").count()):
                    vt = _norm_space(vbox.locator("li").nth(j).inner_text() or "")
                    if vt:
                        vparts.append(vt)
                val = _join_pipe_parts(vparts, sep=" ") if vparts else ""
            else:
                val = _norm_space(row.locator("p.value").first.inner_text() or "")
            if not val:
                val = _safe_text(row.locator("p.value").first)
            if label and val:
                parts.append(f"{label} : {val}")
            elif label:
                parts.append(label)
            elif val:
                parts.append(val)
    except Exception:
        pass
    return _join_pipe_parts(parts)


def _parse_owner_detail_section(page_locator) -> str:
    """.detail-owner-wrap .detail-owner-box ul > li.item-col p.label"""
    box = page_locator.locator(".detail-owner-wrap .detail-owner-box").first
    if box.count() == 0:
        return ""
    parts: list[str] = []
    try:
        cols = box.locator("ul li.item-col")
        for i in range(cols.count()):
            t = _norm_space(cols.nth(i).locator("p.label").first.inner_text() or "")
            if t:
                parts.append(t)
    except Exception:
        pass
    return _join_pipe_parts(parts)


def _parse_detail_warranty_section(page_locator) -> str:
    """.detail-warranty-wrap .detail-warranty-box ul li.item-row > div p.label / p.value"""
    box = page_locator.locator(".detail-warranty-wrap .detail-warranty-box").first
    if box.count() == 0:
        return ""
    parts: list[str] = []
    try:
        rows = box.locator("ul li.item-row")
        for i in range(rows.count()):
            row = rows.nth(i)
            div = row.locator("div").first
            if div.count() == 0:
                continue
            k = _safe_text(div.locator("p.label").first)
            v = _norm_space(div.locator("p.value").first.inner_text() or "")
            if not v:
                v = _safe_text(div.locator("p.value").first)
            if k and v:
                parts.append(f"{k} : {v}")
            elif k:
                parts.append(k)
            elif v:
                parts.append(v)
    except Exception:
        pass
    return _join_pipe_parts(parts)


def _download_detail_image(page, img_src: str, save_path: Path, *, referer: str) -> bool:
    """
    리스트 sdag_kcar_crawler._download_list_image 과 동일 패턴: Playwright APIRequestContext 로 저장.
    """
    if not images_enabled():
        return False
    if not img_src or not isinstance(img_src, str):
        return False
    if img_src.startswith("data:"):
        return False
    if not img_src.startswith("http"):
        return False
    ref = (referer or "").split("#")[0] or "https://www.kcar.com/"
    headers = {
        "Referer": ref,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }
    try:
        response = page.request.get(img_src, timeout=20_000, headers=headers)
        if response.ok and response.body():
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_bytes(response.body())
            return True
    except Exception as e:
        logging.getLogger(__name__).debug("kcar detail 이미지 다운로드 실패 %s: %s", img_src[:100], e)
    return False


def _norm_img_src(raw: str, page_url: str) -> str:
    src = (raw or "").strip()
    if not src or src.startswith("data:"):
        return ""
    if src.startswith("//"):
        return "https:" + src
    if src.startswith("/"):
        return urljoin(page_url, src)
    if not src.startswith("http"):
        return urljoin(page_url, src)
    return src


def _collect_kcar_pdp_thumbnail_urls(page) -> list[str]:
    """
    PDP 썸네일: body → #__nuxt → … → .el-carousel-thumbnail-item[data-thumbnail-item-index]
    트리 안의 img/picture 속성만 읽음(data-src·src·srcset, picture/source).
    lazy-load 스크롤/대기·currentSrc·computed background-image 는 쓰지 않음.
    """
    page_url = page.url or "https://www.kcar.com/"
    try:
        raw = page.evaluate(
            """() => {
            const uniqNodes = (arr) => {
                const seen = new Set();
                const out = [];
                for (const n of arr) {
                    if (n && !seen.has(n)) { seen.add(n); out.push(n); }
                }
                return out;
            };
            const qFullType1 =
              'body #__nuxt #__layout .Container .carInfoWrap.carInfoType1 .carInfoContainer ' +
              '.carInfoContent .carInfoKeyArea .carInfoGallery .pdp-cover-slider-container ' +
              '.el-carousel-thumbnail .el-carousel-thumbnail-scroll.el-scrollbar ' +
              '.el-scrollbar__wrap .el-scrollbar__view .el-carousel-thumbnail-wrap ' +
              '.el-carousel-thumbnail-item[data-thumbnail-item-index]';
            const qFullWrap =
              'body #__nuxt #__layout .Container .carInfoWrap .carInfoContainer ' +
              '.carInfoContent .carInfoKeyArea .carInfoGallery .pdp-cover-slider-container ' +
              '.el-carousel-thumbnail .el-carousel-thumbnail-scroll.el-scrollbar ' +
              '.el-scrollbar__wrap .el-scrollbar__view .el-carousel-thumbnail-wrap ' +
              '.el-carousel-thumbnail-item[data-thumbnail-item-index]';
            const qScrollLoose =
              'body #__nuxt #__layout .Container .carInfoWrap .carInfoContainer ' +
              '.carInfoContent .carInfoKeyArea .carInfoGallery .pdp-cover-slider-container ' +
              '.el-carousel-thumbnail .el-carousel-thumbnail-scroll ' +
              '.el-scrollbar__wrap .el-scrollbar__view .el-carousel-thumbnail-wrap ' +
              '.el-carousel-thumbnail-item[data-thumbnail-item-index]';
            const qGallery =
              'body .carInfoGallery .pdp-cover-slider-container .el-carousel-thumbnail ' +
              '.el-carousel-thumbnail-scroll.el-scrollbar .el-scrollbar__wrap .el-scrollbar__view ' +
              '.el-carousel-thumbnail-wrap .el-carousel-thumbnail-item[data-thumbnail-item-index]';
            const qGalleryShort =
              'body .carInfoGallery .pdp-cover-slider-container .el-carousel-thumbnail ' +
              '.el-scrollbar__wrap .el-scrollbar__view .el-carousel-thumbnail-wrap ' +
              '.el-carousel-thumbnail-item[data-thumbnail-item-index]';
            const qLoose = 'body .carInfoGallery .el-carousel-thumbnail-item[data-thumbnail-item-index]';
            let nodes = uniqNodes([
                ...document.querySelectorAll(qFullType1),
                ...document.querySelectorAll(qFullWrap),
                ...document.querySelectorAll(qScrollLoose),
                ...document.querySelectorAll(qGallery),
                ...document.querySelectorAll(qGalleryShort),
                ...document.querySelectorAll(qLoose),
            ]);
            nodes.sort((a, b) => {
                const ia = parseInt(a.getAttribute('data-thumbnail-item-index') || '0', 10);
                const ib = parseInt(b.getAttribute('data-thumbnail-item-index') || '0', 10);
                return ia - ib;
            });
            const pickImgUrl = (img) => {
                if (!img) return '';
                const a = (img.getAttribute('data-src') || img.getAttribute('data-lazy-src') ||
                    img.getAttribute('data-original') || img.getAttribute('data-url') || '').trim();
                if (a) return a;
                const ss = (img.getAttribute('srcset') || '').trim();
                if (ss) {
                    const first = ss.split(',')[0];
                    const u = (first || '').trim().split(/\\s+/)[0];
                    if (u) return u;
                }
                const s = (img.getAttribute('src') || '').trim();
                return (s && !s.startsWith('data:')) ? s : '';
            };
            const pickFromItem = (el) => {
                const pic = el.querySelector('picture');
                if (pic) {
                    const srcEl = pic.querySelector('source[srcset], source[src]');
                    if (srcEl) {
                        const ss = (srcEl.getAttribute('srcset') || srcEl.getAttribute('src') || '').trim();
                        if (ss) {
                            const first = ss.split(',')[0];
                            const u = (first || '').trim().split(/\\s+/)[0];
                            if (u) return u;
                        }
                    }
                    const pim = pic.querySelector('img');
                    const u = pickImgUrl(pim);
                    if (u) return u;
                }
                const img = el.querySelector('img');
                return pickImgUrl(img);
            };
            const out = [];
            for (const el of nodes) {
                const src = pickFromItem(el);
                if (src) out.push(src);
            }
            return out;
        }"""
        )
    except Exception:
        raw = []

    if not isinstance(raw, list):
        return []

    urls: list[str] = []
    seen: set[str] = set()
    for s in raw:
        src2 = _norm_img_src(str(s or ""), page_url)
        if src2 and src2 not in seen:
            seen.add(src2)
            urls.append(src2)
    return urls


def _collect_kcar_slide_index_button_bg_urls(page) -> list[str]:
    """
    메인 갤러리가 <button data-slide-index="N" style="background-image: url(...);"> 인 경우.
    인라인 style 만 파싱(HTML 엔티티 &quot; 대응). data-slide-index 오름차순.
    """
    page_url = page.url or "https://www.kcar.com/"
    try:
        raw = page.evaluate(
            """() => {
            const scopes = [
                '.carInfoGallery button[data-slide-index]',
                '.pdp-cover-slider-container button[data-slide-index]',
                '#carContent .carInfoGallery button[data-slide-index]',
                '#carContent button[data-slide-index]',
            ];
            const seenEl = new Set();
            const btns = [];
            for (const sel of scopes) {
                document.querySelectorAll(sel).forEach((b) => {
                    if (b && !seenEl.has(b)) {
                        seenEl.add(b);
                        btns.push(b);
                    }
                });
            }
            btns.sort((a, b) => {
                const ia = parseInt(a.getAttribute('data-slide-index') || '0', 10);
                const ib = parseInt(b.getAttribute('data-slide-index') || '0', 10);
                return ia - ib;
            });
            const parseBgUrl = (style) => {
                if (!style) return '';
                let s = String(style).replace(/&quot;/g, '"').replace(/&#34;/g, '"');
                const m = /url\\s*\\(\\s*["']?([^"')]+)["']?\\s*\\)/i.exec(s);
                if (!m) return '';
                return (m[1] || '').trim().replace(/^["']|["']$/g, '');
            };
            const out = [];
            const seenU = new Set();
            for (const b of btns) {
                const u = parseBgUrl(b.getAttribute('style') || '');
                if (u && !u.startsWith('data:') && !seenU.has(u)) {
                    seenU.add(u);
                    out.push(u);
                }
            }
            return out;
        }"""
        )
    except Exception:
        raw = []

    if not isinstance(raw, list):
        return []

    urls: list[str] = []
    seen: set[str] = set()
    for s in raw:
        src2 = _norm_img_src(str(s or ""), page_url)
        if src2 and src2 not in seen:
            seen.add(src2)
            urls.append(src2)
    return urls


def _collect_kcar_gallery_image_urls(page) -> list[str]:
    """
    갤러리 이미지 수집(다운로드는 _crawl_one에서 {product_id}_1.png … 로 저장).
    lazy-load 유도 없이 DOM 속성·인라인 style 만 읽음.
    1) el-carousel 썸네일(data-thumbnail-item-index) — PDP 기본 트리 우선
    2) button[data-slide-index] + background-image:url(...)
    3) 구형 kaps-slider / kaps-item img
    """
    urls = _collect_kcar_pdp_thumbnail_urls(page)
    if urls:
        return urls

    urls = _collect_kcar_slide_index_button_bg_urls(page)
    if urls:
        return urls

    page_url = page.url or "https://www.kcar.com/"
    seen: set[str] = set()

    def _append_from_imgs(imgs_locator) -> None:
        for i in range(imgs_locator.count()):
            img = imgs_locator.nth(i)
            src = (img.get_attribute("data-src") or img.get_attribute("src") or "").strip()
            src2 = _norm_img_src(src, page_url)
            if src2 and src2 not in seen:
                seen.add(src2)
                urls.append(src2)

    root_legacy = page.locator(".carInfoGallery.pdB60").first
    if root_legacy.count() == 0:
        root_legacy = page.locator(".carInfoGallery").first
    if root_legacy.count() == 0:
        return urls

    # 구형 kaps 슬라이더
    for sel in (
        ".kcar-viewer-box .kaps-slider .kaps-slider-content .kaps-item div img",
        ".kcar-viewer-box .kaps-slider-content .kaps-item div img",
        ".kaps-slider-content .kaps-item div img",
    ):
        loc = root_legacy.locator(sel)
        if loc.count() > 0:
            _append_from_imgs(loc)
            break

    if not urls:
        kaps_items = root_legacy.locator(".kaps-item")
        for i in range(kaps_items.count()):
            item = kaps_items.nth(i)
            img = item.locator("div img").first
            if img.count() == 0:
                img = item.locator("img").first
            if img.count() == 0:
                continue
            src = (img.get_attribute("data-src") or img.get_attribute("src") or "").strip()
            if not src:
                continue
            src2 = _norm_img_src(src, page_url)
            if src2 and src2 not in seen:
                seen.add(src2)
                urls.append(src2)

    if not urls:
        _append_from_imgs(root_legacy.locator("img"))

    return urls


def _crawl_one(page, idx: int, product_id: str, detail_url: str, detail_img_dir: Path) -> dict[str, Any] | None:
    d_pnttm, c_dt = _get_now_times()
    data: dict[str, Any] = {f: "" for f in DETAIL_CSV_FIELDS}
    data["model_sn"] = idx
    data["product_id"] = product_id
    data["car_imgs"] = str(detail_img_dir.resolve())
    data["date_crtr_pnttm"] = d_pnttm
    data["create_dt"] = c_dt

    for attempt in range(3):
        try:
            goto_with_retry(
                page,
                GotoSpec(
                    detail_url,
                    wait_until="commit",
                    timeout_ms=90_000,
                    ready_selectors=("#carContent", ".carInfoKeyArea", "body"),
                    ready_timeout_ms=25_000,
                ),
                logger=logging.getLogger(__name__),
                attempts=1,
            )
            page.wait_for_timeout(800)
            _dismiss_kcar_popups(page)
            break
        except Exception as e:
            if _is_playwright_dead_error(e):
                # Airflow SIGTERM 등으로 브라우저가 이미 죽었으면 재시도 무의미
                logging.warning("kcar detail 접속 중단(세션 종료): %s - %s", product_id, e)
                return None
            if attempt < 2:
                logging.warning("kcar detail 재시도 (%d/3): %s - %s", attempt + 2, product_id, e)
                time.sleep(2)
            else:
                logging.error("kcar detail 접속 실패: %s - %s", product_id, e)
                return None

    try:
        # ── Key Area : car_name / car_num / dotLists mapping ───────────
        key_area = page.locator(".carInfoKeyArea").first
        if key_area.count() == 0:
            return None

        data["car_name"] = _safe_text(key_area.locator(".carNameWrap.drct .carName").first) or _safe_text(
            key_area.locator(".carName").first
        )
        data["car_num"] = _safe_text(key_area.locator(".carNameWrap.drct .left .carNum").first) or _safe_text(
            key_area.locator(".carNum").first
        )

        dot_lists = key_area.locator(".dotLists li")
        dot_cols = ["accident", "year", "km", "fuel", "car_color", "trans", "car_price"]
        for i, col in enumerate(dot_cols):
            if dot_lists.count() >= i + 1:
                li = dot_lists.nth(i)
                data[col] = _norm_space(li.inner_text() or "")

        data["user_name"] = _safe_text(key_area.locator(".consultGuide.mT40 .userName").first) or _safe_text(
            page.locator(".consultGuide.mT40 .userName").first
        )
        data["call_guide"] = _safe_text(key_area.locator(".callGuide").first) or _safe_text(
            page.locator(".callGuide").first
        )

        # ── price_lists ────────────────────────────────────────────────
        price_list_lis = page.locator(
            ".carInfoDetailWrap .carInfoLeft.sumSummaryWrap .carPriceView .priceLists li"
        )
        price_parts: list[str] = []
        for i in range(price_list_lis.count()):
            li = price_list_lis.nth(i)
            try:
                formatted = li.evaluate(
                    """el => {
                        // li 자식구조: '할부 ' + <span>월<em class="pointC">33</em>만원</span> + button ...
                        const textNodes = [];
                        for (const node of el.childNodes) {
                            if (node.nodeType === Node.TEXT_NODE) {
                                const t = (node.textContent || '').trim();
                                if (t) textNodes.push(t);
                            }
                        }
                        const prefix = textNodes.join(' ');
                        const span = el.querySelector('span');
                        const spanText = span ? (span.textContent || '').trim() : '';
                        let s = prefix ? (prefix + ' ' + spanText) : spanText;
                        s = (s || '').replace(/\\s+/g, ' ').trim();
                        s = s.replace(/월(\\d+)/, '월 $1');
                        // '  만원'류 정리
                        s = s.replace(/\\s*만원/, '만원');
                        return s;
                    }"""
                )
                formatted2 = _format_price_lists_li_text(str(formatted or ""))
                if formatted2:
                    price_parts.append(formatted2)
            except Exception:
                raw = _norm_space(li.inner_text() or "")
                formatted2 = _format_price_lists_li_text(raw)
                if formatted2:
                    price_parts.append(formatted2)
        if price_parts:
            data["price_lists"] = " | ".join(price_parts)

        # ── car_select_option / main_option ───────────────────────────
        # 옵션 블록은 #carContent 밖(.carInfoContainer 등)에 있을 수 있어 범위 확장.
        # Vue hydration 전이면 비어 있을 수 있어 스크롤·짧은 대기.
        try:
            hint = page.locator(
                "#carContent .option-detail-box, #carContent .option-list-wrap, "
                ".carInfoContainer .option-detail-box, .carInfoContainer .option-list-wrap"
            ).first
            if hint.count() > 0:
                hint.scroll_into_view_if_needed(timeout=5000)
        except Exception:
            pass
        page.wait_for_timeout(500)

        has_detail_box = _kcar_first_option_detail_box(page) is not None
        has_list_wrap = _kcar_first_option_list_wrap(page) is not None

        if has_detail_box:
            cs = _kcar_car_select_option_from_detail_box_locator(page)
            if not cs:
                cs = _car_select_option_from_eval(page)
            if cs:
                data["car_select_option"] = cs

        if has_list_wrap:
            mo = _kcar_main_option_from_list_wrap_locator(page)
            if not mo:
                mo = _main_option_captions_from_eval(page)
            if mo:
                data["main_option"] = mo

        # ── .detailContent.page N번째: 외판/프레임, 진단, 이력, 보증 ────
        detail_pages = _kcar_detail_content_pages(page)
        if detail_pages.count() >= 1:
            p0 = detail_pages.nth(0)
            for ext_sel, col in (
                ("#ext #index ul.labels", "ext_panel"),
                ("#ext #index .labels", "ext_panel"),
            ):
                if str(data.get("ext_panel") or "").strip():
                    break
                u = p0.locator(ext_sel).first
                if u.count() > 0:
                    data[col] = _labels_ul_to_pipe(u)
            if not str(data.get("ext_panel") or "").strip():
                u2 = p0.locator(".threeDArea #ext #index ul.labels").first
                if u2.count() > 0:
                    data["ext_panel"] = _labels_ul_to_pipe(u2)

            for fr_sel, col in (
                ("#frame #index ul.labels", "frame"),
                ("#frame #index .labels", "frame"),
            ):
                if str(data.get("frame") or "").strip():
                    break
                u = p0.locator(fr_sel).first
                if u.count() > 0:
                    data[col] = _labels_ul_to_pipe(u)
            if not str(data.get("frame") or "").strip():
                u2 = p0.locator(".threeDArea #frame #index ul.labels").first
                if u2.count() > 0:
                    data["frame"] = _labels_ul_to_pipe(u2)

            # #ext / #frame 라벨은 캔버스·Vue 구조로 locator가 비는 경우가 있어 evaluate·다중 블록 순회
            if not str(data.get("ext_panel") or "").strip() or not str(data.get("frame") or "").strip():
                n_dp = detail_pages.count()
                for di in range(min(n_dp, 8)):
                    pi = detail_pages.nth(di)
                    try:
                        ta = pi.locator(".threeDArea").first
                        if ta.count() > 0:
                            ta.scroll_into_view_if_needed(timeout=5000)
                    except Exception:
                        pass
                    ex_ev, fr_ev = _ext_frame_labels_from_scope_eval(pi)
                    if ex_ev and not str(data.get("ext_panel") or "").strip():
                        data["ext_panel"] = ex_ev
                    if fr_ev and not str(data.get("frame") or "").strip():
                        data["frame"] = fr_ev
                    if str(data.get("ext_panel") or "").strip() and str(data.get("frame") or "").strip():
                        break

            drb = p0.locator(".detail-result-box").first
            if drb.count() > 0:
                wrap = drb.locator(".item-wrap").first
                rows = wrap.locator(".item-row") if wrap.count() > 0 else drb.locator(".item-row")
                if rows.count() >= 1:
                    r0 = rows.nth(0)
                    t0 = _safe_text(r0.locator(".title").first)
                    v0 = _norm_space(r0.locator(".value").first.inner_text() or "")
                    if not v0:
                        v0 = _safe_text(r0.locator(".value").first)
                    if t0 and v0:
                        data["accident"] = f"{t0} : {v0}"
                    elif v0:
                        data["accident"] = v0
                    elif t0:
                        data["accident"] = t0
                if rows.count() >= 2:
                    ul1 = rows.nth(1).locator("ul").first
                    if ul1.count() > 0:
                        data["diagnosis_pass"] = _join_kv_ul(ul1)
                if rows.count() >= 3:
                    ul2 = rows.nth(2).locator("ul").first
                    if ul2.count() > 0:
                        data["notice"] = _join_kv_ul(ul2)

        if detail_pages.count() >= 2:
            p1 = detail_pages.nth(1)
            dh = _parse_detail_history_section(p1)
            if dh:
                data["detail_history"] = dh
            od = _parse_owner_detail_section(p1)
            if od:
                data["owner_detail"] = od

        if detail_pages.count() >= 3:
            p2 = detail_pages.nth(2)
            dw = _parse_detail_warranty_section(p2)
            if dw:
                data["detail_warranty"] = dw

        # ── 갤러리 이미지 다운로드 (USED_CAR_SKIP_IMAGES=1 이면 스킵) ─────
        try:
            urls = _collect_kcar_gallery_image_urls(page)
            if not urls:
                if images_enabled():
                    logging.warning(
                        "kcar detail 이미지 URL 없음: product_id=%s "
                        "(button[data-slide-index]·썸네일 셀렉터·USED_CAR_SKIP_IMAGES=%s)",
                        product_id,
                        os.environ.get("USED_CAR_SKIP_IMAGES", "0"),
                    )
            if urls:
                referer = (page.url or "").split("#")[0] or "https://www.kcar.com/"
                ok = 0
                for img_idx, img_url in enumerate(urls, 1):
                    out = detail_img_dir / f"{product_id}_{img_idx}.png"
                    if _download_detail_image(page, img_url, out, referer=referer):
                        ok += 1
                if ok == 0 and images_enabled():
                    logging.warning(
                        "kcar detail 이미지 저장 0건: product_id=%s (HTTP/URL/권한 확인)",
                        product_id,
                    )
        except Exception:
            pass

    except Exception as e:
        if _is_playwright_dead_error(e):
            logging.warning("kcar detail 파싱 중단(세션 종료): %s - %s", product_id, e)
            return None
        logging.error("kcar detail 파싱 전체 오류: %s - %s", product_id, e)
        return None

    # 최소값 방어: car_name + car_num 중 하나 이상 채워지면 OK
    core_cols = ("car_name", "car_num", "car_price")
    if sum(1 for c in core_cols if str(data.get(c) or "").strip()) == 0:
        return None
    return data

