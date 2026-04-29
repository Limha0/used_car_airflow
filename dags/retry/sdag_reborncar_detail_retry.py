from __future__ import annotations

import csv
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from util.common_util import CommonUtil
from util.playwright_util import GotoSpec, goto_with_retry, install_route_blocking


# ═══════════════════════════════════════════════════════════════════
#  상수 (리본카 상세 재수집)
# ═══════════════════════════════════════════════════════════════════

SOURCE_LIST_TABLE = "ods.ods_car_list_reborncar"
TARGET_DETAIL_TABLE = "ods.ods_car_detail_reborncar"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"
SITE_NAME = "리본카"

DETAIL_CSV_FIELDS = [
    "model_sn",
    "product_id",
    "car_name",
    "car_num",
    "release_dt",
    "car_navi",
    "grear_box",
    "car_color",
    "car_fuel",
    "car_seat",
    "plan_pay",
    "car_new_price",
    "aci_gbn",
    "info_tit_1",
    "special_carhistory",
    "relamt_per_parent",
    "smell_grade",
    "info_tit_2",
    "vip_option",
    "add_option",
    "figure_panel",
    "figure_frame",
    "aqi_list",
    "aqi_notice_list",
    "tire_summery_front_left",
    "tire_summery_front_right",
    "tire_summery_back_left",
    "tire_summery_back_right",
    "battey_count",
    "brand_surety_con_1",
    "brand_surety_con_2",
    "car_imgs",
    "date_crtr_pnttm",
    "create_dt",
]


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의 (재수집)
# ═══════════════════════════════════════════════════════════════════


@dag(
    dag_id="sdag_reborncar_detail_retry",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,    # 체인 사이클 중첩 방지
    tags=["used_car", "reborncar", "detail", "retry"],
)
def reborncar_detail_retry():
    """
    리본카 상세 재수집:
    - complete_yn != 'Y' (NULL 포함)
    - register_flag != 'N' (NULL 포함)
    - detail_url 존재(널/공백 제외)
    대상 행을 다시 상세 수집하여 CSV를 생성하고, 수집 성공/실패에 따라 list complete_yn을 단건 갱신한다.
    """

    @task
    def fetch_retry_targets() -> list[dict[str, str]]:
        # C안(시간 윈도우): list 수집 후 3일 지난 "난치성" 실패 건은 자동 drop.
        # date_crtr_pnttm 은 YYYYMMDD 문자열이라 문자열 비교로 충분 (사전식 정렬 = 날짜 정렬).
        sql = f"""
        SELECT
            l.product_id,
            l.detail_url,
            l.register_flag,
            l.complete_yn
        FROM {SOURCE_LIST_TABLE} l
        WHERE (l.complete_yn IS NULL OR TRIM(COALESCE(l.complete_yn::text, '')) <> 'Y')
          AND (l.register_flag IS NULL OR TRIM(COALESCE(l.register_flag::text, '')) <> 'N')
          AND l.detail_url IS NOT NULL
          AND TRIM(COALESCE(l.detail_url::text, '')) <> ''
          AND l.date_crtr_pnttm IS NOT NULL
          AND l.date_crtr_pnttm >= to_char(CURRENT_DATE - INTERVAL '3 days', 'YYYYMMDD')
        ORDER BY l.model_sn
        """
        logging.info("reborncar detail retry select_stmt ::: %s", sql)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        conn = hook.get_conn()
        rows: list[dict[str, str]] = []
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall() or []:
                    rows.append(dict(zip(cols, row)))
        finally:
            try:
                conn.close()
            except Exception:
                pass

        logging.info("reborncar detail retry 재수집 대상: %d건", len(rows))
        if not rows:
            logging.info("재수집 대상 0건 — 정상 종료로 진행합니다.")
        return rows

    @task
    def summarize_targets(target_rows: list[dict[str, str]]) -> list[dict[str, str]]:
        n = len(target_rows)
        with_url = sum(1 for r in target_rows if str(r.get("detail_url") or "").strip())
        logging.info("재수집 대상 총 건수: %d", n)
        logging.info("재수집 준비: 대상=%d건(재수집 필요), detail_url 있음 %d건", n, with_url)
        return target_rows

    @task
    def crawl_and_save_csv(target_rows: list[dict[str, str]]) -> str:
        # 원본 리본카는 모듈 최상단 import가 있으나, retry는 runtime import로 통일
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"reborncar_detail_retry_{run_ts}.csv"
        logging.info("reborncar detail retry 출력 CSV: %s", csv_path.resolve())

        detail_base = _get_detail_img_dir()
        detail_base.mkdir(parents=True, exist_ok=True)

        total = len(target_rows)
        # 0건이든, total>0 이라도 전부 실패할 수 있으니 루프 진입 전에 헤더 CSV 를 미리 만들어둔다.
        # 그래야 수집 성공 0건이어도 후속 load 태스크가 빈 CSV 를 정상적으로 처리함.
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=DETAIL_CSV_FIELDS).writeheader()
        if total == 0:
            return str(csv_path)

        # 중간 진행 로그: 1건째 반드시 출력 + 이후 N건 간격(누적 수집 성공 건수 확인용)
        if total >= 200:
            log_every = 40
        elif total >= 100:
            log_every = 20
        elif total > 50:
            log_every = 10
        elif total > 10:
            log_every = 5
        else:
            log_every = 1

        collected = 0
        failed = 0
        skipped = 0
        ghosted = 0  # 유령 차량(register_flag='N' 영구 차단) 건수
        recycle_every = 300
        pg_hook = PostgresHook(postgres_conn_id="car_db_conn")
        list_cols_for_complete_yn = set(
            CommonUtil.get_ods_table_columns(pg_hook, SOURCE_LIST_TABLE)
        )
        logging.info("reborncar detail retry 재수집 처리 시작 — 총 건수: %d", total)

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
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )
            install_route_blocking(context, block_resource_types=("media", "font"))
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            for idx, row in enumerate(target_rows, 1):
                product_id = str(row.get("product_id") or "").strip()
                detail_url = str(row.get("detail_url") or "").strip()

                if not product_id or not detail_url:
                    skipped += 1
                    continue

                per_detail_dir = detail_base / product_id
                per_detail_dir.mkdir(parents=True, exist_ok=True)
                CommonUtil.clear_image_files(per_detail_dir)

                success = False
                crawl_status = "unknown"
                try:
                    detail_data, crawl_status = _crawl_one(
                        page, idx, product_id, detail_url, per_detail_dir
                    )
                    if detail_data:
                        _save_to_csv_append(csv_path, DETAIL_CSV_FIELDS, detail_data)
                        success = True
                        collected += 1
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    crawl_status = "parse_error"
                    logging.exception(
                        "[재수집실패] [%d/%d] product_id=%s detail_url=%s 예외 발생",
                        idx,
                        total,
                        product_id,
                        detail_url,
                    )
                finally:
                    try:
                        if success:
                            # 성공: complete_yn='Y'
                            CommonUtil.update_list_complete_yn_for_product_id(
                                pg_hook,
                                list_table=SOURCE_LIST_TABLE,
                                product_id=product_id,
                                value="Y",
                                list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_NON_N_WITH_DETAIL_URL,
                                register_flag_a_only=False,
                                list_cols=list_cols_for_complete_yn,
                            )
                        elif crawl_status == "ghost":
                            # 유령 차량: register_flag='N' + complete_yn='N' 영구 차단
                            _mark_product_as_ghost(pg_hook, product_id)
                            ghosted += 1
                        else:
                            # 일시적 실패 (timeout, parse_error 등): complete_yn='N' 만
                            CommonUtil.update_list_complete_yn_for_product_id(
                                pg_hook,
                                list_table=SOURCE_LIST_TABLE,
                                product_id=product_id,
                                value="N",
                                list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_NON_N_WITH_DETAIL_URL,
                                register_flag_a_only=False,
                                list_cols=list_cols_for_complete_yn,
                            )
                    except Exception:
                        logging.exception(
                            "[재수집] DB 갱신 실패 product_id=%s status=%s",
                            product_id,
                            crawl_status,
                        )

                if idx == 1 or idx % log_every == 0 or idx == total:
                    logging.info(
                        "재수집 중간 진행: %d/%d건 | 성공=%d | 실패=%d (유령=%d) | 스킵=%d",
                        idx,
                        total,
                        collected,
                        failed,
                        ghosted,
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
                    context = browser.new_context(
                        user_agent=(
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/122.0.0.0 Safari/537.36"
                        ),
                        viewport={"width": 1920, "height": 1080},
                    )
                    install_route_blocking(context, block_resource_types=("media", "font"))
                    page = context.new_page()
                    page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                    logging.info("reborncar detail retry browser context 재생성 완료: processed=%d/%d", idx, total)

                time.sleep(0.2)

            try:
                browser.close()
            except Exception:
                pass

        if not Path(csv_path).exists():
            raise FileNotFoundError(f"CSV 생성 실패: {csv_path}")
        logging.info(
            "✅ reborncar detail retry 완료: collected=%d failed=%d ghosted=%d skipped=%d total=%d csv=%s",
            collected,
            failed,
            ghosted,
            skipped,
            total,
            csv_path,
        )
        return str(csv_path)

    @task
    def load_retry_csv_to_ods(csv_path: str) -> dict[str, Any]:
        """
        재수집 결과 CSV를 해당 사이트 Detail ODS에 append INSERT 후,
        원천 List complete_yn 을 Detail 존재 여부와 동기화한다.
        """
        p = Path(str(csv_path or ""))
        if not p.is_file():
            raise FileNotFoundError(f"재수집 CSV 적재 대상이 없습니다: {p}")

        rows = _read_csv_rows(p)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        refresh_policy = CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_NON_N_WITH_DETAIL_URL
        if not rows:
            logging.info(
                "reborncar detail retry: CSV 데이터 행 없음 → Detail INSERT 생략, List complete_yn 동기화만. csv=%s",
                p,
            )
            CommonUtil.refresh_car_list_complete_flag_vs_detail_ods(
                hook,
                list_table=SOURCE_LIST_TABLE,
                detail_table=TARGET_DETAIL_TABLE,
                list_where_policy=refresh_policy,
            )
            table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
            return {
                "done": True,
                "target_table": TARGET_DETAIL_TABLE,
                "row_count": 0,
                "table_count": table_count,
                "csv_path": str(p),
                "skipped_insert": True,
            }

        CommonUtil.bulk_insert_detail_ods_rows(
            hook,
            TARGET_DETAIL_TABLE,
            rows,
            truncate=False,
            allow_only_table_cols=True,
        )
        CommonUtil.refresh_car_list_complete_flag_vs_detail_ods(
            hook,
            list_table=SOURCE_LIST_TABLE,
            detail_table=TARGET_DETAIL_TABLE,
            list_where_policy=refresh_policy,
        )
        table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
        logging.info(
            "reborncar detail retry CSV → ODS 적재 완료: table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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

    @task_group(group_id="prepare_retry")
    def prepare_retry():
        rows = fetch_retry_targets()
        return summarize_targets(rows)

    @task_group(group_id="retry_crawl")
    def retry_crawl(target_rows: list[dict[str, str]]):
        return crawl_and_save_csv(target_rows)

    prepared = prepare_retry()
    csv_path = retry_crawl(prepared)
    loaded = load_retry_csv_to_ods(csv_path)

    # 사이클 종료 후 다음 사이클의 list DAG 를 즉시 트리거 (무한 체인).
    trigger_next_cycle_list = TriggerDagRunOperator(
        task_id="trigger_next_cycle_list",
        trigger_dag_id="sdag_reborncar_crawler",
        wait_for_completion=False,
        reset_dag_run=False,
    )

    loaded >> trigger_next_cycle_list


dag_object = reborncar_detail_retry()


# ═══════════════════════════════════════════════════════════════════
#  경로/CSV 유틸 + 크롤 최소 유틸
# ═══════════════════════════════════════════════════════════════════


def _get_output_dir() -> Path:
    try:
        base = Path(str(Variable.get(FINAL_FILE_PATH_VAR)).strip())
    except Exception:
        base = Path("/home/limhayoung/data/crawl")
        logging.warning(
            "Airflow Variable '%s' 조회 실패 → 기본 경로 사용: %s",
            FINAL_FILE_PATH_VAR,
            base,
        )
    return CommonUtil.build_dated_site_path(base, SITE_NAME, datetime.now())


def _get_detail_img_dir() -> Path:
    try:
        img_root = Path(str(Variable.get(IMAGE_FILE_PATH_VAR)).strip())
    except Exception:
        img_root = Path("/home/limhayoung/data/img")
        logging.warning(
            "Airflow Variable '%s' 조회 실패 → 기본 경로 사용: %s",
            IMAGE_FILE_PATH_VAR,
            img_root,
        )
    year_site = CommonUtil.build_year_site_path(img_root, SITE_NAME, datetime.now())
    return year_site / "detail"


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
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        w.writerow(row)


def _read_csv_rows(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _safe_text(locator) -> str:
    try:
        return _norm_space(locator.first.inner_text() or "")
    except Exception:
        return ""


def _download_image_requests(image_url: str, save_path: Path, *, referer: str) -> bool:
    """
    (레거시 호환용) requests 기반 fallback — 서버 IP 가 CDN 에 블록되면 사용 자제.
    retry DAG 에서는 대신 _download_image(page, url, save_path) 를 쓸 것.
    """
    if not image_url:
        return False
    headers = {
        "Referer": referer,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    for _ in range(2):
        try:
            r = requests.get(image_url, headers=headers, timeout=20)
            if r.status_code != 200 or not r.content:
                continue
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_bytes(r.content)
            return True
        except Exception:
            continue
    return False


def _mark_product_as_ghost(hook, product_id: str) -> None:
    """
    유령 차량(판매 종료·페이지 삭제) product_id 를
    list 테이블에서 register_flag='N' + complete_yn='N' 으로 마킹하여
    차후 retry 대상에서 영구 제외한다.
    """
    pid = str(product_id or "").strip()
    if not pid:
        return
    sql = f"""
        UPDATE {SOURCE_LIST_TABLE}
        SET register_flag = 'N',
            complete_yn = 'N'
        WHERE TRIM(COALESCE(product_id::text, '')) = TRIM(COALESCE(%s::text, ''))
    """
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (pid,))
            n = int(cur.rowcount or 0)
        conn.commit()
        logging.info("리본카 retry 유령 차량 마킹: product_id=%s updated_rows=%d", pid, n)
    except Exception:
        logging.exception("리본카 retry 유령 차량 마킹 실패: product_id=%s", pid)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _download_image(page, image_url: str, save_path: Path) -> bool:
    """
    Playwright Chromium 세션으로 이미지 다운로드.
    page.request 는 방금 정상 렌더된 페이지의 TLS·쿠키·HTTP2 세션을 재사용하므로,
    서버 IP 가 이미지 CDN 에 블록/throttle 된 환경에서도 일반 사용자로 인식되어 통과한다.
    """
    if not image_url:
        return False
    try:
        headers = {
            "Referer": (page.url or "").split("#")[0] or "https://www.reborncar.co.kr/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
            "Sec-Fetch-Dest": "image",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Site": "same-site",
        }
        resp = page.request.get(image_url, timeout=10000, headers=headers)
        if not resp or not resp.ok:
            return False
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(resp.body())
        return True
    except Exception:
        return False


def _crawl_one(
    page, idx: int, product_id: str, detail_url: str, detail_img_dir: Path
) -> tuple[dict[str, Any] | None, str]:
    """
    반환: (data, status)
      status = "ok"          — 수집 성공
               "ghost"        — 유령 차량 → register_flag='N' 영구 차단 대상
                                ① URL 이 error/notfound/sb1001 으로 리다이렉트
                                ② 페이지에 "판매 완료/게시가 종료" 등 안내 문구 존재
                                ③ .vip-section 자체 없음
                                ④ DOM 은 있지만 car_name·car_num 둘 다 빈값
               "timeout"      — goto 실패 (일시적) → complete_yn='N' 유지
               "parse_error"  — 파싱 예외 (일시적) → complete_yn='N' 유지
    """
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
                    ready_selectors=("#wrap .vip-section,.vip-section,body",),
                    ready_timeout_ms=20_000,
                ),
                logger=logging.getLogger(__name__),
                attempts=1,
            )
            page.wait_for_timeout(300)
            break
        except Exception as e:
            if attempt < 2:
                logging.warning("reborncar detail retry 재시도 (%d/3): %s - %s", attempt + 2, product_id, e)
                time.sleep(2)
            else:
                logging.error("reborncar detail retry 접속 실패: %s - %s", product_id, e)
                return None, "timeout"

    # 유령 판정 ①: goto 후 URL 이 에러/목록 페이지로 리다이렉트 (판매완료·삭제 시 리본카 동작)
    try:
        final_url = (page.url or "").lower()
    except Exception:
        final_url = ""
    if final_url and any(k in final_url for k in ("error", "notfound", "sb1001")):
        # SB1001 = 목록 페이지. 상세 URL (SB1002) 에서 여기로 튕겼으면 상품 삭제.
        logging.info("리본카 retry 유령 판정(URL 리다이렉트): product_id=%s final_url=%s", product_id, final_url)
        return None, "ghost"

    # 유령 판정 ②: 페이지 본문에 판매완료·게시종료 안내 텍스트 존재
    # 리본카는 판매완료 차량 URL 접속 시 "차량 정보를 확인할 수 없습니다.
    # 판매 완료 또는 기타 사유로 게시가 종료된 상태입니다." 안내를 .vip-section 안에 표시.
    try:
        body_text = page.locator("body").inner_text(timeout=2000) or ""
    except Exception:
        body_text = ""
    ghost_phrases = (
        "차량 정보를 확인할 수 없습니다",
        "게시가 종료",
        "판매 완료",
        "판매완료",
        "판매가 종료",
        "노출이 종료",
    )
    if any(phrase in body_text for phrase in ghost_phrases):
        matched = next(p for p in ghost_phrases if p in body_text)
        logging.info("리본카 retry 유령 판정(안내 문구): product_id=%s phrase='%s'", product_id, matched)
        return None, "ghost"

    root = page.locator("#wrap .vip-section").first
    if root.count() == 0:
        root = page.locator(".vip-section").first

    # 유령 판정 ③: .vip-section 자체 없음 — 상세 DOM 구조가 뜨지 않음
    if root.count() == 0:
        logging.info("리본카 retry 유령 판정(.vip-section 없음): product_id=%s", product_id)
        return None, "ghost"

    try:
        head_info = root.locator(".vip-head .vip-head-info").first
        car_infos = head_info.locator(".car-info")
        if car_infos.count() >= 1:
            main = car_infos.nth(0).locator(".car-main-info").first
            data["car_num"] = _safe_text(main.locator(".car-number"))
            data["car_name"] = _safe_text(main.locator(".car-model .car-model-txt"))

        if car_infos.count() >= 2:
            sub = car_infos.nth(1).locator(".car-sub-info").first
            infos = sub.locator(".car-infos").first
            data["release_dt"] = _safe_text(infos.locator(".release-dt"))
            data["car_navi"] = _safe_text(infos.locator(".car-navi"))
            data["grear_box"] = _safe_text(infos.locator(".gear-box"))
            data["car_color"] = _safe_text(infos.locator(".car-color"))
            data["car_fuel"] = _safe_text(infos.locator(".car-fuel"))
            data["car_seat"] = _safe_text(infos.locator(".car-seat"))

        # 이미지 저장(간소화: 보이는 img 중 일부)
        # Chromium 세션 경유(page.request) 로 다운로드 — 서버 IP 가 CDN 에 블록돼도 통과 가능.
        try:
            img_start = time.time()
            imgs = root.locator("img")
            saved = 0
            for i in range(min(imgs.count(), 40)):
                src = (imgs.nth(i).get_attribute("data-src") or imgs.nth(i).get_attribute("src") or "").strip()
                if not src or src.startswith("data:"):
                    continue
                out = detail_img_dir / f"{product_id}_{saved+1}.png"
                if _download_image(page, src, out):
                    saved += 1
                if saved >= 8:
                    break
            logging.info(
                "리본카 retry 이미지: product_id=%s 저장=%d (%.1fs)",
                product_id, saved, time.time() - img_start,
            )
        except Exception:
            pass

    except Exception as e:
        logging.error("reborncar detail retry 파싱 전체 오류: %s - %s", product_id, e)
        return None, "parse_error"

    core_cols = ("car_name", "car_num")
    filled_core = sum(1 for c in core_cols if str(data.get(c) or "").strip())
    if filled_core == 0:
        # DOM 은 있었지만 car_name/car_num 둘 다 빈값 → 판매완료 페이지일 가능성 큼
        logging.info("리본카 retry 유령 판정(필수값 없음): product_id=%s", product_id)
        return None, "ghost"

    return data, "ok"
