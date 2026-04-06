import csv
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from util.common_util import CommonUtil
from util.playwright_util import GotoSpec, goto_with_retry, images_enabled, install_route_blocking


# ═══════════════════════════════════════════════════════════════════
#  상수
# ═══════════════════════════════════════════════════════════════════

SOURCE_LIST_TABLE = "ods.ods_car_list_autoinside"
TARGET_DETAIL_TABLE = "ods.ods_car_detail_autoinside"
FINAL_FILE_PATH_VAR = "used_car_final_file_path"  # Airflow Variable 키
IMAGE_FILE_PATH_VAR = "used_car_image_file_path"  # 예: /home/limhayoung/data/img
SITE_NAME = "오토인사이드"

DETAIL_CSV_FIELDS = [
    "model_sn",
    "product_id",
    "car_name",
    "year",
    "km",
    "car_spec",
    "car_num",
    "car_color",
    "category",
    "inspection",
    "insurance",
    "car_opt",
    "car_history",
    "car_inspect",
    "car_imgs",
    "date_crtr_pnttm",
    "create_dt",
]


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의
# ═══════════════════════════════════════════════════════════════════


@dag(
    dag_id="sdag_autoinside_detail_crawl",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "autoinside", "detail", "crawler"],
)
def autoinside_detail_crawl():
    """오토인사이드 상세: register_flag='A' 만 수집 → CSV 한 줄마다 List complete_yn Y/N, 이후 Detail 적재·동기화. Reg_Flag 는 List DAG 전용."""

    @task
    def fetch_target_urls() -> list[dict[str, str]]:
        """
        ods.ods_car_list_autoinside 에서
        register_flag = 'A' 이고 date_crtr_pnttm 이 테이블 내 최신 적재일자와 같은 행만 조회한다.
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
        logging.info("select_target_urls_stmt ::: %s", sql)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        conn = hook.get_conn()
        rows = []
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
                logging.info(
                    "오토인사이드 detail 수집 기준 date_crtr_pnttm(최신): %s",
                    latest_pnttm,
                )
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall():
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
                                "오토인사이드 상세: 최신 적재일(%s) 기준 신규(register_flag=A) 0건 → "
                                "이번에 새로 추가된 중고차 없음. 상세 크롤 생략 후 DAG 정상 완료로 진행합니다. "
                                "(최신일 전체 행=%s, detail_url 보유 행=%s)",
                                latest_pnttm,
                                total_l,
                                cnt_url,
                            )
                        else:
                            logging.warning(
                                "상세 대상 0건: 최신일(%s)에 신규(A)=%s건 있으나 "
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
        logging.info("상세 크롤 준비: 총 %d건, detail_url 있음 %d건", n, with_url)
        if not target_rows:
            logging.info("상세 수집 대상 0건 — 다음 태스크에서 헤더만 CSV 생성 후 정상 완료합니다.")
        return target_rows

    @task
    def crawl_and_save_csv(target_rows: list[dict[str, str]]) -> str:
        # Airflow DAG 파싱 단계에서 playwright 미설치/무거운 import로 DAGFileProcessor가 죽는 문제 방지
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"autoinside_detail_{run_ts}.csv"
        logging.info("출력 파일: %s", csv_path)

        detail_base = _get_detail_img_dir()
        detail_base.mkdir(parents=True, exist_ok=True)
        logging.info(
            "상세 이미지 루트(삭제 없음): …/detail/{product_id}/{product_id}_N.png → %s",
            detail_base.resolve(),
        )

        total = len(target_rows)
        if total == 0:
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=DETAIL_CSV_FIELDS)
                w.writeheader()
            logging.info(
                "수집할 데이터가 없습니다. Playwright 생략, 헤더만 기록: %s",
                csv_path,
            )
            return str(csv_path)

        collected = 0
        failed = 0
        skipped = 0
        recycle_every = 300
        pg_hook = PostgresHook(postgres_conn_id="car_db_conn")

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
            install_route_blocking(context)
            page = context.new_page()
            page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

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
                        product_id or "(빈값)",
                    )
                    continue

                if idx == 1 or idx % 50 == 0 or idx == total:
                    logging.info(
                        "[%d/%d] 호출 대상 - product_id=%s, detail_url=%s",
                        idx,
                        total,
                        product_id,
                        detail_url,
                    )

                per_detail_dir = detail_base / product_id
                per_detail_dir.mkdir(parents=True, exist_ok=True)

                success = False
                fail_reason: str | None = None
                try:
                    detail_data = _crawl_one(page, idx, product_id, detail_url, per_detail_dir)
                    if detail_data:
                        _save_to_csv_append(csv_path, DETAIL_CSV_FIELDS, detail_data)
                        success = True
                        collected += 1
                    else:
                        fail_reason = (
                            "상세 수집 결과 없음(접속 실패·파싱 오류·필수 필드 car_name/year/km 미충족)"
                        )
                        failed += 1
                except Exception as e:
                    fail_reason = f"{type(e).__name__}: {e}"
                    failed += 1
                    logging.exception(
                        "[상세수집실패] [%d/%d] product_id=%s detail_url=%s 예외 발생",
                        idx,
                        total,
                        product_id or "(빈값)",
                        detail_url,
                    )
                finally:
                    try:
                        yn = "Y" if success else "N"
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
                            product_id or "(빈값)",
                            detail_url,
                            fail_reason,
                        )

                if idx % 100 == 0 or idx == total:
                    logging.info(
                        "상세 수집 진행: processed=%d/%d, collected=%d, failed=%d, skipped=%d",
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
                    context = browser.new_context(
                        user_agent=(
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/122.0.0.0 Safari/537.36"
                        ),
                        viewport={"width": 1920, "height": 1080},
                    )
                    install_route_blocking(context)
                    page = context.new_page()
                    page.add_init_script(
                        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                    )
                    logging.info("브라우저 컨텍스트 재생성 완료: processed=%d/%d", idx, total)

                time.sleep(0.3)

            browser.close()

        logging.info(
            "✅ 완료: collected=%d, failed=%d, skipped=%d, total=%d → %s",
            collected,
            failed,
            skipped,
            total,
            csv_path,
        )
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV 생성 실패: {csv_path}")
        return str(csv_path)

    @task
    def load_detail_csv_to_ods(csv_path: str) -> dict[str, Any]:
        """
        crawl_and_save_csv 결과 CSV를 ods.ods_car_detail_autoinside로 적재.
        - 컬럼 필터링·append insert
        - 원천 Car List vs Detail 로 complete_yn Y/N (크롤 중 단건 갱신 후 최종 동기화, register_flag 미변경)
        """
        p = Path(str(csv_path or ""))
        if not p.is_file():
            raise FileNotFoundError(f"적재 대상 CSV가 없습니다: {p}")

        rows = _read_csv_rows(p)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        if not rows:
            logging.info(
                "오토인사이드 상세 DAG 정상 완료: 신규(register_flag=A) 차량 없음 → detail INSERT 생략, "
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
                "message": "신규 중고차 없음, 상세 수집 생략, complete_yn 동기화만 수행",
                "target_table": TARGET_DETAIL_TABLE,
                "row_count": 0,
                "table_count": table_count,
                "csv_path": str(p),
                "skipped_insert": True,
            }

        CommonUtil.bulk_insert_detail_ods_rows(hook, TARGET_DETAIL_TABLE, rows, truncate=False, allow_only_table_cols=True)
        CommonUtil.refresh_car_list_complete_flag_vs_detail_ods(
            hook,
            list_table=SOURCE_LIST_TABLE,
            detail_table=TARGET_DETAIL_TABLE,
            list_where_policy=CommonUtil.DETAIL_LIST_COMPLETE_FLAG_POLICY_LATEST_SNAPSHOT,
        )
        table_count = CommonUtil.get_table_row_count(hook, TARGET_DETAIL_TABLE)
        logging.info(
            "오토인사이드 detail CSV 적재 완료: table=%s, inserted_rows=%d, table_count=%d, csv=%s",
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


dag_object = autoinside_detail_crawl()


if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    dag_object.test(
        execution_date=datetime(2025, 10, 10, 8, 0),
        conn_file_path=conn_path,
    )


# ═══════════════════════════════════════════════════════════════════
#  경로 유틸
# ═══════════════════════════════════════════════════════════════════


def _get_output_dir() -> Path:
    """
    Airflow Variable: used_car_final_file_path 기준 오늘 날짜 경로 반환.
    예) /home/limhayoung/data/crawl/2026년/오토인사이드/20260327
    """
    try:
        base = Path(Variable.get(FINAL_FILE_PATH_VAR))
    except Exception:
        base = Path("/home/limhayoung/data/crawl")
        logging.warning(
            "Airflow Variable '%s' 조회 실패 → 기본 경로 사용: %s",
            FINAL_FILE_PATH_VAR,
            base,
        )
    return CommonUtil.build_dated_site_path(base, SITE_NAME, datetime.now())


def _get_detail_img_dir() -> Path:
    """
    Airflow Variable used_car_image_file_path 기준
    {img_root}/YYYY년/오토인사이드/detail — 차량별 {product_id}_1.png … 저장(DAG에서 폴더 비우지 않음).
    """
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


# ═══════════════════════════════════════════════════════════════════
#  CSV 유틸
# ═══════════════════════════════════════════════════════════════════


def _get_now_times() -> tuple[str, str]:
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%Y%m%d%H%M")


def _csv_cell_excel_text(val: Any) -> Any:
    """숫자-숫자 패턴 Excel 날짜 변환 방지."""
    if val is None:
        return ""
    s = (val if isinstance(val, str) else str(val)).strip()
    if s and re.match(r"^\d+-\d+$", s):
        return "'" + s
    return s


def _save_to_csv_append(file_path: Path, fieldnames: list[str], data: dict[str, Any]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = file_path.exists()
    row = {
        k: _csv_cell_excel_text(v) if (isinstance(v, str) or v is None) else v
        for k, v in data.items()
    }
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


def _collect_data_nm(li_locator) -> str:
    try:
        n = li_locator.count()
        parts: list[str] = []
        for i in range(n):
            nm = (li_locator.nth(i).get_attribute("data-nm") or "").strip()
            if nm:
                parts.append(_norm_space(nm))
        return " | ".join(parts)
    except Exception:
        return ""


def _download_image(page, image_url: str, save_path: Path) -> bool:
    if not images_enabled():
        return False
    try:
        headers = {
            "Referer": (page.url or "https://www.autoinside.co.kr/").split("#")[0],
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
        }
        resp = page.request.get(image_url, timeout=30000, headers=headers)
        if not resp or not resp.ok:
            return False
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(resp.body())
        return True
    except Exception:
        return False


def _autoinside_norm_img_url(raw: str, page_url: str) -> str:
    s = (raw or "").strip()
    if not s or s.startswith("data:"):
        return ""
    if s.startswith("//"):
        s = "https:" + s
    elif s.startswith("/"):
        s = urljoin(page_url, s)
    elif not s.startswith("http"):
        s = urljoin(page_url, s)
    return s


def _autoinside_collect_urls_from_imgs(imgs_locator, page_url: str, seen: set[str], out: list[str]) -> None:
    """swiper-slide img에서 data-src / currentSrc / src 순으로 URL 수집."""
    for i in range(imgs_locator.count()):
        img = imgs_locator.nth(i)
        try:
            src = img.evaluate(
                """el => {
                    const ds = el.getAttribute('data-src') || el.getAttribute('data-original');
                    if (ds && ds.trim() && !ds.trim().startsWith('data:')) return ds.trim();
                    if (el.currentSrc && el.currentSrc.trim() && !el.currentSrc.startsWith('data:')) return el.currentSrc.trim();
                    const s = el.getAttribute('src');
                    if (s && s.trim() && !s.trim().startsWith('data:')) return s.trim();
                    return '';
                }"""
            )
        except Exception:
            src = ""
        u = _autoinside_norm_img_url(str(src or ""), page_url)
        # onerror에서 치환되는 placeholder(svg)는 저장할 가치가 낮아 제외
        if u and "img_car_view_no_img.svg" in u:
            continue
        if u and u not in seen:
            seen.add(u)
            out.append(u)


def _collect_autoinside_detail_gallery_urls(page, root) -> list[str]:
    """
    오토인사이드 상세 갤러리 URL 수집.
    - 기존 `.img_section.on`만 보면 활성 탭·현재 슬라이드 1장만 잡히는 경우가 많음.
    - 모든 img_section 탭을 순회하고, swiper next로 lazy 슬라이드까지 펼친 뒤 수집한다.
    """
    page_url = page.url or "https://www.autoinside.co.kr/"
    seen: set[str] = set()
    urls: list[str] = []

    # 1) 우선: thumbs swiper(`thumb_slide`) 안에 있는 모든 img를 수집
    #    사용자가 준 DOM 예시 구조:
    #    `.swiper-wrapper` 밑에 `.swiper-slide` 밑 `img`
    try:
        thumb_imgs_sel = ".thumb_slide .swiper-wrapper .swiper-slide img"
        thumb_imgs = root.locator(thumb_imgs_sel)
        if thumb_imgs.count() == 0:
            # fallback: thumb_slide 클래스가 안 잡히거나 DOM이 변형되는 경우
            fallback_sel = ".swiper-wrapper .swiper-slide img"
            thumb_imgs = root.locator(fallback_sel)
            if thumb_imgs.count() == 0:
                thumb_imgs = page.locator(fallback_sel)
        _autoinside_collect_urls_from_imgs(thumb_imgs, page_url, seen, urls)
    except Exception:
        pass

    slide_img_sel = ".main_slide .swiper-slide:not(.swiper-slide-duplicate) img"
    broad_fallback_sel = (
        ".car_view_content .car_img_wrap .main_slide img, "
        ".car_view_content .section.car_img_wrap .main_slide img"
    )

    def collect_from_section(sec) -> None:
        imgs = sec.locator(slide_img_sel)
        if imgs.count() == 0:
            imgs = sec.locator(".main_slide img")
        _autoinside_collect_urls_from_imgs(imgs, page_url, seen, urls)

    def swiper_advance_slide_scope(slide_scope, gallery_wrap) -> None:
        """다음 버튼은 종종 slide_scope 밖(.car_img_wrap 공통)에 있다."""
        next_btn = slide_scope.locator(".swiper-button-next").first
        if next_btn.count() == 0 and gallery_wrap.count() > 0:
            next_btn = gallery_wrap.locator(".swiper-button-next").first
        no_new_streak = 0
        for _ in range(120):
            before = len(urls)
            try:
                if next_btn.count() > 0:
                    next_btn.click(timeout=2500)
                    page.wait_for_timeout(220)
            except Exception:
                break
            imgs2 = slide_scope.locator(slide_img_sel)
            if imgs2.count() == 0:
                imgs2 = slide_scope.locator(".main_slide img")
            if imgs2.count() == 0 and gallery_wrap.count() > 0:
                imgs2 = gallery_wrap.locator(slide_img_sel)
                if imgs2.count() == 0:
                    imgs2 = gallery_wrap.locator(".main_slide img")
            _autoinside_collect_urls_from_imgs(imgs2, page_url, seen, urls)
            if len(urls) == before:
                no_new_streak += 1
                if no_new_streak >= 10:
                    break
            else:
                no_new_streak = 0

    try:
        gallery_wrap = root.locator(".car_view_content .section.car_img_wrap").first
        if gallery_wrap.count() == 0:
            gallery_wrap = root.locator(".car_view_content .car_img_wrap").first

        sections = root.locator(".car_view_content .section.car_img_wrap .img_section")
        n_sec = sections.count()
        if n_sec > 0:
            for si in range(n_sec):
                sec = sections.nth(si)
                try:
                    sec.scroll_into_view_if_needed(timeout=5000)
                except Exception:
                    pass
                try:
                    sec.click(timeout=3000)
                except Exception:
                    try:
                        sec.click(timeout=3000, force=True)
                    except Exception:
                        pass
                page.wait_for_timeout(350)
                collect_from_section(sec)
                swiper_advance_slide_scope(sec, gallery_wrap)
        elif gallery_wrap.count() > 0:
            collect_from_section(gallery_wrap)
            swiper_advance_slide_scope(gallery_wrap, gallery_wrap)

        if not urls:
            imgs = root.locator(broad_fallback_sel)
            _autoinside_collect_urls_from_imgs(imgs, page_url, seen, urls)
            wrap2 = root.locator(".section.car_img_wrap, .car_img_wrap").first
            if wrap2.count() > 0:
                swiper_advance_slide_scope(wrap2, wrap2)
    except Exception as e:
        logging.debug("[갤러리 URL] 수집 예외: %s", e)

    return urls


# ═══════════════════════════════════════════════════════════════════
#  단일 상세페이지 크롤링
# ═══════════════════════════════════════════════════════════════════


def _crawl_one(
    page,
    idx: int,
    product_id: str,
    detail_url: str,
    detail_img_dir: Path,
) -> dict[str, Any] | None:
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
                    ready_selectors=(".page.car_view_wrap,body",),
                    ready_timeout_ms=20_000,
                ),
                logger=logging.getLogger(__name__),
                attempts=1,
            )
            # 고정 대기 대신 최소 대기(대부분 selector 로드로 충분)
            page.wait_for_timeout(250)
            break
        except Exception as e:
            if attempt < 2:
                logging.warning("재시도 (%d/3): %s - %s", attempt + 2, product_id, e)
                time.sleep(2)
            else:
                logging.error("접속 실패: %s - %s", product_id, e)
                return None

    root = page.locator("#wrap #frm .container .container_inn .page.car_view_wrap").first
    if root.count() == 0:
        # fallback: 페이지 구조가 다를 때
        root = page.locator(".page.car_view_wrap").first

    try:
        # ── 우측 사이드: 차량명/스펙/기타정보 ────────────────────────────
        side = root.locator(".car_view_side .car_view_side_inn").first
        price_wrap = side.locator(".car_view_price_wrap").first

        data["car_name"] = _safe_text(price_wrap.locator(".car_nm.carName"))

        spec_spans = price_wrap.locator(".car_spec span")
        try:
            if spec_spans.count() >= 1:
                data["year"] = _norm_space(spec_spans.nth(0).inner_text() or "")
            if spec_spans.count() >= 2:
                data["km"] = _norm_space(spec_spans.nth(1).inner_text() or "")
            if spec_spans.count() >= 3:
                data["car_spec"] = _norm_space(spec_spans.nth(2).inner_text() or "")
            if spec_spans.count() >= 4:
                data["car_num"] = _norm_space(spec_spans.nth(3).inner_text() or "")
            if spec_spans.count() >= 5:
                data["car_color"] = _norm_space(spec_spans.nth(4).inner_text() or "")
        except Exception:
            pass

        etc_lis = side.locator(".car_info_etc li")
        try:
            if etc_lis.count() >= 1:
                data["category"] = _safe_text(
                    etc_lis.nth(0).locator(".etc_box.link_tooltip .txt.main")
                )
            if etc_lis.count() >= 2:
                data["inspection"] = _safe_text(
                    etc_lis.nth(1).locator(".etc_box.link_tooltip .txt")
                )
            if etc_lis.count() >= 3:
                data["insurance"] = _safe_text(
                    etc_lis.nth(2).locator(".etc_box.link_tooltip .txt")
                )
        except Exception:
            pass

        # ── 옵션: data-nm 속성 join ─────────────────────────────────────
        # 오토인사이드는 옵션명이 보통 `li > a.item[data-nm]`에 존재한다.
        # car_opt 섹션이 여러 번 등장할 수 있어(중첩/레이아웃 분기),
        # 모든 섹션을 순회하며 data-nm 값을 |로 합친다.
        try:
            opt_sections = root.locator(".car_view_content .section.car_opt")
            seen: set[str] = set()
            opt_parts: list[str] = []
            for si in range(opt_sections.count()):
                sec = opt_sections.nth(si)
                # 1) 일반 케이스: a.item[data-nm]
                nm_nodes = sec.locator(".list a.item[data-nm]")
                # 2) fallback: 리스트 내부의 어떤 엘리먼트든 data-nm가 있으면 수집
                if nm_nodes.count() == 0:
                    nm_nodes = sec.locator(".list [data-nm]")

                for ni in range(nm_nodes.count()):
                    nm = (nm_nodes.nth(ni).get_attribute("data-nm") or "").strip()
                    nm = _norm_space(nm)
                    if not nm or nm in seen:
                        continue
                    seen.add(nm)
                    opt_parts.append(nm)
            data["car_opt"] = " | ".join(opt_parts)
        except Exception:
            data["car_opt"] = ""

        # ── 히스토리: tit/txt join ──────────────────────────────────────
        hist_section = root.locator(".car_view_content .section.car_history").first
        hist_items = hist_section.locator(".list li.item")
        try:
            parts: list[str] = []
            for i in range(hist_items.count()):
                it = hist_items.nth(i)
                tit = _safe_text(it.locator(".tit"))
                txt = _safe_text(it.locator(".txt"))
                if tit and txt:
                    parts.append(f"{tit} : {txt}")
                elif tit:
                    parts.append(tit)
                elif txt:
                    parts.append(txt)
            data["car_history"] = " | ".join(parts)
        except Exception:
            data["car_history"] = ""

        # ── 성능점검: (span1 : span2) join ──────────────────────────────
        insp_section = root.locator(".car_view_content .section.car_inspect").first
        boxes = insp_section.locator(".inspect_wrap .inspect_box .inspect_img .txt")
        try:
            parts: list[str] = []
            for i in range(boxes.count()):
                box = boxes.nth(i)
                spans = box.locator("span")
                if spans.count() < 2:
                    continue
                k = _norm_space(spans.nth(0).inner_text() or "")
                v = _norm_space(spans.nth(1).inner_text() or "")
                if k and v:
                    parts.append(f"{k} : {v}")
            data["car_inspect"] = " | ".join(parts)
        except Exception:
            data["car_inspect"] = ""

        # ── 상세 이미지 저장 (탭·swiper 전체 순회) ────────────────────────
        try:
            gallery_urls = _collect_autoinside_detail_gallery_urls(page, root)
            logging.debug(
                "[갤러리 이미지] %s 수집 URL %d건",
                product_id,
                len(gallery_urls),
            )
            for j, src in enumerate(gallery_urls, start=1):
                out = detail_img_dir / f"{product_id}_{j}.png"
                _download_image(page, src, out)
        except Exception as e:
            logging.debug("[갤러리 이미지] %s : %s", product_id, e)

    except Exception as e:
        logging.error("파싱 전체 오류: %s - %s", product_id, e)
        return None

    # 최소 핵심값이 비어있으면 실패로 처리(품질 방어)
    core_cols = ("car_name", "year", "km")
    filled_core = sum(1 for c in core_cols if str(data.get(c) or "").strip())
    if filled_core == 0:
        return None

    return data

