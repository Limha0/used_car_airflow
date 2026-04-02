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
SOURCE_LIST_TABLE    = "ods.ods_car_list_heydealer"
FINAL_FILE_PATH_VAR  = "used_car_final_file_path"   # Airflow Variable 키
IMAGE_FILE_PATH_VAR  = "used_car_image_file_path"   # 예: /home/limhayoung/data/img
SITE_NAME            = "헤이딜러"

DETAIL_CSV_FIELDS = [
    "model_sn",
    "product_id",
    "car_name",
    "year",
    "km",
    "accident",
    "insurance",
    "guarantee",
    "refund",
    "inner_car_wash",
    "exterior_description",
    "interior_description",
    "main_option",
    "delivery_information",
    "recommendation_comment",
    "tire",
    "tinting",
    "car_key",
    "inspection_records_1",
    "inspection_records_2",
    "car_imgs",
    "date_crtr_pnttm",
    "create_dt",
]


# ═══════════════════════════════════════════════════════════════════
#  DAG 정의
# ═══════════════════════════════════════════════════════════════════

@dag(
    dag_id="sdag_heydealer_detail_only1",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "heydealer", "detail", "crawler"],
)
def heydealer_detail_crawl():
    """헤이딜러 상세페이지 크롤링 DAG (register_flag != 'N' 전체). Task group으로 준비 → 크롤·저장 단계 분리."""

    # ── Task 1 : DB에서 수집 대상 조회 ────────────────────────────────────
    @task
    def fetch_target_urls() -> list[dict[str, str]]:
        """
        ods.ods_car_list_heydealer 에서
        register_flag != 'N' (NULL 포함) 인 product_id, detail_url 조회
        """
        sql = f"""
        SELECT
            product_id,
            detail_url,
            register_flag
        FROM {SOURCE_LIST_TABLE}
        WHERE (register_flag IS NULL OR TRIM(register_flag) != 'N')
          AND detail_url IS NOT NULL
          AND TRIM(detail_url) != ''
        ORDER BY model_sn
        """
        logging.info("select_target_urls_stmt ::: %s", sql)
        hook = PostgresHook(postgres_conn_id="car_db_conn")
        conn = hook.get_conn()
        rows = []
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(cols, row)))
        finally:
            try:
                conn.close()
            except Exception:
                pass

        logging.info("수집 대상: %d건", len(rows))
        if not rows:
            raise ValueError("수집 대상 URL이 없습니다. 테이블을 확인하세요.")
        return rows

    @task
    def summarize_targets(target_rows: list[dict[str, str]]) -> list[dict[str, str]]:
        """대상 건수·URL 유효 건수만 집계 후 그대로 전달 (그래프에서 준비 단계 분리)."""
        n = len(target_rows)
        with_url = sum(1 for r in target_rows if str(r.get("detail_url") or "").strip())
        logging.info("상세 크롤 준비: 총 %d건, detail_url 있음 %d건", n, with_url)
        if not target_rows:
            raise ValueError("summarize_targets: 대상이 비어 있습니다.")
        return target_rows

    # ── Task 2 : 상세 크롤링 + CSV 저장 ───────────────────────────────────
    @task
    def crawl_and_save_csv(target_rows: list[dict[str, str]]) -> str:
        # Airflow DAG 파싱 단계에서 playwright 미설치/무거운 import로 DAGFileProcessor가 죽는 문제 방지
        from playwright.sync_api import sync_playwright

        """
        playwright로 detail_url 순회 크롤링 후
        heydealer_detail_YYYYMMDDHHMI.csv 저장.
        반환값: 생성된 CSV 절대경로 문자열
        """
        # 출력 경로 결정
        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts   = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"heydealer_detail_{run_ts}.csv"
        logging.info("출력 파일: %s", csv_path)

        detail_img_dir = _get_detail_img_dir()
        detail_img_dir.mkdir(parents=True, exist_ok=True)
        CommonUtil.clear_image_files(detail_img_dir, recursive=False)
        logging.info("상세 이미지 저장 디렉터리: %s", detail_img_dir.resolve())

        total = len(target_rows)
        collected = 0
        failed = 0
        skipped = 0
        recycle_every = 300

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

                if not detail_url:
                    skipped += 1
                    continue

                # 진행 로그: 1건 / 100건마다 / 마지막
                if idx == 1 or idx % 50 == 0 or idx == total:
                    logging.info(
                        "[%d/%d] 호출 대상 - product_id=%s, detail_url=%s",
                        idx, total, product_id, detail_url,
                    )

                try:
                    detail_data = _crawl_one(page, idx, product_id, detail_url, detail_img_dir)
                    if detail_data:
                        _save_to_csv_append(csv_path, DETAIL_CSV_FIELDS, detail_data)
                        collected += 1
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    logging.exception(
                        "[%d/%d] 상세 수집 예외 - product_id=%s, detail_url=%s, err=%s",
                        idx, total, product_id, detail_url, e,
                    )

                if idx % 100 == 0 or idx == total:
                    logging.info(
                        "상세 수집 진행: processed=%d/%d, collected=%d, failed=%d, skipped=%d",
                        idx, total, collected, failed, skipped,
                    )

                # 장시간 실행 안정화를 위해 페이지/컨텍스트를 주기적으로 재생성
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

                time.sleep(0.3)  # 서버 부하 방지

            browser.close()

        logging.info(
            "✅ 완료: collected=%d, failed=%d, skipped=%d, total=%d → %s",
            collected, failed, skipped, total, csv_path,
        )

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV 생성 실패: {csv_path}")

        return str(csv_path)

    @task_group(group_id="prepare_detail_crawl")
    def prepare_detail_crawl():
        """1단계: DB에서 대상 조회 → 건수 요약."""
        rows = fetch_target_urls()
        return summarize_targets(rows)

    @task_group(group_id="crawl_and_persist")
    def crawl_and_persist(target_rows: list[dict[str, str]]):
        """2단계: 상세 페이지 크롤링 후 CSV 저장."""
        return crawl_and_save_csv(target_rows)

    prepared = prepare_detail_crawl()
    crawl_and_persist(prepared)


dag_object = heydealer_detail_crawl()


# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"

    dag_object.test(
        execution_date=datetime(2025, 10, 10, 8, 0),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
    )


# ═══════════════════════════════════════════════════════════════════
#  경로 유틸
# ═══════════════════════════════════════════════════════════════════

def _get_output_dir() -> Path:
    """
    Airflow Variable: used_car_final_file_path 기준 오늘 날짜 경로 반환.
    예) /home/limhayoung/data/crawl/2026년/헤이딜러/20260327
    """
    try:
        base = Path(Variable.get(FINAL_FILE_PATH_VAR))
    except Exception:
        base = Path("/home/limhayoung/data/crawl")
        logging.warning(
            "Airflow Variable '%s' 조회 실패 → 기본 경로 사용: %s",
            FINAL_FILE_PATH_VAR, base,
        )
    return CommonUtil.build_dated_site_path(base, SITE_NAME, datetime.now())


def _get_detail_img_dir() -> Path:
    """
    Airflow Variable used_car_image_file_path 기준
    {img_root}/YYYY년/헤이딜러/detail
    예) /home/limhayoung/data/img/2026년/헤이딜러/detail
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
    """숫자-숫자 패턴(사브 9-3 등) Excel 날짜 변환 방지."""
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


# ═══════════════════════════════════════════════════════════════════
#  파싱 헬퍼
# ═══════════════════════════════════════════════════════════════════

def _safe_texts_join(locator, sep: str = " | ") -> str:
    try:
        count = locator.count()
        parts = []
        for i in range(count):
            t = re.sub(r"\s+", " ", (locator.nth(i).inner_text() or "")).strip()
            if t:
                parts.append(t)
        return sep.join(parts)
    except Exception:
        return ""


def _parse_kv_rows(
    section_locator,
    row_class: str,
    key_class: str,
    key_map: dict[str, str],
) -> dict[str, str]:
    """
    section_locator 안의 row_class 행을 순회하며
    key_class 텍스트를 key_map으로 매핑해 값을 추출한다.
    """
    result: dict[str, str] = {v: "" for v in key_map.values()}
    try:
        rows  = section_locator.locator(f".{row_class}")
        count = rows.count()
        for i in range(count):
            row_el = rows.nth(i)

            # 키 텍스트: 직접 텍스트 → 없으면 하위 div
            key_text = ""
            try:
                key_text = row_el.locator(f".{key_class}").first.inner_text().strip()
            except Exception:
                pass
            if not key_text:
                try:
                    key_text = row_el.locator(f".{key_class} div").first.inner_text().strip()
                except Exception:
                    pass

            col_name = key_map.get(key_text)
            if not col_name:
                continue

            # 값: key_class 가 아닌 첫 번째 형제 div
            try:
                val_divs = row_el.locator("> div")
                for vi in range(val_divs.count()):
                    vd  = val_divs.nth(vi)
                    cls = vd.get_attribute("class") or ""
                    if key_class not in cls:
                        result[col_name] = re.sub(
                            r"\s+", " ", (vd.inner_text() or "")
                        ).strip()
                        break
            except Exception:
                pass
    except Exception:
        pass
    return result


def _extract_value_from_page_text(body_text: str, label: str) -> str:
    """
    클래스 구조가 바뀌었을 때를 대비한 텍스트 기반 fallback.
    예: '연식 2023년식' / '주행거리 20,310km'
    """
    if not body_text:
        return ""
    try:
        pattern = rf"{re.escape(label)}\s*[:：]?\s*([^\n\r|]+)"
        m = re.search(pattern, body_text)
        return re.sub(r"\s+", " ", (m.group(1) if m else "")).strip()
    except Exception:
        return ""


def _apply_detail_extras_fallback(page, data: dict[str, Any]) -> None:
    """
    타이어·틴팅·차 키는 섹션 nth가 어긋겼을 때(전역 .css-ltrevz 혼선 등) body 텍스트로 보완.
    """
    pairs = (("tire", "타이어"), ("tinting", "틴팅"), ("car_key", "차 키"))
    if all(str(data.get(c) or "").strip() for c, _ in pairs):
        return
    try:
        body_text = re.sub(
            r"[ \t]+", " ",
            page.locator("body").first.inner_text() or "",
        )
    except Exception:
        return
    for col, label in pairs:
        if str(data.get(col) or "").strip():
            continue
        v = _extract_value_from_page_text(body_text, label)
        if v:
            data[col] = v


def _apply_text_fallback(page, data: dict[str, Any]) -> None:
    """
    기존 css selector 파싱 실패 시, body 텍스트 기반으로 핵심 필드를 보완한다.
    """
    try:
        body_text = page.locator("body").first.inner_text()
    except Exception:
        body_text = ""
    body_text = re.sub(r"[ \t]+", " ", body_text or "")

    # car_name fallback
    if not str(data.get("car_name") or "").strip():
        for sel in (".css-1ugrlhy", "h1", "title"):
            try:
                t = page.locator(sel).first.inner_text().strip()
                if t:
                    data["car_name"] = re.sub(r"\s+", " ", t)
                    break
            except Exception:
                continue

    label_to_col = {
        "연식": "year",
        "주행거리": "km",
        "사고": "accident",
        "자차 보험처리": "insurance",
        "헤이딜러 보증": "guarantee",
        "환불": "refund",
        "실내 세차": "inner_car_wash",
        "타이어": "tire",
        "틴팅": "tinting",
        "차 키": "car_key",
    }
    for label, col in label_to_col.items():
        if str(data.get(col) or "").strip():
            continue
        v = _extract_value_from_page_text(body_text, label)
        if v:
            data[col] = v

    # 주행거리 형식 보정(예: "20,310 km" -> "20,310km")
    if data.get("km"):
        data["km"] = str(data["km"]).replace(" km", "km").strip()


def _heydealer_img_src(img_locator, page_url: str) -> str:
    try:
        s = (img_locator.get_attribute("src") or img_locator.get_attribute("data-src") or "").strip()
        if not s or "svg" in s.lower():
            return ""
        return s if s.startswith("http") else urljoin(page_url or "https://www.heydealer.com/", s)
    except Exception:
        return ""


def _collect_heydealer_detail_gallery_urls(page) -> list[str]:
    """
    .css-1uus6sd > .css-12qft46 내부:
    - 2번째 .css-ltrevz: ... button.css-tt5cop img.css-q38rgl
    - 4번째 .css-ltrevz: ... .css-hf19cn .css-1a3591h img.css-158t7i4 및 .css-w9nhgi img.css-158t7i4
    """
    seen: set[str] = set()
    urls: list[str] = []
    page_url = page.url or ""

    try:
        root = page.locator(".css-1uus6sd .css-12qft46").first
        if root.count() == 0:
            return []
        try:
            root.scroll_into_view_if_needed(timeout=8000)
            page.wait_for_timeout(150)
        except Exception:
            pass

        ltrevz = root.locator(".css-ltrevz")
        n_sec = ltrevz.count()

        if n_sec >= 2:
            sec = ltrevz.nth(1)
            imgs = sec.locator(".css-5pr39e .css-1i3qy3r .css-1dpi6xl button.css-tt5cop img.css-q38rgl")
            for i in range(imgs.count()):
                u = _heydealer_img_src(imgs.nth(i), page_url)
                if u and u not in seen:
                    seen.add(u)
                    urls.append(u)

        if n_sec >= 4:
            sec = ltrevz.nth(3)
            for sel in (
                ".css-5pr39e .css-1i3qy3r .css-hf19cn .css-1a3591h img.css-158t7i4",
                ".css-w9nhgi img.css-158t7i4",
            ):
                imgs = sec.locator(sel)
                for i in range(imgs.count()):
                    u = _heydealer_img_src(imgs.nth(i), page_url)
                    if u and u not in seen:
                        seen.add(u)
                        urls.append(u)
    except Exception as e:
        logging.debug("갤러리 URL 수집 실패: %s", e)

    return urls


def _download_detail_gallery_image(page, image_url: str, save_path: Path) -> bool:
    if not images_enabled():
        return False
    try:
        headers = {
            "Referer": (page.url or "https://www.heydealer.com/").split("#")[0],
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


def _parse_inspection(record_items_locator, label_text: str) -> str:
    """
    css-1p1dktr 목록에서 label_text 와 일치하는 항목의
    css-eirbbd 행을 "키 : 값 | 키 : 값" 형식으로 반환.
    """
    try:
        count = record_items_locator.count()
        for i in range(count):
            item = record_items_locator.nth(i)
            try:
                title = item.locator(".css-13alljm .css-1njdrvn").first.inner_text().strip()
            except Exception:
                continue
            if label_text not in title:
                continue
            try:
                detail_rows = item.locator(".css-1ajetyb .css-eirbbd")
                parts = []
                for di in range(detail_rows.count()):
                    dr = detail_rows.nth(di)
                    try:
                        k = dr.locator(".css-1b7o1k1").first.inner_text().strip()
                        v = re.sub(
                            r"\s+", " ",
                            (dr.locator(".css-nr5phs").first.inner_text() or "")
                        ).strip()
                        if k:
                            parts.append(f"{k} : {v}")
                    except Exception:
                        continue
                return " | ".join(parts)
            except Exception:
                return ""
    except Exception:
        pass
    return ""


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
    """단일 상세페이지 크롤링. 성공 시 dict 반환, 실패 시 None."""
    d_pnttm, c_dt = _get_now_times()
    data: dict[str, Any] = {f: "" for f in DETAIL_CSV_FIELDS}
    data["model_sn"]        = idx
    data["product_id"]      = product_id
    data["car_imgs"]        = str(detail_img_dir.resolve())
    data["date_crtr_pnttm"] = d_pnttm
    data["create_dt"]       = c_dt

    # 페이지 로드 (최대 3회 재시도)
    for attempt in range(3):
        try:
            goto_with_retry(
                page,
                GotoSpec(
                    detail_url,
                    wait_until="commit",
                    timeout_ms=90_000,
                    ready_selectors=(".css-1uus6sd,.css-12qft46,body",),
                    ready_timeout_ms=20_000,
                ),
                logger=logging.getLogger(__name__),
                attempts=1,
            )
            page.wait_for_timeout(250)
            break
        except Exception as e:
            if attempt < 2:
                logging.warning("재시도 (%d/3): %s - %s", attempt + 2, product_id, e)
                time.sleep(2)
            else:
                logging.error("접속 실패: %s - %s", product_id, e)
                return None

    try:
        # 본문 영역(.css-1uus6sd .css-12qft46) 안의 ltrevz만 쓰면 nth(0~4)가 페이지 전역
        # 헤더·배너 등 추가 ltrevz에 밀리지 않음 (레이 EV 등에서 타이어 섹션 누락 방지).
        wrap = page.locator(".css-1uus6sd .css-12qft46")
        sections = wrap.first.locator(".css-ltrevz") if wrap.count() > 0 else page.locator(
            ".css-ltrevz"
        )
        try:
            if wrap.count() > 0:
                try:
                    wrap.first.scroll_into_view_if_needed(timeout=8000)
                except Exception:
                    pass
                page.wait_for_timeout(150)
            # 상세 섹션 늦게 뜨는 케이스 보완
            if sections.count() == 0:
                page.wait_for_timeout(400)
                if sections.count() == 0:
                    page.reload(wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(400)
            elif wrap.count() > 0 and sections.count() < 5:
                page.wait_for_timeout(300)
        except Exception:
            pass

        # ── 섹션 1 : car_name + 기본 스펙 ─────────────────────────────────
        try:
            sec1 = sections.nth(0)

            # car_name = css-1ugrlhy + css-pjgjzs span 합치기
            try:
                name_main = re.sub(
                    r"\s+", " ",
                    sec1.locator(".css-1ugrlhy").first.inner_text()
                ).strip()
                sub_spans = sec1.locator(".css-pjgjzs span")
                sub_parts = []
                for si in range(sub_spans.count()):
                    t = sub_spans.nth(si).inner_text().strip()
                    if t:
                        sub_parts.append(t)
                data["car_name"] = " ".join(filter(None, [name_main] + sub_parts))
            except Exception:
                pass

            data.update(_parse_kv_rows(
                sec1.locator(".css-c9qil9"),
                row_class="css-113wzqa",
                key_class="css-1b7o1k1",
                key_map={
                    "연식":         "year",
                    "주행거리":      "km",
                    "사고":         "accident",
                    "자차 보험처리":  "insurance",
                    "헤이딜러 보증":  "guarantee",
                    "환불":         "refund",
                    "실내 세차":     "inner_car_wash",
                },
            ))
        except Exception as e:
            logging.debug("[섹션1] %s : %s", product_id, e)

        # ── 섹션 2 : exterior_description / interior_description ───────────
        try:
            sec2      = sections.nth(1)
            desc_rows = sec2.locator(".css-1i3qy3r .css-1cfq7ri .css-113wzqa")
            for di in range(desc_rows.count()):
                dr = desc_rows.nth(di)
                try:
                    label    = dr.locator(".css-1b7o1k1").first.inner_text().strip()
                    val      = ""
                    val_divs = dr.locator("> div")
                    for vi in range(val_divs.count()):
                        vd  = val_divs.nth(vi)
                        cls = vd.get_attribute("class") or ""
                        if "css-1b7o1k1" not in cls:
                            val = re.sub(r"\s+", " ", vd.inner_text() or "").strip()
                            break
                    if label == "외부":
                        data["exterior_description"] = val
                    elif label == "실내":
                        data["interior_description"] = val
                except Exception:
                    continue
        except Exception as e:
            logging.debug("[섹션2] %s : %s", product_id, e)

        # ── 섹션 3 : main_option / delivery_information / recommendation_comment
        try:
            sec3 = sections.nth(2)

            try:
                data["main_option"] = _safe_texts_join(
                    sec3.locator(".css-vsdo2k .css-g5wwb2 .css-13wylk3"), " | "
                )
            except Exception:
                pass

            try:
                deliv_items = sec3.locator(".css-1i3qy3r .css-1cfq7ri")
                deliv_parts = []
                for di in range(deliv_items.count()):
                    ds = deliv_items.nth(di)
                    try:
                        title   = ds.locator(".css-1njdrvn").first.inner_text().strip()
                        content = re.sub(
                            r"\n+", " | ",
                            ds.locator(".css-1n3oo4w").first.inner_text().strip()
                        )
                        if title or content:
                            deliv_parts.append(
                                f"{title} : {content}" if title else content
                            )
                    except Exception:
                        continue
                data["delivery_information"] = " | ".join(deliv_parts)
            except Exception:
                pass

            try:
                data["recommendation_comment"] = _safe_texts_join(
                    sec3.locator(".css-isc2b5 .css-yfldxx"), " | "
                )
            except Exception:
                pass
        except Exception as e:
            logging.debug("[섹션3] %s : %s", product_id, e)

        # ── 섹션 4 : tire / tinting / car_key ─────────────────────────────
        try:
            sec4 = sections.nth(3)
            data.update(_parse_kv_rows(
                sec4.locator(".css-1i3qy3r .css-1cfq7ri"),
                row_class="css-113wzqa",
                key_class="css-1b7o1k1",
                key_map={
                    "타이어": "tire",
                    "틴팅":  "tinting",
                    "차 키": "car_key",
                },
            ))
        except Exception as e:
            logging.debug("[섹션4] %s : %s", product_id, e)

        # ── 섹션 5 : inspection_records_1 / inspection_records_2 ──────────
        try:
            sec5         = sections.nth(4)
            record_items = sec5.locator(
                ".css-5pr39e .css-7y52wh .css-1b6pogx "
                ".css-154rxsx .css-dkq6n3 .css-1p1dktr"
            )
            data["inspection_records_1"] = _parse_inspection(record_items, "단순교환 무사고")
            data["inspection_records_2"] = _parse_inspection(record_items, "하부 정상")
        except Exception as e:
            logging.debug("[섹션5] %s : %s", product_id, e)

        # ── 상세 갤러리 이미지 (car_imgs 컬럼은 저장 디렉터리 절대경로, 파일은 detail/{product_id}_N.png)
        try:
            gallery_urls = _collect_heydealer_detail_gallery_urls(page)
            for gi, gurl in enumerate(gallery_urls, 1):
                out = detail_img_dir / f"{product_id}_{gi}.png"
                _download_detail_gallery_image(page, gurl, out)
        except Exception as e:
            logging.debug("[갤러리 이미지] %s : %s", product_id, e)

    except Exception as e:
        logging.error("파싱 전체 오류: %s - %s", product_id, e)
        return None

    # 핵심 값이 거의 비어 있으면 텍스트 기반 fallback으로 보완
    core_cols = ("car_name", "year", "km", "accident", "insurance", "guarantee", "refund")
    filled_core = sum(1 for c in core_cols if str(data.get(c) or "").strip())
    if filled_core <= 2:
        _apply_text_fallback(page, data)

    _apply_detail_extras_fallback(page, data)

    return data