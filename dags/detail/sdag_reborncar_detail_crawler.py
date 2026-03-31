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
from playwright.sync_api import sync_playwright

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from util.common_util import CommonUtil
from util.playwright_util import GotoSpec, goto_with_retry, install_route_blocking


# ═══════════════════════════════════════════════════════════════════
#  상수
# ═══════════════════════════════════════════════════════════════════

SOURCE_LIST_TABLE = "ods.ods_car_list_reborncar"
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
#  DAG 정의
# ═══════════════════════════════════════════════════════════════════


@dag(
    dag_id="sdag_reborncar_detail_crawl",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["used_car", "reborncar", "detail", "crawler"],
)
def reborncar_detail_crawl():
    """리본카 상세페이지 크롤링 DAG (register_flag != 'N' 전체)."""

    @task
    def fetch_target_urls() -> list[dict[str, str]]:
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
        rows: list[dict[str, str]] = []
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
        n = len(target_rows)
        with_url = sum(1 for r in target_rows if str(r.get("detail_url") or "").strip())
        logging.info("상세 크롤 준비: 총 %d건, detail_url 있음 %d건", n, with_url)
        if not target_rows:
            raise ValueError("summarize_targets: 대상이 비어 있습니다.")
        return target_rows

    @task
    def crawl_and_save_csv(target_rows: list[dict[str, str]]) -> str:
        # Airflow DAG 파싱 단계에서 playwright 미설치/무거운 import로 불안정해지는 문제 방지
        from playwright.sync_api import sync_playwright

        output_dir = _get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now().strftime("%Y%m%d%H%M")
        csv_path = output_dir / f"reborncar_detail_{run_ts}.csv"
        logging.info("출력 파일: %s", csv_path)
        # 수집 결과가 0건이어도 파일은 항상 생성(헤더 포함)되도록 한다.
        if not csv_path.exists():
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=DETAIL_CSV_FIELDS)
                w.writeheader()

        detail_img_dir = _get_detail_img_dir()
        detail_img_dir.mkdir(parents=True, exist_ok=True)
        CommonUtil.clear_image_files(detail_img_dir, recursive=False)
        logging.info("상세 이미지 저장 디렉터리: %s", detail_img_dir.resolve())

        total = len(target_rows)
        collected = 0
        failed = 0
        skipped = 0
        # 장시간 실행 시 누적 메모리/리소스 이슈를 줄이기 위해 더 자주 재생성
        recycle_every = 100

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

            def _new_context_and_page():
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1920, "height": 1080},
                )

                # 이미지/폰트/미디어는 페이지 렌더에만 필요하고,
                # 우리는 src만 뽑아서 page.request로 따로 다운로드하므로 차단해 크래시 확률을 줄인다.
                def _route_filter(route, request):
                    try:
                        if request.resource_type in ("image", "media", "font"):
                            route.abort()
                            return
                    except Exception:
                        pass
                    route.continue_()

                try:
                    ctx.route("**/*", _route_filter)
                except Exception:
                    pass

                pg = ctx.new_page()
                pg.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )
                return ctx, pg

            context, page = _new_context_and_page()

            for idx, row in enumerate(target_rows, 1):
                product_id = str(row.get("product_id") or "").strip()
                detail_url = str(row.get("detail_url") or "").strip()

                if not detail_url:
                    skipped += 1
                    continue

                if idx == 1 or idx % 50 == 0 or idx == total:
                    logging.info(
                        "[%d/%d] 호출 대상 - product_id=%s, detail_url=%s",
                        idx,
                        total,
                        product_id,
                        detail_url,
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
                        idx,
                        total,
                        product_id,
                        detail_url,
                        e,
                    )
                    # page/context 크래시가 나면 같은 page로 계속하면 연쇄 실패하므로 즉시 재생성
                    msg = str(e)
                    if "Target crashed" in msg or "Page crashed" in msg:
                        try:
                            page.close()
                        except Exception:
                            pass
                        try:
                            context.close()
                        except Exception:
                            pass
                        context, page = _new_context_and_page()

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
                    context, page = _new_context_and_page()
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
            raise FileNotFoundError(f"CSV 생성 실패(경로/권한 확인): {csv_path}")
        return str(csv_path)

    @task_group(group_id="prepare_detail_crawl")
    def prepare_detail_crawl():
        rows = fetch_target_urls()
        return summarize_targets(rows)

    @task_group(group_id="crawl_and_persist")
    def crawl_and_persist(target_rows: list[dict[str, str]]):
        return crawl_and_save_csv(target_rows)

    prepared = prepare_detail_crawl()
    crawl_and_persist(prepared)


dag_object = reborncar_detail_crawl()


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


def _join_kv_pairs(pairs: list[tuple[str, str]]) -> str:
    parts: list[str] = []
    for k, v in pairs:
        k2 = _norm_space(k)
        v2 = _norm_space(v)
        if k2 and v2:
            parts.append(f"{k2} : {v2}")
        elif k2:
            parts.append(k2)
        elif v2:
            parts.append(v2)
    return " | ".join(parts)


def _parse_count_blocks(container_locator) -> str:
    """
    예: <div class="sheeting-count">판금 <span>1건</span></div>
        <div class="change-count">교환 <span>1건</span></div>
    -> "판금 : 1건 | 교환 : 1건"
    """
    try:
        blocks = container_locator.locator("> div")
        pairs: list[tuple[str, str]] = []
        for i in range(blocks.count()):
            b = blocks.nth(i)
            label_raw = ""
            try:
                label_raw = b.evaluate(
                    """el => {
                        const sp = el.querySelector('span');
                        const t = (el.textContent || '').trim();
                        if (!sp) return t;
                        const v = (sp.textContent || '').trim();
                        return t.replace(v, '').trim();
                    }"""
                )
            except Exception:
                label_raw = _safe_text(b)
            val = _safe_text(b.locator("span"))
            if label_raw or val:
                pairs.append((label_raw, val))
        return _join_kv_pairs(pairs)
    except Exception:
        return ""


def _parse_tire_summary(box) -> str:
    tread_title = _safe_text(box.locator(".tire-tread .trad-title"))
    tread_val = _safe_text(box.locator(".tire-tread .trad-txt"))
    date_title = _safe_text(box.locator(".tire-date .date-title"))
    date_val = _safe_text(box.locator(".tire-date .date-txt"))
    return _join_kv_pairs([(tread_title, tread_val), (date_title, date_val)])


def _download_image(page, image_url: str, save_path: Path) -> bool:
    try:
        headers = {
            "Referer": (page.url or "").split("#")[0] or "https://www.reborncar.co.kr/",
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
                    ready_selectors=("#wrap .vip-section,.vip-section,body",),
                    ready_timeout_ms=20_000,
                ),
                logger=logging.getLogger(__name__),
                attempts=1,
            )
            # 고정 1.8초 대신 짧게만 대기(대부분 selector 로드로 충분)
            page.wait_for_timeout(300)
            break
        except Exception as e:
            if attempt < 2:
                logging.warning("재시도 (%d/3): %s - %s", attempt + 2, product_id, e)
                time.sleep(2)
            else:
                logging.error("접속 실패: %s - %s", product_id, e)
                return None

    root = page.locator("#wrap .vip-section").first
    if root.count() == 0:
        root = page.locator(".vip-section").first

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

            # plan_pay
            pay_ul = car_infos.nth(1).locator(".car-sub-pay .plan-pay").first
            li = pay_ul.locator("li")
            pay_parts: list[str] = []
            for i in range(li.count()):
                item = li.nth(i)
                raw = _norm_space(item.inner_text() or "")
                if not raw:
                    continue
                name = raw
                try:
                    sp = _safe_text(item.locator("span"))
                    strong = _safe_text(item.locator("strong"))
                    # base label: 텍스트에서 숫자/개월 제거가 어려워서 첫 단어를 라벨로 사용
                    label = _norm_space((raw.split(" ")[0] if raw.split(" ") else raw))
                    amount = _norm_space((sp or ""))
                    month = _norm_space((strong or "")).strip()
                    if amount and "만원" not in amount:
                        amount = amount + "만원"
                    if label and amount:
                        pay_parts.append(f"{label} : {amount} {month}".strip())
                    else:
                        pay_parts.append(raw)
                except Exception:
                    pay_parts.append(name)
            data["plan_pay"] = " | ".join([p.strip() for p in pay_parts if p.strip()])

        # 신차 출고가
        info_section = root.locator(".vip-body .vip-con .con-section.info").first
        head_list = info_section.locator(".vip-car-info .vip-car-info-head .info-list li")
        for i in range(head_list.count()):
            li = head_list.nth(i)
            title = _safe_text(li.locator(".title"))
            if title == "신차 출고가":
                data["car_new_price"] = _safe_text(li.locator(".txt .sub-pay.car-new-price"))
                break

        # info-list-con 6개
        body_list = info_section.locator(".vip-car-info .vip-car-info-body .info-list .info-list-con")
        for i in range(body_list.count()):
            con = body_list.nth(i)
            label = _safe_text(con.locator(".list-con .info-txt"))
            # 라벨별로 info-tit의 세부 클래스가 다를 수 있어 우선순위 셀렉터를 둔다.
            val = ""
            if label == "사고여부":
                val = _safe_text(con.locator(".list-con .info-tit.aci-gbn"))
            elif label == "침수여부":
                val = _safe_text(con.locator(".list-con .info-tit"))
            elif label == "용도변경":
                val = _safe_text(con.locator(".list-con .info-tit.special-carhistory"))
            elif label == "신차가격대비":
                val = _safe_text(con.locator(".list-con .info-tit.relamt-per-parent"))
            elif label == "냄새등급":
                val = _safe_text(con.locator(".list-con .info-tit.smell-grade"))
            elif label == "안심환불":
                val = _safe_text(con.locator(".list-con .info-tit"))
            if not val:
                val = _safe_text(con.locator(".list-con .info-tit"))
            if label == "사고여부":
                data["aci_gbn"] = val or "-"
            elif label == "침수여부":
                data["info_tit_1"] = val or "-"
            elif label == "용도변경":
                data["special_carhistory"] = val or "-"
            elif label == "신차가격대비":
                data["relamt_per_parent"] = val or "-"
            elif label == "냄새등급":
                data["smell_grade"] = val or "-"
            elif label == "안심환불":
                data["info_tit_2"] = val or "-"

        for col in (
            "aci_gbn",
            "info_tit_1",
            "special_carhistory",
            "relamt_per_parent",
            "smell_grade",
            "info_tit_2",
        ):
            if not str(data.get(col) or "").strip():
                data[col] = "-"

        # 옵션 리스트
        option_section = root.locator(".vip-con .con-section.option").first
        opt_txt = option_section.locator(".vip-cont .vip-option-form .vip-option-list .vip-option-con .vip-option-txt")
        opt_parts: list[str] = []
        seen_opt: set[str] = set()
        for i in range(opt_txt.count()):
            t = _safe_text(opt_txt.nth(i))
            if t and t not in seen_opt:
                seen_opt.add(t)
                opt_parts.append(t)
        data["vip_option"] = " | ".join(opt_parts)

        # 추가 옵션
        add_con = option_section.locator(".vip-cont.selected-option.add_choice_option .vip-add-option .add-option-list .add-option-con")
        add_parts: list[str] = []
        for i in range(add_con.count()):
            c = add_con.nth(i)
            title = _safe_text(c.locator(".add-option-title"))
            pay = _safe_text(c.locator(".add-option-pay"))
            if title and pay:
                add_parts.append(f"{title}({pay})")
            elif title:
                add_parts.append(title)
        data["add_option"] = " | ".join([p for p in add_parts if p])

        # AQI 섹션
        aqi_section = root.locator(".vip-body .vip-con .con-section.aqi").first
        # figure panel / frame
        figure_form = aqi_section.locator(".vip-cont .car-figure-form .car-figure-info .car-figure-info-list").first
        panel_cont = figure_form.locator(".figure-panel .cont.sheeting-status")
        frame_cont = figure_form.locator(".figure-frame .cont.change-status")
        data["figure_panel"] = _parse_count_blocks(panel_cont.first) if panel_cont.count() else "-"
        data["figure_frame"] = _parse_count_blocks(frame_cont.first) if frame_cont.count() else "-"
        if not str(data["figure_panel"]).strip():
            data["figure_panel"] = "-"
        if not str(data["figure_frame"]).strip():
            data["figure_frame"] = "-"

        # aqi_list (title : status)
        aqi_group = aqi_section.locator(".vip-cont .vip-aqi-form .vip-aqi-box .vip-aqi-cont .vip-aqi-list.vip-aqi-group .aqi-list")
        aqi_pairs: list[tuple[str, str]] = []
        for i in range(aqi_group.count()):
            it = aqi_group.nth(i)
            aqi_pairs.append((_safe_text(it.locator(".title")), _safe_text(it.locator(".status"))))
        data["aqi_list"] = _join_kv_pairs(aqi_pairs)

        # aqi_notice_list (title : txt)
        notice_items = aqi_section.locator(".vip-cont .vip-aqi-notice-form .vip-aqi-notice-box .vip-aqi-notice-cont .vip-aqi-notice-list .aqi-notice-list")
        notice_pairs: list[tuple[str, str]] = []
        for i in range(notice_items.count()):
            it = notice_items.nth(i)
            box = it.locator(".aqi-notice-list-txt").first
            notice_pairs.append((_safe_text(box.locator(".title")), _safe_text(box.locator(".txt"))))
        data["aqi_notice_list"] = _join_kv_pairs(notice_pairs)

        # tire summaries
        another = aqi_section.locator(".vip-cont .aqi-another-form .aqi-another-box").first
        for cls, col in (
            (".aqi-tire .cont.aqi-tire-tread .tire-summery.front.left", "tire_summery_front_left"),
            (".aqi-tire .cont.aqi-tire-tread .tire-summery.front.right", "tire_summery_front_right"),
            (".aqi-tire .cont.aqi-tire-tread .tire-summery.back.left", "tire_summery_back_left"),
            (".aqi-tire .cont.aqi-tire-tread .tire-summery.back.right", "tire_summery_back_right"),
        ):
            box = another.locator(cls).first
            data[col] = _parse_tire_summary(box) if box.count() else "-"
            if not str(data[col]).strip():
                data[col] = "-"

        # battery
        data["battey_count"] = "-"
        bat = another.locator(".aqi-another-con .aqi-battey .cont.bettey-exist").first
        if bat.count():
            percent = _safe_text(bat.locator(".bettery-info .battey-count"))
            percent = _norm_space(percent).replace(" ", "")
            comment = _safe_text(bat.locator(".bettey-comment"))
            parts = [p for p in (percent, comment) if p]
            if parts:
                data["battey_count"] = " | ".join(parts)

        # brand_surety (4번째 vip-cont)
        surety_items = aqi_section.locator(".vip-cont .brand-surety-form .brand-surety-new .brand-surety-con")
        def _parse_surety_item(item) -> str:
            surety_cons = item.locator(".surety-list-con .surety-con")
            pairs: list[tuple[str, str]] = []
            for i in range(surety_cons.count()):
                sc = surety_cons.nth(i)
                k = _safe_text(sc.locator(".surety-con-head .txt"))
                cont = _safe_text(sc.locator(".surety-con-head .cont-txt"))
                if "보증 만료" in cont:
                    cont = "보증 만료"
                cont = cont.replace(" 까지", "까지").strip()
                pairs.append((k, cont))
            return _join_kv_pairs(pairs)

        data["brand_surety_con_1"] = _parse_surety_item(surety_items.nth(0)) if surety_items.count() >= 1 else "-"
        data["brand_surety_con_2"] = _parse_surety_item(surety_items.nth(1)) if surety_items.count() >= 2 else "-"
        if not str(data["brand_surety_con_1"]).strip():
            data["brand_surety_con_1"] = "-"
        if not str(data["brand_surety_con_2"]).strip():
            data["brand_surety_con_2"] = "-"

        # 이미지 저장
        try:
            page_url = page.url or ""
            img_nodes = []
            img_nodes.append(root.locator(".vip-visual .vip-visual-detail .vip-visual-detail .visual-detail .detail-img img"))
            img_nodes.append(root.locator(".vip-visual .vip-visual-list .visual-box .visual-con img"))
            seen: set[str] = set()
            idx_img = 0
            try:
                root.locator(".vip-visual").first.scroll_into_view_if_needed(timeout=5000)
                page.wait_for_timeout(200)
            except Exception:
                pass
            for loc in img_nodes:
                for i in range(loc.count()):
                    img = loc.nth(i)
                    src = (img.get_attribute("data-src") or img.get_attribute("src") or "").strip()
                    if not src:
                        continue
                    if src.startswith("//"):
                        src = "https:" + src
                    elif src.startswith("/"):
                        src = urljoin(page_url or "https://www.reborncar.co.kr/", src)
                    elif not src.startswith("http"):
                        src = urljoin(page_url or "https://www.reborncar.co.kr/", src)
                    if src in seen:
                        continue
                    seen.add(src)
                    idx_img += 1
                    out = detail_img_dir / f"{product_id}_{idx_img}.png"
                    _download_image(page, src, out)
        except Exception:
            pass

    except Exception as e:
        logging.error("파싱 전체 오류: %s - %s", product_id, e)
        return None

    core_cols = ("car_num", "car_name", "release_dt")
    if sum(1 for c in core_cols if str(data.get(c) or "").strip()) == 0:
        return None
    return data

