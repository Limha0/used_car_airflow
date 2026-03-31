from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class GotoSpec:
    url: str
    wait_until: str = "commit"  # commit이 domcontentloaded보다 timeout에 강함
    timeout_ms: int = 90_000
    ready_selectors: tuple[str, ...] = ()
    ready_timeout_ms: int = 30_000


def install_route_blocking(
    context,
    *,
    block_resource_types: Iterable[str] = ("image", "media", "font"),
    block_url_substrings: Sequence[str] = (
        "googletagmanager.com",
        "google-analytics.com",
        "doubleclick.net",
        "googlesyndication.com",
        "adsystem.com",
        "facebook.net",
        "facebook.com/tr",
        "hotjar.com",
        "clarity.ms",
        "naver.com/analytics",
        "dable.io",
        "criteo.com",
    ),
) -> None:
    """
    페이지 렌더링에 불필요한 리소스를 차단해 속도/안정성을 높인다.
    - 이미지 다운로드는 별도로 page.request로 하는 경우가 많아 렌더링 이미지 요청은 차단해도 무방.
    """

    blocked_types = set(block_resource_types)
    blocked_subs = tuple(s.lower() for s in (block_url_substrings or ()))

    def _route_filter(route, request):
        try:
            if request.resource_type in blocked_types:
                route.abort()
                return
        except Exception:
            pass
        # 분석/광고 트래픽은 페이지 준비를 늦추는 경우가 많아 안전하게 차단
        try:
            if blocked_subs:
                u = (request.url or "").lower()
                if u and any(sub in u for sub in blocked_subs):
                    route.abort()
                    return
        except Exception:
            pass
        try:
            route.continue_()
        except Exception:
            pass

    try:
        context.route("**/*", _route_filter)
    except Exception:
        # route는 컨텍스트/브라우저 상태에 따라 실패할 수 있으니 무시
        return


def goto_with_retry(page, spec: GotoSpec, *, logger: logging.Logger | None = None, attempts: int = 3) -> None:
    """
    - goto 재시도
    - ready selector 기반 대기
    - 중간에 팝업/리다이렉트 등으로 늦어져도 timeout에 덜 민감하게
    """
    log = logger or logging.getLogger(__name__)
    last_err: Exception | None = None

    for attempt in range(attempts):
        try:
            page.goto(spec.url, wait_until=spec.wait_until, timeout=spec.timeout_ms)
            if spec.ready_selectors:
                page.wait_for_selector(",".join(spec.ready_selectors), timeout=spec.ready_timeout_ms)
            return
        except Exception as e:
            last_err = e
            if attempt >= attempts - 1:
                break
            log.warning("goto 재시도(%d/%d): url=%s err=%s", attempt + 2, attempts, spec.url, e)
            try:
                page.wait_for_timeout(800)
                page.reload(wait_until="domcontentloaded", timeout=spec.timeout_ms)
            except Exception:
                pass
            time.sleep(1 + attempt)

    raise last_err if last_err is not None else RuntimeError(f"goto 실패: {spec.url}")


def safe_wait_networkidle(page, *, timeout_ms: int = 3000) -> None:
    """
    networkidle은 사이트에 따라 영원히 안 끝날 수 있어서 best-effort로만 사용.
    """
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception:
        return


def wait_for_any_selector(page, selectors: Sequence[str], *, timeout_ms: int = 10_000) -> str:
    """
    selectors 중 하나라도 먼저 나타나면 해당 selector를 반환.
    모두 실패하면 빈 문자열 반환.
    """
    if not selectors:
        return ""
    joined = ",".join([s for s in selectors if s])
    if not joined:
        return ""
    try:
        page.wait_for_selector(joined, timeout=timeout_ms)
    except Exception:
        return ""
    # 어떤 것이 매칭됐는지 확인(가장 앞의 visible 하나)
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                return sel
        except Exception:
            continue
    return ""


def is_playwright_crash_error(err: BaseException) -> bool:
    msg = str(err)
    return ("Target crashed" in msg) or ("Page crashed" in msg)


def images_enabled() -> bool:
    """
    이미지 다운로드는 트래픽/시간을 가장 많이 잡아먹는다.
    USED_CAR_SKIP_IMAGES=1(true/yes) 이면 전체 이미지 다운로드를 스킵한다.
    """
    v = os.environ.get("USED_CAR_SKIP_IMAGES", "0").strip().lower()
    return v not in ("1", "true", "yes", "y")

