"""
个股深度分析 — 网页端
====================
输入 A 股代码 → 后台采集 akshare 数据 → 浏览器直接渲染完整 HTML 报告。

启动:
  uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
"""

from __future__ import annotations

import json
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data_fetcher import collect, detect_market, render_html  # noqa: E402

WEB_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "output"
CACHE_DIR = OUTPUT_DIR / "web_cache"
TEMPLATE_DIR = WEB_DIR / "templates"


def _render_template(name: str, **ctx: str) -> str:
    text = (TEMPLATE_DIR / name).read_text(encoding="utf-8")
    for key, value in ctx.items():
        text = text.replace("{{ " + key + " }}", str(value))
        text = text.replace("{{" + key + "}}", str(value))
    return text

DEFAULT_MAX_KLINE_YEARS = 3
CACHE_TTL_SECONDS = 3600
_executor = ThreadPoolExecutor(max_workers=2)


def normalize_code(raw: str) -> str:
    """从输入中提取 6 位 A 股代码。"""
    text = (raw or "").strip().upper()
    text = text.replace(".", "").replace(" ", "")
    for prefix in ("SH", "SZ", "BJ"):
        if text.startswith(prefix) and len(text) >= 8:
            text = text[2:]
    match = re.search(r"\d{6}", text)
    if not match:
        raise ValueError("请输入有效的 6 位 A 股代码，例如 600519 或 000066")
    return match.group(0)


def cache_path(code: str) -> Path:
    return CACHE_DIR / f"stock_report_{code}.html"


def read_cache(code: str) -> str | None:
    path = cache_path(code)
    if not path.is_file():
        return None
    age = time.time() - path.stat().st_mtime
    if age > CACHE_TTL_SECONDS:
        return None
    return path.read_text(encoding="utf-8")


def write_cache(code: str, html_text: str) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path(code).write_text(html_text, encoding="utf-8")


JobStatus = Literal["pending", "running", "done", "error"]


@dataclass
class ReportJob:
    code: str
    status: JobStatus = "pending"
    message: str = "排队中…"
    html: str | None = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    max_kline_years: int = DEFAULT_MAX_KLINE_YEARS


_jobs: dict[str, ReportJob] = {}
_jobs_lock = threading.Lock()


def _set_job(job_id: str, **kwargs: Any) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        for key, value in kwargs.items():
            setattr(job, key, value)


def _run_report(job_id: str) -> None:
    with _jobs_lock:
        job = _jobs[job_id]
        code = job.code
        years = job.max_kline_years

    cached = read_cache(code)
    if cached:
        _set_job(job_id, status="done", message="已从缓存加载", html=cached)
        return

    try:
        _set_job(job_id, status="running", message="正在采集行情与财务数据（约 2–5 分钟）…")
        detect_market(code)
        data = collect(code, max_kline_years=years)
        _set_job(job_id, message="正在生成 HTML 报告…")
        html_text = render_html(data)
        write_cache(code, html_text)
        _set_job(job_id, status="done", message="报告已生成", html=html_text)
    except Exception as exc:
        _set_job(job_id, status="error", message="生成失败", error=str(exc))


def start_job(code: str, max_kline_years: int = DEFAULT_MAX_KLINE_YEARS) -> str:
    cached = read_cache(code)
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = ReportJob(
            code=code,
            status="done" if cached else "pending",
            message="已从缓存加载" if cached else "排队中…",
            html=cached,
            max_kline_years=max_kline_years,
        )
    if not cached:
        _executor.submit(_run_report, job_id)
    return job_id


app = FastAPI(
    title="个股深度分析",
    description="A 股个股全维度 HTML 报告 — 网页端",
    version="1.0.0",
)

app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html_text = _render_template("index.html", default_code="000066")
    return HTMLResponse(content=html_text)


@app.get("/report/{code}", response_class=HTMLResponse)
async def report_loading(
    code: str,
    years: int = Query(DEFAULT_MAX_KLINE_YEARS, ge=1, le=10, alias="years"),
) -> HTMLResponse:
    try:
        normalized = normalize_code(code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    job_id = start_job(normalized, max_kline_years=years)
    html_text = _render_template(
        "loading.html",
        code=normalized,
        job_id=json.dumps(job_id),
    )
    return HTMLResponse(content=html_text)


@app.post("/api/report")
async def api_start_report(
    code: str = Query(..., description="6 位 A 股代码"),
    years: int = Query(DEFAULT_MAX_KLINE_YEARS, ge=1, le=10),
) -> dict[str, Any]:
    try:
        normalized = normalize_code(code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    job_id = start_job(normalized, max_kline_years=years)
    cached = read_cache(normalized) is not None
    return {
        "job_id": job_id,
        "code": normalized,
        "cached": cached,
        "view_url": f"/view/{normalized}",
        "status_url": f"/api/jobs/{job_id}",
    }


@app.get("/api/jobs/{job_id}")
async def api_job_status(job_id: str) -> dict[str, Any]:
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="任务不存在或已过期")

    payload: dict[str, Any] = {
        "job_id": job_id,
        "code": job.code,
        "status": job.status,
        "message": job.message,
    }
    if job.status == "done":
        payload["view_url"] = f"/view/{job.code}"
    if job.status == "error":
        payload["error"] = job.error
    return payload


@app.get("/view/{code}", response_model=None)
async def view_report(
    code: str,
    refresh: bool = Query(False, description="忽略缓存并重新生成"),
):
    try:
        normalized = normalize_code(code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if refresh:
        path = cache_path(normalized)
        if path.is_file():
            path.unlink()

    cached = read_cache(normalized)
    if cached:
        return HTMLResponse(content=cached)

    job_id = start_job(normalized)
    return RedirectResponse(url=f"/report/{normalized}?job={job_id}", status_code=302)


@app.get("/api/jobs/{job_id}/html", response_class=HTMLResponse)
async def api_job_html(job_id: str) -> HTMLResponse:
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="任务不存在")
    if job.status != "done" or not job.html:
        raise HTTPException(status_code=409, detail=job.message or "报告尚未就绪")
    return HTMLResponse(content=job.html)


@app.get("/examples/demo", response_class=HTMLResponse)
async def demo_report() -> FileResponse:
    demo = ROOT / "examples" / "个股研究-中国长城.html"
    if not demo.is_file():
        raise HTTPException(status_code=404, detail="示例报告不存在")
    return FileResponse(demo, media_type="text/html; charset=utf-8")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
