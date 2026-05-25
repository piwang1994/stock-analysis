"""数据采集进度（供网页端实时展示）。"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Literal

TaskStatus = Literal["pending", "running", "ok", "fail"]


@dataclass
class FetchTask:
    id: str
    group: str
    label: str
    block: str = ""
    status: TaskStatus = "pending"
    rows: int | None = None
    elapsed: float | None = None
    message: str | None = None


def build_collect_manifest(max_kline_years: int, market: str) -> list[dict[str, str]]:
    """与 data_fetcher.collect 顺序一致的采集任务清单。"""
    yj_periods = ["20240331", "20240630", "20240930", "20241231"]
    steps: list[dict[str, str]] = [
        {"id": "basic_info", "group": "1. 基础信息", "label": "雪球-公司概况", "block": "basic_info"},
        {"id": "spot", "group": "1. 基础信息", "label": "新浪-全市场快照", "block": "spot"},
        {"id": "share_structure", "group": "1. 基础信息", "label": "股本结构变动", "block": "share_structure"},
        {"id": "zygc", "group": "2. 主营业务", "label": "主营构成(行业/产品/地区)", "block": "zygc"},
        {"id": "kline_daily", "group": "3. 行情 K 线", "label": f"新浪-日K（{max_kline_years}年前复权）", "block": "kline_daily"},
        {"id": "kline_minute", "group": "3. 行情 K 线", "label": "新浪-1分钟分时", "block": "kline_minute"},
        {"id": "fund_flow", "group": "4. 资金流向", "label": "个股资金流向(近100日)", "block": "fund_flow"},
        {"id": "lhb", "group": "5. 龙虎榜", "label": "龙虎榜近30日", "block": "lhb"},
        {"id": "fin_abstract", "group": "6. 财务指标", "label": "财务摘要（按报告期）", "block": "fin_abstract"},
        {"id": "fin_indicator_ths", "group": "6. 财务指标", "label": "同花顺-关键指标", "block": "fin_indicator_ths"},
        {"id": "balance_sheet", "group": "7. 三大报表", "label": "资产负债表", "block": "balance_sheet"},
        {"id": "income_statement", "group": "7. 三大报表", "label": "利润表", "block": "income_statement"},
        {"id": "cashflow", "group": "7. 三大报表", "label": "现金流量表", "block": "cashflow"},
    ]
    for p in yj_periods:
        steps.append({"id": f"yjyg_{p}", "group": "8. 业绩预告/快报", "label": f"业绩预告 {p}", "block": "yjyg"})
        steps.append({"id": f"yjkb_{p}", "group": "8. 业绩预告/快报", "label": f"业绩快报 {p}", "block": "yjkb"})
    steps.extend([
        {"id": "top10", "group": "9. 股东结构", "label": "十大股东", "block": "top10"},
        {"id": "top10_free", "group": "9. 股东结构", "label": "十大流通股东", "block": "top10_free"},
        {"id": "gdhs", "group": "9. 股东结构", "label": "股东户数变动", "block": "gdhs"},
        {"id": "share_hold_change", "group": "9. 股东结构",
         "label": "高管持股变动（上交所）" if market == "sh" else "高管持股变动（深交所）",
         "block": "share_hold_change"},
        {"id": "dividend", "group": "10. 分红解禁", "label": "历史分红", "block": "dividend"},
        {"id": "share_alloc", "group": "10. 分红解禁", "label": "历史送转", "block": "share_alloc"},
        {"id": "release", "group": "10. 分红解禁", "label": "限售解禁排队", "block": "release"},
        {"id": "notice", "group": "11. 公告新闻", "label": "当日公告", "block": "notice"},
        {"id": "news", "group": "11. 公告新闻", "label": "个股新闻", "block": "news"},
        {"id": "research", "group": "11. 公告新闻", "label": "研究报告", "block": "research"},
        {"id": "recommend", "group": "12. 机构持仓", "label": "机构推荐评级", "block": "recommend"},
        {"id": "fund_hold", "group": "12. 机构持仓", "label": "基金持仓", "block": "fund_hold"},
        {"id": "margin", "group": "13. 融资融券",
         "label": "上交所融资融券" if market == "sh" else "深交所融资融券", "block": "margin"},
    ])
    return steps


class CollectProgress:
    """线程安全的采集进度。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.phase: str = "collect"
        self.tasks: list[FetchTask] = []
        self._index: dict[str, int] = {}

    def init_tasks(self, manifest: list[dict[str, str]]) -> None:
        with self._lock:
            self.tasks = [
                FetchTask(id=m["id"], group=m["group"], label=m["label"], block=m.get("block", ""))
                for m in manifest
            ]
            self._index = {t.id: i for i, t in enumerate(self.tasks)}

    def add_task(self, task_id: str, group: str, label: str, block: str = "") -> None:
        with self._lock:
            if task_id in self._index:
                return
            self.tasks.append(FetchTask(id=task_id, group=group, label=label, block=block))
            self._index[task_id] = len(self.tasks) - 1

    def set_phase(self, phase: str) -> None:
        with self._lock:
            self.phase = phase

    def task_start(self, task_id: str) -> None:
        with self._lock:
            t = self._task(task_id)
            if t:
                t.status = "running"
                t.message = "请求中…"
                t.rows = None
                t.elapsed = None

    def task_end(
        self,
        task_id: str,
        *,
        ok: bool,
        rows: int = 0,
        elapsed: float | None = None,
        message: str | None = None,
    ) -> None:
        with self._lock:
            t = self._task(task_id)
            if not t:
                return
            t.status = "ok" if ok else "fail"
            t.rows = rows
            t.elapsed = elapsed
            if message:
                t.message = message
            elif ok:
                t.message = f"{rows} 行" if rows else "无数据"
            else:
                t.message = "失败"

    def _task(self, task_id: str) -> FetchTask | None:
        idx = self._index.get(task_id)
        if idx is None:
            return None
        return self.tasks[idx]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            total = len(self.tasks)
            ok_n = sum(1 for t in self.tasks if t.status == "ok")
            fail_n = sum(1 for t in self.tasks if t.status == "fail")
            done_n = ok_n + fail_n
            running = next((t for t in self.tasks if t.status == "running"), None)
            percent = round(done_n / total * 100) if total else 0
            return {
                "phase": self.phase,
                "total": total,
                "completed": done_n,
                "ok": ok_n,
                "fail": fail_n,
                "percent": percent,
                "current": running.label if running else None,
                "tasks": [
                    {
                        "id": t.id,
                        "group": t.group,
                        "label": t.label,
                        "block": t.block,
                        "status": t.status,
                        "rows": t.rows,
                        "elapsed": round(t.elapsed, 2) if t.elapsed is not None else None,
                        "message": t.message,
                    }
                    for t in self.tasks
                ],
            }

    @staticmethod
    def cached_snapshot() -> dict[str, Any]:
        return {
            "phase": "done",
            "total": 1,
            "completed": 1,
            "ok": 1,
            "fail": 0,
            "percent": 100,
            "current": None,
            "tasks": [
                {
                    "id": "cache",
                    "group": "缓存",
                    "label": "命中本地 HTML 缓存",
                    "block": "",
                    "status": "ok",
                    "rows": None,
                    "elapsed": 0,
                    "message": "跳过采集",
                }
            ],
        }
