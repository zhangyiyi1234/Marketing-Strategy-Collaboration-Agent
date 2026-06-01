"""
命令行测试 /api/crew 与 /api/crew/{job_id}，无需 Apifox。
依赖：仅 Python 标准库。

用法（先启动 Redis、Celery worker、本服务）:
  cd crewaiFlowsBackend
  python apitest.py
  python apitest.py --base-url http://127.0.0.1:8012 --timeout 600
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any

DEFAULT_BASE = "http://127.0.0.1:8012"
SAMPLE_BODY = {
    "customer_domain": "https://www.emqx.com/zh",
    "project_description": (
        "EMQX是一种开源的分布式消息中间件，专注于处理物联网 (IoT) 场景下的大规模消息通信。"
        "它基于MQTT协议，能够实现高并发、低延迟的实时消息推送，支持设备之间、设备与服务器之间的双向通信。"
        "客户领域:分布式消息中间件解决方案,项目概述:创建一个全面的营销活动，"
        "以提高企业客户对 EMQX 服务的认识和采用。"
    ),
}


def _request_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    timeout: int = 120,
) -> tuple[int, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        data = raw
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode() or 0
            text = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        status = e.code
        text = e.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        raise SystemExit(f"请求失败: {e}") from e
    try:
        parsed = json.loads(text) if text.strip() else None
    except json.JSONDecodeError:
        parsed = text
    return status, parsed


def main() -> None:
    p = argparse.ArgumentParser(description="测试 Crew Flow API（POST 启动作业 + GET 轮询状态）")
    p.add_argument("--base-url", default=DEFAULT_BASE, help=f"API 根地址，默认 {DEFAULT_BASE}")
    p.add_argument("--interval", type=float, default=3.0, help="轮询间隔（秒）")
    p.add_argument("--timeout", type=int, default=900, help="最长等待作业结束（秒）")
    p.add_argument("--no-poll", action="store_true", help="只 POST 创建作业，不轮询 GET")
    args = p.parse_args()

    base = args.base_url.rstrip("/")
    post_url = f"{base}/api/crew"

    print(f"POST {post_url}")
    status, payload = _request_json("POST", post_url, SAMPLE_BODY, timeout=60)
    print(f"HTTP {status}")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    if args.no_poll or status >= 400:
        sys.exit(0 if status < 400 else 1)

    if not isinstance(payload, dict) or "job_id" not in payload:
        raise SystemExit("响应中缺少 job_id，无法轮询")

    job_id = payload["job_id"]
    get_url = f"{base}/api/crew/{job_id}"
    deadline = time.monotonic() + args.timeout

    print(f"\n轮询 GET {get_url}（间隔 {args.interval}s，超时 {args.timeout}s）\n")

    while time.monotonic() < deadline:
        st, job = _request_json("GET", get_url, None, timeout=60)
        if st == 404:
            print("404 Job not found（可能 MySQL 未就绪或 job 未写入）")
        elif isinstance(job, dict):
            s = job.get("status")
            print(f"[{time.strftime('%H:%M:%S')}] HTTP {st} status={s}")
            if s in ("COMPLETE", "ERROR"):
                print(json.dumps(job, ensure_ascii=False, indent=2))
                sys.exit(0 if s == "COMPLETE" else 2)
        else:
            print(f"HTTP {st}", job)
        time.sleep(args.interval)

    raise SystemExit("超时：作业仍未 COMPLETE/ERROR")


if __name__ == "__main__":
    main()
