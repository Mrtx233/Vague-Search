import os
import time
import socket
from datetime import datetime
from typing import Dict, Optional

import requests


def get_device_name() -> str:
    """获取设备名称"""
    try:
        return socket.gethostname()
    except Exception:
        return "未知设备"


WPS_NOTIFY_ENABLED = True
WPS_ROBOT_WEBHOOK = os.getenv(
    "WPS_ROBOT_WEBHOOK",
    "https://365.kdocs.cn/woa/api/v1/webhook/send?key=add87b4b34f7ecaebf14c4e133ab9d5c",
)
WPS_THROTTLE_SECONDS = 60
_LAST_WPS_TS: Dict[str, float] = {}


def send_wps_robot(content: str, throttle_key: str = "default", timeout: int = 10) -> bool:
    """发送文本消息到 WPS 机器人（带节流）。"""
    if not WPS_NOTIFY_ENABLED:
        return False
    if not WPS_ROBOT_WEBHOOK:
        return False
    if not content:
        return False

    now = time.time()
    last_ts = _LAST_WPS_TS.get(throttle_key, 0.0)
    if now - last_ts < WPS_THROTTLE_SECONDS:
        return False
    _LAST_WPS_TS[throttle_key] = now

    try:
        payload = {"msgtype": "text", "text": {"content": content}}
        resp = requests.post(WPS_ROBOT_WEBHOOK, json=payload, timeout=timeout)
        return resp.status_code == 200
    except Exception:
        return False


def notify_event(
    event_title: str,
    start_dt: datetime,
    config: Dict,
    extra: str = "",
    throttle_key: str = "event",
    error_detail: str = "",
    jsonl_filename: Optional[str] = None,
    script_name: Optional[str] = None,
) -> bool:
    """统一事件通知出口。"""
    device_name = get_device_name()
    msg = (
        "【Crawler通知】\n"
        f"设备: {device_name}\n\n"
        f"事件: {event_title}\n"
        f"开始时间: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"运行脚本: {script_name or ''}\n"
        f"关键词文件: {config.get('keyword_path')}\n"
        f"进度: {extra}"
    )
    return send_wps_robot(msg, throttle_key=throttle_key)
