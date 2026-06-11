from __future__ import annotations

from datetime import datetime, timezone
import asyncio
import contextlib
import hashlib
import json
import os
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent, JoinEvent


"""
TikTok Live Collector -> Node.js Bridge

Flow mới:
    Python TikTok Collector
        -> POST comment/event sang Node.js
        -> Node.js lưu Supabase + broadcast SSE cho Next.js client

File này đã bỏ:
    - SSE stream trực tiếp từ Python sang client
    - analyze_comment_by_rule
    - analyze_comment_by_ai
    - COMMENT_UPDATED do AI

Python chỉ còn nhiệm vụ:
    - Start/stop collector theo username
    - Lấy comment TikTok
    - Lọc comment hệ thống/noise
    - Chuẩn hóa payload comment
    - Gửi comment và live event trực tiếp sang Node.js
    - Log lỗi rõ ràng nếu Node/mạng lỗi
"""


load_dotenv()


# =========================
# ENV CONFIG
# =========================

DEFAULT_TIKTOK_USERNAME = os.getenv("DEFAULT_TIKTOK_USERNAME", "@theunbeatablequeen26").strip()

HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("HTTP_PORT", "8765"))

# Node endpoint nhận comment từ Python.
# Khuyến nghị tạo route ở Node:
#   POST /api/internal/live-comments/ingest
NODE_COMMENT_INGEST_URL = os.getenv(
    "NODE_COMMENT_INGEST_URL",
    "http://localhost:3001/api/internal/live-comments/ingest",
).strip()

# Optional: Node endpoint nhận event trạng thái collector/live.
# Nếu để rỗng, Python chỉ gửi comment, không gửi LIVE_CONNECTED/LIVE_ERROR...
NODE_EVENT_INGEST_URL = os.getenv("NODE_EVENT_INGEST_URL", "").strip()

# Dùng để Node xác thực request nội bộ từ Python.
# Header gửi lên Node: x-internal-api-key: <NODE_INTERNAL_API_KEY>
NODE_INTERNAL_API_KEY = os.getenv("NODE_INTERNAL_API_KEY", "").strip()

# Optional: protect Python control API when Node gọi start/stop collector.
# Nếu để rỗng thì không check key.
COLLECTOR_CONTROL_API_KEY = os.getenv("COLLECTOR_CONTROL_API_KEY", "").strip()

NODE_REQUEST_TIMEOUT = int(os.getenv("NODE_REQUEST_TIMEOUT", "8"))

MAX_LATEST_COMMENTS = int(os.getenv("MAX_LATEST_COMMENTS", "200"))
ONLY_NUMBER_COMMENTS = os.getenv("ONLY_NUMBER_COMMENTS", "0").strip() == "1"

AUTO_START_USERNAME = os.getenv("AUTO_START_USERNAME", "").strip()

CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", "*").split(",")
    if origin.strip()
]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_LOG_ALL = os.getenv("TELEGRAM_LOG_ALL", "0").strip() == "1"
TELEGRAM_SEND_COMMENTS = os.getenv("TELEGRAM_SEND_COMMENTS", "0").strip() == "1"
TELEGRAM_MAX_LENGTH = 3500

HAS_NUMBER_RE = re.compile(r"\d")

SYSTEM_COMMENT_PATTERNS = [
    "đã chia sẻ",
    "da chia se",
    "shared",
    "đã thích",
    "da thich",
    "liked",
    "started following",
    "followed the host",
    "sent likes",
    "welcome to",
]

# Patterns cho join events (sẽ gửi như event riêng)
JOIN_COMMENT_PATTERNS = [
    "đã tham gia",
    "da tham gia",
    "joined",
]

TIKTOK_EMOJI_MAP = {
    "laughcry": "😂",
    "thanks": "🙏",
    "smile": "😊",
    "happy": "😄",
    "cry": "😭",
    "angry": "😡",
    "loveface": "😍",
    "surprised": "😲",
    "wow": "😮",
    "wronged": "🥺",
    "thinking": "🤔",
    "cool": "😎",
    "blink": "😉",
    "hehe": "😏",
    "flushed": "😳",
    "cute": "🥰",
    "greedy": "🤑",
    "joyful": "😆",
    "proud": "😌",
    "speechless": "😶",
    "awkward": "😅",
    "wicked": "😈",
    "rage": "😤",
    "sulk": "😒",
    "drool": "🤤",
    "complacent": "😌",
    "lovely": "🥰",
    "greteeth": "😁",
    "nap": "😴",
    "yummy": "😋",
    "shock": "😱",
    "slap": "😵",
    "tears": "😭",
    "stun": "😵",
}


# =========================
# APP
# =========================

app = FastAPI(title="TikTok Live Collector Node Bridge")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS if CORS_ORIGINS != ["*"] else ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# DATA TYPES
# =========================


@dataclass
class TikTokRoom:
    username: str
    client: Optional[TikTokLiveClient] = None
    task: Optional[asyncio.Task] = None
    is_running: bool = False
    is_stopping: bool = False
    room_id: str = ""
    collector_session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    shop_id: str = ""  # id shop do Node truyền vào khi start collector
    live_session_id: str = ""  # optional id do Node truyền vào khi start
    latest_comments: list[dict] = field(default_factory=list)
    comment_count: int = 0
    started_at: str = ""
    last_comment_at: str = ""
    last_error: str = ""


class StartCollectorBody(BaseModel):
    username: str
    shopId: Optional[str] = None
    liveSessionId: Optional[str] = None


class StopCollectorBody(BaseModel):
    username: Optional[str] = None
    stopAll: Optional[bool] = False


class LegacySubscribeBody(BaseModel):
    clientId: Optional[str] = None
    username: str
    shopId: Optional[str] = None
    liveSessionId: Optional[str] = None


class LegacyStopBody(BaseModel):
    clientId: Optional[str] = None
    username: Optional[str] = None
    stopAll: Optional[bool] = False


rooms: Dict[str, TikTokRoom] = {}
rooms_lock = asyncio.Lock()

metrics = {
    "started_at": time.time(),
    "total_comments": 0,
    "total_comments_enqueued": 0,
    "total_node_sent": 0,
    "total_node_send_error": 0,
    "comment_timestamps": [],
}


# =========================
# UTILITIES
# =========================


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_tiktok_username(username: str) -> str:
    value = str(username or "").strip()
    if not value:
        return DEFAULT_TIKTOK_USERNAME
    return value if value.startswith("@") else f"@{value}"


def normalize_at_username(value: str) -> str:
    username = str(value or "").strip()
    if not username:
        return ""
    return username if username.startswith("@") else f"@{username}"


def normalize_unique_id(value: str) -> str:
    return str(value or "").strip().lstrip("@")


def normalize_comment_text(text: str) -> str:
    return str(text or "").replace("\n", " ").strip()


def render_tiktok_emoji_tokens(text: str) -> str:
    value = str(text or "")

    def replace_token(match):
        token = match.group(1).strip().lower()
        return TIKTOK_EMOJI_MAP.get(token, match.group(0))

    return re.sub(r"\[([a-zA-Z0-9_]+)\]", replace_token, value)


def is_system_comment(text: str) -> bool:
    value = normalize_comment_text(text).lower()
    if not value:
        return True
    return any(pattern in value for pattern in SYSTEM_COMMENT_PATTERNS)


def is_join_event(text: str) -> bool:
    value = normalize_comment_text(text).lower()
    if not value:
        return False
    return any(pattern in value for pattern in JOIN_COMMENT_PATTERNS)


def is_number_comment(text: str) -> bool:
    return bool(HAS_NUMBER_RE.search(normalize_comment_text(text)))


def normalize_for_dedup(value: str) -> str:
    return str(value or "").lower().replace("@", "").replace("\n", " ").strip()


def make_hash_id(*parts: str) -> str:
    raw = ":".join(normalize_for_dedup(part) for part in parts if part is not None)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def get_lan_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram_message_sync(text: str):
    if not telegram_enabled():
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    body = urllib.parse.urlencode(
        {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text[:TELEGRAM_MAX_LENGTH],
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            response.read()
    except Exception as error:
        print("[TELEGRAM ERROR]", error, flush=True)


async def send_telegram_message(text: str):
    await asyncio.to_thread(send_telegram_message_sync, text)


def telegram_fire_and_forget(text: str):
    if not telegram_enabled():
        return

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(send_telegram_message(text))
    except RuntimeError:
        pass


def log(*args, telegram: bool = False):
    current = datetime.now().strftime("%H:%M:%S")
    message = " ".join(str(item) for item in args)
    line = f"[{current}] {message}"
    print(line, flush=True)

    if telegram or TELEGRAM_LOG_ALL:
        telegram_fire_and_forget(line)


# =========================
# SAFE OBJECT EXTRACTION
# =========================


def object_to_dict(obj: Any) -> dict:
    if not obj:
        return {}

    if isinstance(obj, dict):
        return obj

    for method_name in ["to_pydict", "to_dict", "dict", "model_dump"]:
        try:
            method = getattr(obj, method_name, None)
            if callable(method):
                data = method()
                if isinstance(data, dict):
                    return data
        except Exception:
            pass

    try:
        data = vars(obj)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    return {}


def deep_find_value(data: Any, keys: list[str]) -> Any:
    if data is None:
        return ""

    if isinstance(data, dict):
        for key in keys:
            if key in data and data[key]:
                return data[key]

        for value in data.values():
            found = deep_find_value(value, keys)
            if found:
                return found

    if isinstance(data, list):
        for item in data:
            found = deep_find_value(item, keys)
            if found:
                return found

    return ""


def extract_url_from_value(value: Any) -> str:
    if not value:
        return ""

    if isinstance(value, str):
        return value if value.startswith("http") else ""

    if isinstance(value, list):
        for item in value:
            found = extract_url_from_value(item)
            if found:
                return found
        return ""

    if isinstance(value, dict):
        for key in [
            "urlList",
            "url_list",
            "urls",
            "url",
            "uri",
            "avatar_url",
            "profilePictureUrl",
            "profile_picture_url",
        ]:
            if key in value:
                found = extract_url_from_value(value.get(key))
                if found:
                    return found

        for item in value.values():
            found = extract_url_from_value(item)
            if found:
                return found

    return ""


def get_direct_attr(obj: Any, names: list[str]) -> str:
    if not obj:
        return ""

    for name in names:
        try:
            value = getattr(obj, name, "")
            if value:
                return str(value)
        except Exception:
            pass

    return ""


def get_nested_user_candidates(event: CommentEvent) -> list[Any]:
    candidates: list[Any] = []

    # Tránh gọi event.user quá nhiều vì một số version TikTokLive có thể crash khi parse user.
    for attr_name in ["user_info", "userInfo", "author_info", "authorInfo"]:
        try:
            value = getattr(event, attr_name, None)
            if value:
                candidates.append(value)
        except Exception as error:
            log(f"SKIP EVENT ATTR {attr_name}:", error)

    event_dict = object_to_dict(event)
    if event_dict:
        for key in ["user_info", "userInfo", "author_info", "authorInfo", "data"]:
            value = event_dict.get(key)
            if value:
                candidates.append(value)

    return [item for item in candidates if item]


def first_deep_value(candidates: list[Any], keys: list[str]) -> str:
    for candidate in candidates:
        direct = get_direct_attr(candidate, keys)
        if direct:
            return direct

        data = object_to_dict(candidate)
        found = deep_find_value(data, keys)
        if found:
            return str(found)

    return ""


def merge_user_dicts(candidates: list[Any]) -> dict:
    merged: dict = {}

    for candidate in candidates:
        data = object_to_dict(candidate)
        if data:
            merged = {**merged, **data}

    return merged


def get_comment_avatar(user_dict: dict) -> str:
    avatar_value = (
        deep_find_value(
            user_dict,
            [
                "avatarThumb",
                "avatarMedium",
                "avatarLarger",
                "avatar_thumb",
                "avatar_medium",
                "avatar_larger",
                "profilePicture",
                "profilePictureUrl",
                "profile_picture",
                "profile_picture_url",
                "display_image",
                "image",
            ],
        )
        or ""
    )
    return extract_url_from_value(avatar_value)


def extract_comment_text(event: CommentEvent) -> str:
    """
    Lấy text comment từ nhiều nguồn khác nhau.

    Không chỉ phụ thuộc event.comment, vì một số payload TikTokLive có thể
    đổi field hoặc comment text nằm sâu trong raw event. Hàm này ưu tiên
    direct field trước, sau đó fallback đọc sâu trong raw dict.
    """
    direct_candidates = [
        "comment",
        "text",
        "content",
        "message",
        "display_text",
        "displayText",
    ]

    for attr_name in direct_candidates:
        try:
            value = getattr(event, attr_name, "")
            value = normalize_comment_text(value)
            if value:
                return value
        except Exception:
            pass

    event_dict = object_to_dict(event)

    # Ưu tiên các nhánh hay chứa nội dung comment trước để tránh nhầm sang
    # text/bio/nickname trong user payload.
    preferred_parent_keys = [
        "comment",
        "commentInfo",
        "comment_info",
        "message",
        "messageInfo",
        "message_info",
        "content",
        "data",
    ]

    for parent_key in preferred_parent_keys:
        parent_value = event_dict.get(parent_key) if isinstance(event_dict, dict) else None
        if not parent_value:
            continue

        if isinstance(parent_value, str):
            value = normalize_comment_text(parent_value)
            if value:
                return value

        found = deep_find_value(
            parent_value,
            [
                "comment",
                "text",
                "content",
                "message",
                "display_text",
                "displayText",
            ],
        )
        found = normalize_comment_text(found)
        if found:
            return found

    found = deep_find_value(
        event_dict,
        [
            "comment",
            "text",
            "content",
            "message",
            "display_text",
            "displayText",
        ],
    )
    found = normalize_comment_text(found)

    return found or ""


def get_event_user_tiktok_username_safe(event: CommentEvent) -> str:
    """
    Không gọi event.user.

    Một số version TikTokLive bị lỗi khi parse user payload có key camelCase
    như nickName, dẫn tới lỗi:
        User.__init__() got an unexpected keyword argument 'nickName'

    Vì vậy chỉ đọc từ raw candidates/user_info/author_info/data.
    """
    candidates = get_nested_user_candidates(event)
    username = (
        first_deep_value(
            candidates,
            [
                "tiktokUsername",
                "tiktok_username",
                "uniqueId",
                "unique_id",
                "display_id",
                "displayId",
                "username",
                "user_name",
                "userName",
            ],
        )
        or ""
    )
    return str(username or "").strip()


def get_comment_user(event: CommentEvent) -> tuple[str, str, str]:
    tiktok_username_from_event_user = get_event_user_tiktok_username_safe(event)

    candidates = get_nested_user_candidates(event)
    user_dict = merge_user_dicts(candidates)

    fallback_tiktok_username = (
        first_deep_value(
            candidates,
            [
                "tiktokUsername",
                "tiktok_username",
                "uniqueId",
                "unique_id",
                "display_id",
                "displayId",
                "username",
                "user_name",
                "userName",
            ],
        )
        or ""
    )

    display_name = (
        first_deep_value(
            candidates,
            ["nickname", "nick_name", "nickName", "display_name", "displayName"],
        )
        or tiktok_username_from_event_user
        or fallback_tiktok_username
        or "Unknown"
    )

    profile_username = tiktok_username_from_event_user or fallback_tiktok_username
    tiktok_username = normalize_at_username(profile_username)
    avatar = get_comment_avatar(user_dict)

    return str(display_name), str(tiktok_username), str(avatar)


def extract_tiktok_message_id(event: CommentEvent) -> str:
    candidates = []

    # Direct fields hay gặp trong payload TikTokLive.
    for attr_name in [
        "id",
        "msg_id",
        "msgId",
        "message_id",
        "messageId",
        "comment_id",
        "commentId",
    ]:
        try:
            value = getattr(event, attr_name, "")
            if value:
                candidates.append(value)
        except Exception:
            pass

    event_dict = object_to_dict(event)
    found = deep_find_value(
        event_dict,
        ["id", "msg_id", "msgId", "message_id", "messageId", "comment_id", "commentId"],
    )

    if found:
        candidates.append(found)

    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text

    return ""


def compact_raw_payload(data: Any, max_length: int = 8000) -> Any:
    try:
        text = json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        return {"raw": str(data)[:max_length]}

    if len(text) <= max_length:
        try:
            return json.loads(text)
        except Exception:
            return {"raw": text}

    return {"raw": text[:max_length], "truncated": True}


def build_comment_payload(
    *,
    room: TikTokRoom,
    event: CommentEvent,
    display_name: str,
    tiktok_username: str,
    avatar_url: str,
    text: str,
    raw_text: str,
) -> dict:
    created_at = now_iso()
    source_message_id = extract_tiktok_message_id(event)

    # Ưu tiên id thật từ TikTok. Nếu không có, tạo id có created_at để không làm mất
    # trường hợp cùng 1 user comment cùng nội dung nhiều lần, ví dụ "1", "1", "1".
    external_comment_id = source_message_id or make_hash_id(
        room.username,
        tiktok_username or display_name,
        raw_text or text,
        created_at,
    )

    event_id = make_hash_id(room.username, external_comment_id)
    event_dict = object_to_dict(event)
    tiktok_unique_id = normalize_unique_id(tiktok_username)
    has_number = is_number_comment(text)

    comment = {
        "id": external_comment_id,
        "externalCommentId": external_comment_id,
        "tiktokCommentId": external_comment_id,
        "tiktok_comment_id": external_comment_id,
        "dedupKey": event_id,
        "shopId": room.shop_id or None,
        "shop_id": room.shop_id or None,

        "username": display_name,
        "displayName": display_name,
        "display_name": display_name,

        "tiktokUsername": tiktok_username,
        "tiktok_username": tiktok_username,
        "tiktokUniqueId": tiktok_unique_id,
        "tiktok_unique_id": tiktok_unique_id,

        "avatar": avatar_url,
        "avatarUrl": avatar_url,
        "avatar_url": avatar_url,
        "profilePictureUrl": avatar_url,

        "text": text,
        "comment": text,
        "commentText": text,
        "comment_text": text,
        "rawText": raw_text,
        "raw_text": raw_text,

        "tiktokLiveUsername": room.username,
        "liveUsername": room.username,

        "createdAt": created_at,
        "created_at": created_at,

        # Không còn AI/rule ở Python. Giữ default để DB/client không bị undefined.
        "intent": "normal",
        "priorityLevel": "normal",
        "priority_level": "normal",
        "finalScore": 0,
        "final_score": 0,
        "hasNumber": has_number,
        "has_number": has_number,
        "canCreateOrder": False,
        "can_create_order": False,
        "isOrderCreated": False,
        "is_order_created": False,
    }

    return {
        "eventId": event_id,
        "eventType": "COMMENT",
        "source": "python-tiktok-collector",
        "shopId": room.shop_id or None,
        "liveUsername": room.username,
        "liveSessionId": room.live_session_id or None,
        "collectorSessionId": room.collector_session_id,
        "externalCommentId": external_comment_id,
        "dedupKey": event_id,

        # Flat fields cho Node map vào DB nhanh.
        "tiktokUsername": tiktok_username,
        "tiktokUniqueId": tiktok_unique_id,
        "tiktok_unique_id": tiktok_unique_id,
        "tiktokCommentId": external_comment_id,
        "tiktok_comment_id": external_comment_id,
        "displayName": display_name,
        "avatarUrl": avatar_url,
        "commentText": text,
        "rawText": raw_text,
        "intent": "normal",
        "priorityLevel": "normal",
        "finalScore": 0,
        "hasNumber": has_number,
        "has_number": has_number,
        "canCreateOrder": False,
        "can_create_order": False,
        "isOrderCreated": False,
        "createdAt": created_at,

        # Nested comment giữ tương thích với client/schema cũ.
        "comment": comment,

        # Payload gốc để Node lưu raw_payload nếu cần debug.
        "rawPayload": compact_raw_payload(event_dict),
    }


# =========================
# LIVE EVENT SENDER
# =========================


async def send_comment_to_node(payload: dict):
    try:
        await post_to_node("COMMENT", payload)
        metrics["total_comments_enqueued"] += 1
        metrics["total_node_sent"] += 1
        log("COMMENT SENT TO NODE", "| eventId:", payload.get("eventId"))
    except Exception as exc:
        metrics["total_node_send_error"] += 1
        log("COMMENT SEND ERROR", "| eventId:", payload.get("eventId"), "| error:", exc)


async def get_outbox_stats():
    return {
        "enabled": False,
        "dbPath": None,
    }

def post_json_sync(url: str, payload: dict) -> dict:
    if not url:
        raise RuntimeError("Node ingest URL is empty")

    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "tiktok-python-collector/1.0",
    }

    event_id = str(payload.get("eventId") or "").strip()
    if event_id:
        # Node có thể dùng header này để idempotency/dedupe.
        headers["x-event-id"] = event_id

    if NODE_INTERNAL_API_KEY:
        headers["x-internal-api-key"] = NODE_INTERNAL_API_KEY

    request = urllib.request.Request(url, data=body, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(request, timeout=NODE_REQUEST_TIMEOUT) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            status_code = response.getcode()
    except urllib.error.HTTPError as error:
        response_body = error.read().decode("utf-8", errors="replace") if error.fp else ""
        raise RuntimeError(f"Node returned HTTP {error.code}: {response_body[:800]}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"Cannot connect to Node: {error}") from error

    if status_code < 200 or status_code >= 300:
        raise RuntimeError(f"Node returned HTTP {status_code}: {response_body[:800]}")

    if not response_body:
        return {"ok": True}

    try:
        data = json.loads(response_body)
    except Exception:
        return {"ok": True, "raw": response_body}

    if isinstance(data, dict) and data.get("ok") is False:
        raise RuntimeError(str(data.get("message") or data))

    return data if isinstance(data, dict) else {"ok": True, "data": data}


async def post_to_node(event_type: str, payload: dict) -> dict:
    url = NODE_COMMENT_INGEST_URL if event_type == "COMMENT" else NODE_EVENT_INGEST_URL
    return await asyncio.to_thread(post_json_sync, url, payload)


async def send_realtime_live_event(event_type: str, payload: dict):
    if not NODE_EVENT_INGEST_URL:
        log("LIVE EVENT IGNORED | NODE_EVENT_INGEST_URL is not configured:", event_type)
        return

    try:
        await post_to_node(event_type, payload)
    except Exception as exc:
        metrics["total_node_send_error"] += 1
        log("LIVE EVENT SEND ERROR", "| eventType:", event_type, "| error:", exc)



# =========================
# ROOM / COLLECTOR
# =========================


async def enqueue_live_event(room: TikTokRoom, event_type: str, payload: dict):
    event_id = payload.get("eventId") or make_hash_id(room.username, event_type, now_iso())
    envelope = {
        "eventId": event_id,
        "eventType": event_type,
        "source": "python-tiktok-collector",
        "shopId": room.shop_id or None,
        "liveUsername": room.username,
        "liveSessionId": room.live_session_id or None,
        "collectorSessionId": room.collector_session_id,
        "createdAt": now_iso(),
        **payload,
    }
    await send_realtime_live_event(event_type, envelope)


def upsert_latest_comment(room: TikTokRoom, payload: dict):
    comment = payload.get("comment") or payload
    comment_id = comment.get("id") or payload.get("externalCommentId")

    if not comment_id:
        return

    for index, item in enumerate(room.latest_comments):
        item_id = item.get("id") or item.get("externalCommentId")
        if item_id == comment_id:
            room.latest_comments[index] = {**item, **comment, "updatedAt": now_iso()}
            return

    room.latest_comments.insert(0, comment)
    del room.latest_comments[MAX_LATEST_COMMENTS:]


async def stop_room(room: TikTokRoom):
    room.is_stopping = True
    log("STOP ROOM:", room.username)

    if room.task and not room.task.done():
        room.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await room.task

    if room.client:
        for method_name in ["disconnect", "stop", "close"]:
            try:
                method = getattr(room.client, method_name, None)
                if callable(method):
                    result = method()
                    if asyncio.iscoroutine(result):
                        await result
                    break
            except Exception as error:
                log(f"Stop room client via {method_name} error:", room.username, error)

    room.client = None
    room.task = None
    room.is_running = False
    room.is_stopping = False


async def get_or_create_room(
    username: str,
    shop_id: Optional[str] = None,
    live_session_id: Optional[str] = None,
) -> TikTokRoom:
    normalized_username = normalize_tiktok_username(username)

    async with rooms_lock:
        room = rooms.get(normalized_username)

        if not room:
            room = TikTokRoom(username=normalized_username)
            rooms[normalized_username] = room
            log("ROOM CREATED:", normalized_username, telegram=True)

        if shop_id:
            room.shop_id = str(shop_id)

        if live_session_id:
            room.live_session_id = str(live_session_id)

        return room


async def start_room(
    username: str,
    shop_id: Optional[str] = None,
    live_session_id: Optional[str] = None,
) -> TikTokRoom:
    room = await get_or_create_room(username, shop_id=shop_id, live_session_id=live_session_id)

    async with rooms_lock:
        if room.task and not room.task.done():
            log("ROOM ALREADY RUNNING:", room.username)
            return room

        room.collector_session_id = str(uuid.uuid4())
        room.started_at = now_iso()
        room.last_error = ""
        room.comment_count = 0
        room.latest_comments = []
        room.is_stopping = False
        room.task = asyncio.create_task(run_room_collector(room))

    log("ROOM STARTED:", room.username, telegram=True)
    return room


async def stop_room_by_username(username: str) -> bool:
    normalized_username = normalize_tiktok_username(username)

    async with rooms_lock:
        room = rooms.pop(normalized_username, None)

    if not room:
        return False

    await stop_room(room)
    log("ROOM REMOVED:", normalized_username, telegram=True)
    return True


async def stop_all_rooms() -> int:
    async with rooms_lock:
        active_rooms = list(rooms.values())
        rooms.clear()

    for room in active_rooms:
        await stop_room(room)

    return len(active_rooms)


def create_tiktok_client_for_room(room: TikTokRoom) -> TikTokLiveClient:
    username = room.username
    client = TikTokLiveClient(unique_id=username)

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent):
        room.room_id = str(getattr(client, "room_id", "") or "")

        log("======================================")
        log("CONNECTED TO TIKTOK LIVE ROOM", telegram=True)
        log("TikTok username:", username)
        log("Shop ID:", room.shop_id or "(empty)")
        log("Room ID:", room.room_id)
        log("======================================")

        await enqueue_live_event(
            room,
            "LIVE_CONNECTED",
            {
                "username": username,
                "roomId": room.room_id,
                "createdAt": now_iso(),
            },
        )

    @client.on(JoinEvent)
    async def on_join(event: JoinEvent):
        try:
            nickname = getattr(event.user, "nickname", None) or getattr(event.user, "display_id", None) or ""
            unique_id = getattr(event.user, "unique_id", None) or getattr(event.user, "display_id", None) or ""
            avatar_url = ""
            try:
                avatar_url = str(event.user.avatar_thumb.m_urls[0])
            except Exception:
                pass

            created_at = now_iso()
            event_id = make_hash_id(room.username, "USER_JOINED", unique_id or nickname, created_at)

            join_payload = {
                "eventId": event_id,
                "eventType": "USER_JOINED",
                "source": "python-tiktok-collector",
                "shopId": room.shop_id or None,
                "liveUsername": room.username,
                "liveSessionId": room.live_session_id or None,
                "collectorSessionId": room.collector_session_id,
                "joinUsername": unique_id,
                "joinDisplayName": nickname,
                "joinAvatarUrl": avatar_url,
                "nickname": nickname,
                "createdAt": created_at,
            }

            await send_realtime_live_event("USER_JOINED", join_payload)

            log("======================================")
            log("USER JOINED (native JoinEvent)")
            log("Live room:", username)
            log("Nickname:", nickname)
            log("TikTok unique_id:", unique_id or "(empty)")
            log("eventId:", event_id)
            log("======================================")
        except Exception as exc:
            log("JoinEvent handler error:", exc)

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):
        raw_text = extract_comment_text(event)
        text = normalize_comment_text(render_tiktok_emoji_tokens(raw_text))

        if not raw_text:
            event_dict = object_to_dict(event)
            log("COMMENT IGNORED | Empty text")
            log("RAW EVENT KEYS:", list(event_dict.keys())[:50] if isinstance(event_dict, dict) else [])
            log(
                "RAW EVENT COMPACT:",
                json.dumps(compact_raw_payload(event_dict, 1500), ensure_ascii=False, default=str),
            )
            return

        display_name, tiktok_username, avatar_url = get_comment_user(event)

        # Check nếu là join event
        if is_join_event(raw_text) or is_join_event(text):
            created_at = now_iso()
            event_id = make_hash_id(room.username, "USER_JOINED", tiktok_username or display_name, created_at)

            join_payload = {
                "eventId": event_id,
                "eventType": "USER_JOINED",
                "source": "python-tiktok-collector",
                "shopId": room.shop_id or None,
                "liveUsername": room.username,
                "liveSessionId": room.live_session_id or None,
                "collectorSessionId": room.collector_session_id,
                "joinUsername": tiktok_username,
                "joinDisplayName": display_name,
                "joinAvatarUrl": avatar_url,
                "nickname": display_name,
                "joinText": text,
                "rawText": raw_text,
                "createdAt": created_at,
            }

            await send_realtime_live_event("USER_JOINED", join_payload)

            log("======================================")
            log("USER JOINED EVENT SENT TO NODE")
            log("Live room:", username)
            log("Display name:", display_name)
            log("TikTok username:", tiktok_username or "(empty)")
            log("Join text:", text)
            log("eventId:", event_id)
            log("======================================")
            return

        if is_system_comment(raw_text) or is_system_comment(text):
            log("COMMENT IGNORED | System/noise:", raw_text)
            return

        if ONLY_NUMBER_COMMENTS and not is_number_comment(text):
            log("COMMENT IGNORED | No number:", text)
            return

        payload = build_comment_payload(
            room=room,
            event=event,
            display_name=display_name,
            tiktok_username=tiktok_username,
            avatar_url=avatar_url,
            text=text,
            raw_text=raw_text,
        )

        upsert_latest_comment(room, payload)

        room.comment_count += 1
        room.last_comment_at = payload["createdAt"]
        metrics["total_comments"] += 1
        metrics["comment_timestamps"].append(time.time())
        metrics["comment_timestamps"] = metrics["comment_timestamps"][-10000:]

        await send_comment_to_node(payload)

        log("======================================")
        log("NEW COMMENT SENT TO NODE")
        log("Live room:", username)
        log("Shop ID:", room.shop_id or "(empty)")
        log("Display name:", display_name)
        log("TikTok username:", tiktok_username or "(empty)")
        log("Text:", text)
        log("Raw text:", raw_text)
        log("eventId:", payload.get("eventId"))
        log("======================================")

        if TELEGRAM_SEND_COMMENTS:
            telegram_fire_and_forget(
                "\n".join(
                    [
                        "💬 New TikTok LIVE comment queued to Node",
                        f"LIVE: {username}",
                        f"User: {display_name}",
                        f"TikTok: {tiktok_username or '(empty)'}",
                        f"Text: {text}",
                        f"Time: {now_iso()}",
                    ]
                )
            )

    @client.on(DisconnectEvent)
    async def on_disconnect(event: DisconnectEvent):
        log("TIKTOK LIVE DISCONNECTED:", username, telegram=True)
        await enqueue_live_event(
            room,
            "LIVE_DISCONNECTED",
            {
                "username": username,
                "createdAt": now_iso(),
            },
        )

    return client


async def run_room_collector(room: TikTokRoom):
    room.is_running = True
    room.last_error = ""

    try:
        while room.username in rooms and not room.is_stopping:
            try:
                log("STARTING ROOM COLLECTOR:", room.username)

                client = create_tiktok_client_for_room(room)
                room.client = client

                try:
                    is_live = await client.is_live()
                    log("TikTok is_live:", room.username, is_live)

                    if is_live is False:
                        message = "Account đang không LIVE"
                        room.last_error = message

                        await enqueue_live_event(
                            room,
                            "LIVE_ERROR",
                            {
                                "message": message,
                                "username": room.username,
                                "shouldStop": True,
                                "retry": False,
                                "createdAt": now_iso(),
                            },
                        )

                        log("ACCOUNT OFFLINE, STOP COLLECTOR:", room.username)
                        break

                except Exception as error:
                    error_text = repr(error)
                    room.last_error = error_text
                    log("CHECK LIVE ERROR:", room.username, error_text, telegram=True)

                    is_retryable_error = (
                        "SignAPIError" in error_text
                        or "SIGN_NOT_200" in error_text
                        or "status code 500" in error_text
                        or "status code 503" in error_text
                        or "503 error occurred" in error_text
                        or "500 error occurred" in error_text
                        or "fetching the webcast URL" in error_text
                    )

                    await enqueue_live_event(
                        room,
                        "LIVE_ERROR",
                        {
                            "message": "TikTok Sign API đang lỗi, server sẽ tự thử lại sau 10 giây."
                            if is_retryable_error
                            else str(error),
                            "username": room.username,
                            "shouldStop": not is_retryable_error,
                            "retry": is_retryable_error,
                            "createdAt": now_iso(),
                        },
                    )

                    if is_retryable_error:
                        log("SIGN API ERROR | RETRY AFTER 10s:", room.username)
                        await asyncio.sleep(10)
                        continue

                    log("LIVE_ERROR SENT, STOP COLLECTOR:", room.username)
                    break

                result = await client.start()

                if isinstance(result, asyncio.Task):
                    log("TikTokLive returned task, waiting task:", room.username)
                    await result

                if room.is_stopping:
                    break

                await enqueue_live_event(
                    room,
                    "LIVE_ERROR",
                    {
                        "message": "TikTokLive room stopped",
                        "username": room.username,
                        "shouldStop": False,
                        "retry": True,
                        "createdAt": now_iso(),
                    },
                )

                log("ROOM STOPPED | RETRY AFTER 10s:", room.username)
                await asyncio.sleep(10)
                continue

            except asyncio.CancelledError:
                log("ROOM COLLECTOR CANCELLED:", room.username)
                raise

            except Exception as error:
                error_text = repr(error)
                room.last_error = error_text
                log("ROOM COLLECTOR ERROR:", room.username, error_text, telegram=True)

                is_retryable_error = (
                    "SignAPIError" in error_text
                    or "SIGN_NOT_200" in error_text
                    or "status code 500" in error_text
                    or "status code 503" in error_text
                    or "503 error occurred" in error_text
                    or "500 error occurred" in error_text
                    or "fetching the webcast URL" in error_text
                )

                await enqueue_live_event(
                    room,
                    "LIVE_ERROR",
                    {
                        "message": "TikTok Sign API đang lỗi, server sẽ tự thử lại sau 10 giây."
                        if is_retryable_error
                        else str(error),
                        "username": room.username,
                        "shouldStop": not is_retryable_error,
                        "retry": is_retryable_error,
                        "createdAt": now_iso(),
                    },
                )

                if is_retryable_error:
                    log("RETRYABLE ROOM ERROR | RETRY AFTER 10s:", room.username)
                    await asyncio.sleep(10)
                    continue

                break

    finally:
        room.is_running = False
        room.client = None
        room.task = None

        await enqueue_live_event(
            room,
            "COLLECTOR_STOPPED",
            {
                "username": room.username,
                "commentCount": room.comment_count,
                "lastCommentAt": room.last_comment_at or None,
                "createdAt": now_iso(),
            },
        )

        log("ROOM COLLECTOR STOPPED:", room.username)


# =========================
# API ROUTES
# =========================


async def require_control_api_key(
    x_internal_api_key: Optional[str] = Header(default=None),
    x_collector_api_key: Optional[str] = Header(default=None),
):
    if not COLLECTOR_CONTROL_API_KEY:
        return True

    incoming_key = (x_collector_api_key or x_internal_api_key or "").strip()

    if incoming_key != COLLECTOR_CONTROL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid collector control API key")

    return True


@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "TikTok Live Collector Node Bridge",
        "flow": "Python TikTok Collector -> Node.js -> SSE -> Next.js",
        "start": "POST /collectors/start { username, shopId }",
        "stop": "POST /collectors/stop",
        "collectors": "GET /collectors",
        "metrics": "GET /metrics",
        "nodeCommentIngestUrl": NODE_COMMENT_INGEST_URL,
        "nodeEventIngestUrl": NODE_EVENT_INGEST_URL or None,
    }


@app.get("/health")
async def health():
    return {"ok": True, "createdAt": now_iso()}


@app.post("/collectors/start")
async def start_collector(body: StartCollectorBody, _: bool = Depends(require_control_api_key)):
    if not body.username:
        return {"ok": False, "message": "Missing username"}

    room = await start_room(body.username, shop_id=body.shopId, live_session_id=body.liveSessionId)

    return {
        "ok": True,
        "username": room.username,
        "shopId": room.shop_id or None,
        "liveSessionId": room.live_session_id or None,
        "collectorSessionId": room.collector_session_id,
        "isRunning": bool(room.task and not room.task.done()),
    }


@app.post("/collectors/stop")
async def stop_collector(body: StopCollectorBody, _: bool = Depends(require_control_api_key)):
    if body.stopAll:
        count = await stop_all_rooms()
        return {"ok": True, "stopped": count}

    if not body.username:
        return {"ok": False, "message": "Missing username or stopAll=true"}

    stopped = await stop_room_by_username(body.username)
    return {"ok": True, "username": normalize_tiktok_username(body.username), "stopped": stopped}


# Compatibility nếu Node/client cũ vẫn gọi /subscribe và /stop.
@app.post("/subscribe")
async def legacy_subscribe(body: LegacySubscribeBody, _: bool = Depends(require_control_api_key)):
    room = await start_room(body.username, shop_id=body.shopId, live_session_id=body.liveSessionId)
    return {
        "ok": True,
        "clientId": body.clientId,
        "username": room.username,
        "shopId": room.shop_id or None,
        "liveSessionId": room.live_session_id or None,
        "collectorSessionId": room.collector_session_id,
    }


@app.post("/stop")
async def legacy_stop(body: LegacyStopBody, _: bool = Depends(require_control_api_key)):
    if body.stopAll:
        count = await stop_all_rooms()
        return {"ok": True, "clientId": body.clientId, "stopped": count}

    if not body.username:
        count = await stop_all_rooms()
        return {"ok": True, "clientId": body.clientId, "stopped": count}

    stopped = await stop_room_by_username(body.username)
    return {"ok": True, "clientId": body.clientId, "username": normalize_tiktok_username(body.username), "stopped": stopped}


@app.get("/collectors")
async def collectors(_: bool = Depends(require_control_api_key)):
    return {
        "ok": True,
        "collectors": [
            {
                "username": room.username,
                "shopId": room.shop_id or None,
                "roomId": room.room_id or None,
                "liveSessionId": room.live_session_id or None,
                "collectorSessionId": room.collector_session_id,
                "isRunning": room.is_running,
                "commentCount": room.comment_count,
                "startedAt": room.started_at or None,
                "lastCommentAt": room.last_comment_at or None,
                "lastError": room.last_error or None,
                "latestComments": room.latest_comments[:20],
            }
            for room in rooms.values()
        ],
        "createdAt": now_iso(),
    }


@app.get("/metrics")
async def metrics_route(_: bool = Depends(require_control_api_key)):
    now = time.time()
    recent_comments = [ts for ts in metrics["comment_timestamps"] if now - ts <= 60]
    outbox_stats = await get_outbox_stats()

    return {
        "ok": True,
        "uptimeSeconds": int(now - metrics["started_at"]),
        "activeRooms": len(rooms),
        "totalComments": metrics["total_comments"],
        "totalCommentsEnqueued": metrics["total_comments_enqueued"],
        "totalNodeSent": metrics["total_node_sent"],
        "totalNodeSendError": metrics["total_node_send_error"],
        "commentsPerMinute": len(recent_comments),
        "commentsPerSecond": round(len(recent_comments) / 60, 2),
        "outbox": outbox_stats,
        "createdAt": now_iso(),
    }


# =========================
# LIFECYCLE
# =========================


@app.on_event("startup")
async def on_startup():
    if AUTO_START_USERNAME:
        await start_room(AUTO_START_USERNAME)


@app.on_event("shutdown")
async def on_shutdown():
    await stop_all_rooms()


if __name__ == "__main__":
    import uvicorn

    lan_ip = get_lan_ip()

    log("======================================")
    log("STARTING TIKTOK LIVE COLLECTOR NODE BRIDGE", telegram=True)
    log("Running file:", __file__)
    log("Bind host:", HTTP_HOST)
    log("Port:", HTTP_PORT)
    log("Local URL:", f"http://localhost:{HTTP_PORT}")
    log("LAN URL:", f"http://{lan_ip}:{HTTP_PORT}")
    log("Node comment ingest URL:", NODE_COMMENT_INGEST_URL)
    log("Collector control API key:", "enabled" if COLLECTOR_CONTROL_API_KEY else "disabled")
    log("Node event ingest URL:", NODE_EVENT_INGEST_URL or "(disabled)")
    log("Default TikTok username:", DEFAULT_TIKTOK_USERNAME)
    log("Message: POST /collectors/start, POST /collectors/stop, GET /collectors")
    log("======================================")

    uvicorn.run(app, host=HTTP_HOST, port=HTTP_PORT, reload=False)
