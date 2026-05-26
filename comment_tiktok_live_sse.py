from datetime import datetime, timezone
import asyncio
import contextlib
import hashlib
import json
import os
import re
import socket
import time
import urllib.parse
import urllib.request
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent


DEFAULT_TIKTOK_USERNAME = "@theunbeatablequeen26"

HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("HTTP_PORT", "8765"))

MAX_COMMENTS = int(os.getenv("MAX_COMMENTS", "500"))
CLIENT_QUEUE_MAX_SIZE = int(os.getenv("CLIENT_QUEUE_MAX_SIZE", "1000"))

AUTO_STOP_ROOM_WHEN_EMPTY = os.getenv("AUTO_STOP_ROOM_WHEN_EMPTY", "1") == "1"
AUTO_STOP_EMPTY_ROOM_DELAY = int(os.getenv("AUTO_STOP_EMPTY_ROOM_DELAY", "30"))

CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", "*").split(",")
    if origin.strip()
]

HAS_NUMBER_RE = re.compile(r"\d")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_LOG_ALL = os.getenv("TELEGRAM_LOG_ALL", "0").strip() == "1"
TELEGRAM_SEND_COMMENTS = os.getenv("TELEGRAM_SEND_COMMENTS", "0").strip() == "1"
TELEGRAM_MAX_LENGTH = 3500


app = FastAPI(title="TikTok Live SSE Comment Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS if CORS_ORIGINS != ["*"] else ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@dataclass
class AppLiveSession:
    id: str
    username: str
    started_at: str
    comment_count: int = 0


@dataclass
class SseClient:
    id: str
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=CLIENT_QUEUE_MAX_SIZE))
    username: str = ""
    connected_at: str = ""


@dataclass
class TikTokRoom:
    username: str
    client: Optional[TikTokLiveClient] = None
    task: Optional[asyncio.Task] = None
    subscribers: Set[str] = field(default_factory=set)
    latest_comments: list[dict] = field(default_factory=list)
    empty_stop_task: Optional[asyncio.Task] = None
    is_running: bool = False


class SubscribeBody(BaseModel):
    clientId: str
    username: str


class StopBody(BaseModel):
    clientId: str


clients: Dict[str, SseClient] = {}
client_subscriptions: Dict[str, str] = {}
rooms: Dict[str, TikTokRoom] = {}
active_sessions: Dict[str, AppLiveSession] = {}

rooms_lock = asyncio.Lock()
clients_lock = asyncio.Lock()

metrics = {
    "started_at": time.time(),
    "total_comments": 0,
    "total_sent": 0,
    "total_send_error": 0,
    "total_clients_connected": 0,
    "total_clients_disconnected": 0,
    "comment_timestamps": deque(maxlen=10000),
    "send_latency_ms": deque(maxlen=10000),
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def calc_duration_seconds(start_time: str, end_time: str) -> int:
    start = datetime.fromisoformat(start_time)
    end = datetime.fromisoformat(end_time)
    return max(0, int((end - start).total_seconds()))


def normalize_tiktok_username(username: str) -> str:
    value = str(username or "").strip()

    if not value:
        return DEFAULT_TIKTOK_USERNAME

    return value if value.startswith("@") else f"@{value}"


def get_lan_ip():
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
    now = datetime.now().strftime("%H:%M:%S")
    message = " ".join(str(item) for item in args)
    line = f"[{now}] {message}"

    print(line, flush=True)

    if telegram or TELEGRAM_LOG_ALL:
        telegram_fire_and_forget(line)


def make_comment_id(username: str, text: str) -> str:
    raw = f"{username}:{text}:{time.time_ns()}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def normalize_comment_text(text: str) -> str:
    return str(text or "").replace("\n", " ").strip()


def is_number_comment(text: str) -> bool:
    value = normalize_comment_text(text)

    if not value:
        return False

    return bool(HAS_NUMBER_RE.search(value))


def object_to_dict(obj: Any):
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


def deep_find_value(data: Any, keys: list[str]):
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
        if value.startswith("http"):
            return value
        return ""

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


def get_comment_user(event: CommentEvent):
    user_info = getattr(event, "user_info", None)
    user_dict = object_to_dict(user_info)

    username = (
        deep_find_value(user_dict, ["nickname", "nickName", "nick_name"])
        or deep_find_value(user_dict, ["uniqueId", "unique_id", "display_id"])
        or "Unknown"
    )

    unique_id = (
        deep_find_value(user_dict, ["uniqueId", "unique_id", "display_id"])
        or ""
    )

    avatar = get_comment_avatar(user_dict)

    return str(username), str(unique_id), str(avatar)


def live_time_payload(
    session: AppLiveSession,
    *,
    ended_at: Optional[str] = None,
    reason: str = "",
) -> dict:
    duration_seconds = 0

    if ended_at:
        duration_seconds = calc_duration_seconds(session.started_at, ended_at)

    return {
        "sessionId": session.id,
        "session_id": session.id,
        "username": session.username,
        "startedAt": session.started_at,
        "started_at": session.started_at,
        "endedAt": ended_at,
        "ended_at": ended_at,
        "durationSeconds": duration_seconds,
        "duration_seconds": duration_seconds,
        "commentCount": session.comment_count,
        "comment_count": session.comment_count,
        "reason": reason,
        "createdAt": now_iso(),
        "created_at": now_iso(),
    }


def build_sse(event: str, payload: dict):
    data = json.dumps(payload, ensure_ascii=False)
    return f"event: {event}\ndata: {data}\n\n"


def build_sse_ping():
    return ": ping\n\n"


async def put_client_event(client: SseClient, event: str, payload: dict):
    item = {
        "event": event,
        "payload": payload,
    }

    try:
        client.queue.put_nowait(item)
    except asyncio.QueueFull:
        with contextlib.suppress(asyncio.QueueEmpty):
            client.queue.get_nowait()

        try:
            client.queue.put_nowait(item)
        except asyncio.QueueFull:
            return False

    return True


async def send_to_client(client_id: str, event: str, payload: dict):
    started = time.perf_counter()
    client = clients.get(client_id)

    if not client:
        metrics["total_send_error"] += 1
        return False

    ok = await put_client_event(client, event, payload)

    if ok:
        elapsed_ms = (time.perf_counter() - started) * 1000
        metrics["total_sent"] += 1
        metrics["send_latency_ms"].append(elapsed_ms)
        return True

    metrics["total_send_error"] += 1
    return False


async def start_live_time_session(
    client_id: str,
    username: str,
    *,
    reason: str = "first_comment",
):
    normalized_username = normalize_tiktok_username(username)
    existing_session = active_sessions.get(client_id)

    if existing_session and existing_session.username == normalized_username:
        return existing_session

    if existing_session and existing_session.username != normalized_username:
        await end_live_time_session(
            client_id,
            reason="change_username",
            notify=True,
        )

    session = AppLiveSession(
        id=str(uuid.uuid4()),
        username=normalized_username,
        started_at=now_iso(),
        comment_count=0,
    )

    active_sessions[client_id] = session

    await send_to_client(
        client_id,
        "LIVE_TIME_STARTED",
        live_time_payload(session, reason=reason),
    )

    log(
        "LIVE TIME STARTED",
        "| clientId:",
        client_id,
        "| username:",
        normalized_username,
        "| session:",
        session.id,
        "| reason:",
        reason,
    )

    return session


async def end_live_time_session(
    client_id: str,
    *,
    reason: str = "unsubscribe",
    notify: bool = True,
):
    session = active_sessions.pop(client_id, None)

    if not session:
        return None

    ended_at = now_iso()
    payload = live_time_payload(session, ended_at=ended_at, reason=reason)

    if notify:
        await send_to_client(client_id, "LIVE_TIME_ENDED", payload)

    log(
        "LIVE TIME ENDED",
        "| clientId:",
        client_id,
        "| username:",
        session.username,
        "| session:",
        session.id,
        "| duration:",
        payload["durationSeconds"],
        "| comments:",
        session.comment_count,
        "| reason:",
        reason,
    )

    return payload


def increase_live_time_comment_count(client_id: str):
    session = active_sessions.get(client_id)

    if session:
        session.comment_count += 1


def get_live_time_status_payload(client_id: str):
    session = active_sessions.get(client_id)

    if not session:
        return {
            "running": False,
            "createdAt": now_iso(),
            "created_at": now_iso(),
        }

    payload = live_time_payload(session, reason="status")
    payload["running"] = True
    payload["durationSeconds"] = calc_duration_seconds(session.started_at, now_iso())
    payload["duration_seconds"] = payload["durationSeconds"]

    return payload


async def broadcast_to_room(room: TikTokRoom, event: str, payload: dict):
    log(
        "ROOM BROADCAST",
        "| room:",
        room.username,
        "| event:",
        event,
        "| subscribers:",
        len(room.subscribers),
    )

    if not room.subscribers:
        return

    disconnected = []

    for client_id in list(room.subscribers):
        outgoing_payload = payload

        if event == "COMMENT":
            session = await start_live_time_session(
                client_id,
                room.username,
                reason="first_comment",
            )

            outgoing_payload = {
                **payload,
                "liveSessionId": session.id,
                "live_session_id": session.id,
                "liveSessionStartedAt": session.started_at,
                "live_session_started_at": session.started_at,
            }

        ok = await send_to_client(client_id, event, outgoing_payload)

        if ok and event == "COMMENT":
            increase_live_time_comment_count(client_id)

        if not ok:
            disconnected.append(client_id)

    for client_id in disconnected:
        await unsubscribe_client(client_id, notify=False, schedule_stop=True, reason="sse_missing")


def create_tiktok_client_for_room(room: TikTokRoom) -> TikTokLiveClient:
    username = room.username
    client = TikTokLiveClient(unique_id=username)

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent):
        log("======================================")
        log("CONNECTED TO TIKTOK LIVE ROOM", telegram=True)
        log("TikTok username:", username)
        log("Room ID:", client.room_id)
        log("Subscribers:", len(room.subscribers))
        log("======================================")

        await broadcast_to_room(
            room,
            "LIVE_CONNECTED",
            {
                "username": username,
                "roomId": str(client.room_id),
                "createdAt": now_iso(),
            },
        )

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):
        raw_text = getattr(event, "comment", "") or ""
        text = normalize_comment_text(raw_text)

        if not text:
            log("COMMENT IGNORED | Empty text")
            return

        username_display, unique_id, avatar = get_comment_user(event)

        comment = {
            "id": make_comment_id(unique_id or username_display, text),
            "username": username_display,
            "avatar": avatar,
            "avatarUrl": avatar,
            "profilePictureUrl": avatar,
            "uniqueId": unique_id,
            "text": text,
            "comment": text,
            "raw_text": text,
            "intent": "buying",
            "tiktokLiveUsername": username,
            "createdAt": now_iso(),
            "created_at": now_iso(),
        }

        room.latest_comments.insert(0, comment)
        del room.latest_comments[MAX_COMMENTS:]

        metrics["total_comments"] += 1
        metrics["comment_timestamps"].append(time.time())

        log("======================================")
        log("NEW COMMENT")
        log("Live room:", username)
        log("Username:", username_display)
        log("Unique ID:", unique_id)
        log("Avatar:", avatar)
        log("Text:", text)
        log("Subscribers:", len(room.subscribers))
        log("======================================")

        if TELEGRAM_SEND_COMMENTS:
            telegram_fire_and_forget(
                "\n".join(
                    [
                        "💬 New TikTok LIVE comment",
                        f"LIVE: {username}",
                        f"User: {username_display}",
                        f"Text: {text}",
                        f"Time: {now_iso()}",
                    ]
                )
            )

        await broadcast_to_room(room, "COMMENT", comment)

    @client.on(DisconnectEvent)
    async def on_disconnect(event: DisconnectEvent):
        log("TIKTOK LIVE DISCONNECTED:", username, telegram=True)

        await broadcast_to_room(
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

    try:
        while room.username in rooms:
            if not room.subscribers:
                log("ROOM HAS NO SUBSCRIBERS, STOP COLLECTOR:", room.username)
                break

            try:
                log("STARTING ROOM COLLECTOR:", room.username)

                client = create_tiktok_client_for_room(room)
                room.client = client

                try:
                    is_live = await client.is_live()
                    log("TikTok is_live:", room.username, is_live)

                    if is_live is False:
                        await broadcast_to_room(
                            room,
                            "LIVE_ERROR",
                            {
                                "message": "Account đang không LIVE",
                                "username": room.username,
                                "createdAt": now_iso(),
                                "shouldStop": True,
                            },
                        )

                        log("ACCOUNT OFFLINE, STOP COLLECTOR:", room.username)
                        break

                except Exception as error:
                    error_text = str(error)
                    log("Check is_live error:", room.username, error_text)

                    await broadcast_to_room(
                        room,
                        "LIVE_ERROR",
                        {
                            "message": error_text,
                            "username": room.username,
                            "createdAt": now_iso(),
                            "shouldStop": True,
                        },
                    )

                    log("CHECK LIVE ERROR, STOP COLLECTOR:", room.username)
                    break

                if not room.subscribers:
                    log("NO SUBSCRIBERS BEFORE client.start(), STOP:", room.username)
                    break

                result = await client.start()

                if isinstance(result, asyncio.Task):
                    log("TikTokLive returned task, waiting task:", room.username)
                    await result

                log("TikTokLive room stopped:", room.username)

                await broadcast_to_room(
                    room,
                    "LIVE_ERROR",
                    {
                        "message": "TikTokLive room stopped",
                        "username": room.username,
                        "createdAt": now_iso(),
                        "shouldStop": True,
                    },
                )

                log("ROOM STOPPED, STOP COLLECTOR:", room.username)
                break

            except asyncio.CancelledError:
                log("ROOM COLLECTOR CANCELLED:", room.username)
                raise

            except Exception as error:
                error_text = repr(error)

                log("ROOM COLLECTOR ERROR:", room.username, error_text, telegram=True)

                await broadcast_to_room(
                    room,
                    "LIVE_ERROR",
                    {
                        "message": str(error),
                        "username": room.username,
                        "createdAt": now_iso(),
                        "shouldStop": True,
                    },
                )

                log("LIVE_ERROR SENT, STOP COLLECTOR:", room.username)
                break

    finally:
        room.is_running = False
        room.client = None
        room.task = None

        log("ROOM COLLECTOR STOPPED:", room.username)


async def stop_room(room: TikTokRoom):
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


async def get_or_create_room(username: str) -> TikTokRoom:
    normalized_username = normalize_tiktok_username(username)

    async with rooms_lock:
        room = rooms.get(normalized_username)

        if not room:
            room = TikTokRoom(username=normalized_username)
            rooms[normalized_username] = room
            log("ROOM CREATED:", normalized_username, telegram=True)

        return room


async def start_room_if_needed(room: TikTokRoom):
    async with rooms_lock:
        if not room.task or room.task.done():
            room.task = asyncio.create_task(run_room_collector(room))
            log("ROOM STARTED:", room.username, telegram=True)


async def delayed_stop_empty_room(username: str, delay: int):
    try:
        if delay > 0:
            await asyncio.sleep(delay)

        async with rooms_lock:
            room = rooms.get(username)

            if not room:
                return

            if room.subscribers:
                log("SKIP STOP ROOM | Has subscribers again:", username)
                return

            rooms.pop(username, None)

        await stop_room(room)
        log("ROOM REMOVED BECAUSE EMPTY:", username, telegram=True)

    except asyncio.CancelledError:
        log("DELAYED STOP ROOM CANCELLED:", username)
        raise


async def schedule_stop_empty_room(username: str):
    if not AUTO_STOP_ROOM_WHEN_EMPTY:
        return

    async with rooms_lock:
        room = rooms.get(username)

        if not room or room.subscribers:
            return

        if room.empty_stop_task and not room.empty_stop_task.done():
            return

        room.empty_stop_task = asyncio.create_task(
            delayed_stop_empty_room(username, AUTO_STOP_EMPTY_ROOM_DELAY)
        )

    log("ROOM EMPTY | Scheduled stop", username, f"after {AUTO_STOP_EMPTY_ROOM_DELAY}s", telegram=True)


async def get_or_create_client(client_id: str) -> SseClient:
    async with clients_lock:
        client = clients.get(client_id)

        if client:
            return client

        client = SseClient(
            id=client_id,
            connected_at=now_iso(),
        )
        clients[client_id] = client
        metrics["total_clients_connected"] += 1

        return client


async def unsubscribe_client(
    client_id: str,
    *,
    notify: bool = True,
    schedule_stop: bool = True,
    reason: str = "unsubscribe",
):
    username = client_subscriptions.pop(client_id, None)
    client = clients.get(client_id)

    if client:
        client.username = ""

    await end_live_time_session(client_id, reason=reason, notify=notify)

    if not username:
        return

    room = rooms.get(username)

    if room:
        room.subscribers.discard(client_id)

        log(
            "CLIENT UNSUBSCRIBED",
            "| clientId:",
            client_id,
            "| username:",
            username,
            "| room subscribers:",
            len(room.subscribers),
            telegram=True,
        )

        if notify:
            await send_to_client(
                client_id,
                "UNSUBSCRIBED",
                {
                    "username": username,
                    "createdAt": now_iso(),
                },
            )

    if schedule_stop:
        asyncio.create_task(schedule_stop_empty_room(username))


async def cleanup_client(client_id: str, *, reason: str = "sse_disconnect"):
    await unsubscribe_client(
        client_id,
        notify=False,
        schedule_stop=True,
        reason=reason,
    )

    async with clients_lock:
        if clients.pop(client_id, None):
            metrics["total_clients_disconnected"] += 1

    log(
        "SSE CLIENT DISCONNECTED",
        "| clientId:",
        client_id,
        "| active clients:",
        len(clients),
        "| active rooms:",
        list(rooms.keys()),
    )


async def subscribe_client_to_username(client_id: str, username: str):
    client = await get_or_create_client(client_id)
    next_username = normalize_tiktok_username(username)
    old_username = client_subscriptions.get(client_id)

    if old_username == next_username:
        room = await get_or_create_room(next_username)
        room.subscribers.add(client_id)
        client_subscriptions[client_id] = next_username
        client.username = next_username

        await send_to_client(
            client_id,
            "SUBSCRIBED",
            {
                "username": next_username,
                "comments": room.latest_comments,
                "subscriberCount": len(room.subscribers),
                "createdAt": now_iso(),
            },
        )

        await start_room_if_needed(room)
        return room

    if old_username and old_username != next_username:
        await unsubscribe_client(client_id, notify=True, schedule_stop=True, reason="change_username")

    await send_to_client(
        client_id,
        "SUBSCRIBING",
        {
            "username": next_username,
            "oldUsername": old_username,
            "createdAt": now_iso(),
        },
    )

    room = await get_or_create_room(next_username)

    if room.empty_stop_task and not room.empty_stop_task.done():
        room.empty_stop_task.cancel()
        room.empty_stop_task = None
        log("ROOM STOP CANCELLED | Subscriber returned:", next_username)

    room.subscribers.add(client_id)
    client_subscriptions[client_id] = next_username
    client.username = next_username

    await send_to_client(
        client_id,
        "SUBSCRIBED",
        {
            "username": next_username,
            "comments": room.latest_comments,
            "subscriberCount": len(room.subscribers),
            "createdAt": now_iso(),
        },
    )

    await start_room_if_needed(room)

    log(
        "CLIENT SUBSCRIBED",
        "| clientId:",
        client_id,
        "| username:",
        next_username,
        "| old username:",
        old_username,
        "| room subscribers:",
        len(room.subscribers),
        telegram=True,
    )

    return room


def get_metrics_payload():
    now = time.time()

    recent_comments = [ts for ts in metrics["comment_timestamps"] if now - ts <= 60]
    latencies = list(metrics["send_latency_ms"])
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    max_latency = max(latencies) if latencies else 0
    total_subscribers = sum(len(room.subscribers) for room in rooms.values())

    return {
        "uptimeSeconds": int(now - metrics["started_at"]),
        "activeRooms": len(rooms),
        "activeClients": len(clients),
        "activeSubscribers": total_subscribers,
        "totalComments": metrics["total_comments"],
        "totalSent": metrics["total_sent"],
        "totalSendError": metrics["total_send_error"],
        "totalClientsConnected": metrics["total_clients_connected"],
        "totalClientsDisconnected": metrics["total_clients_disconnected"],
        "commentsPerMinute": len(recent_comments),
        "commentsPerSecond": round(len(recent_comments) / 60, 2),
        "avgSendLatencyMs": round(avg_latency, 2),
        "maxSendLatencyMs": round(max_latency, 2),
        "rooms": [
            {
                "username": username,
                "subscribers": len(room.subscribers),
                "latestComments": len(room.latest_comments),
                "isRunning": room.is_running,
            }
            for username, room in rooms.items()
        ],
        "createdAt": now_iso(),
    }


@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "TikTok Live SSE Comment Server",
        "events": "/events?clientId=YOUR_CLIENT_ID",
        "subscribe": "POST /subscribe",
        "stop": "POST /stop",
        "metrics": "/metrics",
    }


@app.get("/health")
async def health():
    return {"ok": True, "createdAt": now_iso()}


@app.get("/metrics")
async def metrics_route():
    return get_metrics_payload()


@app.get("/live-time-status")
async def live_time_status(clientId: str):
    return get_live_time_status_payload(clientId)


@app.post("/subscribe")
async def subscribe(body: SubscribeBody):
    if not body.clientId:
        return {"ok": False, "message": "Missing clientId"}

    if not body.username:
        return {"ok": False, "message": "Missing username"}

    room = await subscribe_client_to_username(body.clientId, body.username)

    return {
        "ok": True,
        "clientId": body.clientId,
        "username": room.username,
        "subscriberCount": len(room.subscribers),
    }


@app.post("/stop")
async def stop(body: StopBody):
    if not body.clientId:
        return {"ok": False, "message": "Missing clientId"}

    await unsubscribe_client(
        body.clientId,
        notify=True,
        schedule_stop=True,
        reason="app_stop",
    )

    return {"ok": True, "clientId": body.clientId}


@app.get("/events")
async def events(request: Request, clientId: str):
    if not clientId:
        clientId = str(uuid.uuid4())

    client = await get_or_create_client(clientId)

    async def event_generator():
        log("SSE CLIENT CONNECTED", "| clientId:", clientId, "| active clients:", len(clients))

        await send_to_client(
            clientId,
            "CONNECTED",
            {
                "message": "Connected to TikTok SSE comment server",
                "clientId": clientId,
                "defaultTikTokUsername": DEFAULT_TIKTOK_USERNAME,
                "serverTime": now_iso(),
            },
        )

        try:
            while True:
                if await request.is_disconnected():
                    break

                try:
                    item = await asyncio.wait_for(client.queue.get(), timeout=15)

                    yield build_sse(
                        str(item.get("event") or "MESSAGE"),
                        item.get("payload") or {},
                    )

                except asyncio.TimeoutError:
                    yield build_sse_ping()

        finally:
            await cleanup_client(clientId, reason="sse_disconnect")

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def shutdown_all_rooms():
    log("STOPPING ROOMS...")

    async with rooms_lock:
        active_rooms = list(rooms.values())
        rooms.clear()

    for room in active_rooms:
        await stop_room(room)

    log("ALL ROOMS STOPPED")


@app.on_event("shutdown")
async def on_shutdown():
    await shutdown_all_rooms()


if __name__ == "__main__":
    import uvicorn

    lan_ip = get_lan_ip()

    log("======================================")
    log("STARTING TIKTOK LIVE SSE SERVER", telegram=True)
    log("Bind host:", HTTP_HOST)
    log("Port:", HTTP_PORT)
    log("Local URL:", f"http://localhost:{HTTP_PORT}")
    log("LAN URL:", f"http://{lan_ip}:{HTTP_PORT}")
    log("Events URL:", f"http://localhost:{HTTP_PORT}/events?clientId=YOUR_CLIENT_ID")
    log("Default TikTok username:", DEFAULT_TIKTOK_USERNAME)
    log("Message: GET /events + POST /subscribe / POST /stop / GET /live-time-status")
    log("======================================")

    uvicorn.run(
        "comment_tiktok_live_sse:app",
        host=HTTP_HOST,
        port=HTTP_PORT,
        reload=False,
    )
