import asyncio
import json
import time
from collections import defaultdict

import websockets

import resource

def increase_open_file_limit():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = min(4096, hard)

    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        print(f"Open file limit: {soft} -> {target}")
    except Exception as error:
        print("Cannot increase open file limit:", error)
        print("Current limit:", soft, hard)

increase_open_file_limit()


WS_URL = "ws://localhost:8765"
TOTAL_CLIENTS = 100
# Mỗi client giữ kết nối trong bao lâu
DURATION_SECONDS = 60*30
# Mỗi bao lâu mới start 1 collection/1 username
COLLECTION_DELAY_SECONDS = 5
# Nếu muốn 1 phút thì đổi thành:
# COLLECTION_DELAY_SECONDS = 60

CONNECT_CONCURRENCY = 20
CONNECT_TIMEOUT_SECONDS = 8
CLIENT_START_DELAY_SECONDS = 0.03

connect_semaphore = asyncio.Semaphore(CONNECT_CONCURRENCY)

REPORT_INTERVAL = 3

received_count = 0
error_count = 0
connected_count = 0
disconnected_count = 0
total_connect_count = 0
total_disconnect_count = 0
message_type_count = defaultdict(int)

stats_by_username = defaultdict(lambda: {
    "connected": 0,
    "disconnected": 0,
    "received": 0,
    "comments": 0,
    "live_connected": 0,
    "live_error": 0,
    "errors": 0,
})

connect_latencies_ms = []



def normalize_username(username: str) -> str:
    value = str(username or "").strip()

    # Xoá dấu nháy thừa
    value = value.strip("'").strip('"').strip()

    # Nếu bị dạng '@abc' bên trong dấu nháy
    value = value.replace("'", "").replace('"', "").strip()

    if not value:
        return ""

    return value if value.startswith("@") else f"@{value}"


def parse_usernames(raw: str) -> list[str]:
    usernames = []

    for item in raw.split(","):
        username = normalize_username(item)
        if username:
            usernames.append(username)

    return usernames


async def fake_browser_client(index: int, username: str):
    global received_count, error_count, connected_count, disconnected_count
    global total_connect_count, total_disconnect_count

    connect_started = time.perf_counter()
    ws = None

    try:
        async with connect_semaphore:
            ws = await asyncio.wait_for(
                websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=10_000_000,
                    open_timeout=CONNECT_TIMEOUT_SECONDS,
                    close_timeout=2,
                ),
                timeout=CONNECT_TIMEOUT_SECONDS + 2,
            )

        connect_ms = (time.perf_counter() - connect_started) * 1000
        connect_latencies_ms.append(connect_ms)

        connected_count += 1
        total_connect_count += 1
        stats_by_username[username]["connected"] += 1

        await ws.send(
            json.dumps(
                {
                    "type": "SUBSCRIBE_TIKTOK_USERNAME",
                    "payload": {
                        "username": username,
                    },
                },
                ensure_ascii=False,
            )
        )

        started = time.time()

        while time.time() - started < DURATION_SECONDS:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=1)

                received_count += 1
                stats_by_username[username]["received"] += 1

                try:
                    data = json.loads(message)
                    msg_type = str(data.get("type") or "UNKNOWN")

                    message_type_count[msg_type] += 1

                    if msg_type == "COMMENT":
                        stats_by_username[username]["comments"] += 1

                    elif msg_type == "LIVE_CONNECTED":
                        stats_by_username[username]["live_connected"] += 1

                    elif msg_type == "LIVE_ERROR":
                        stats_by_username[username]["live_error"] += 1

                except Exception:
                    message_type_count["INVALID_JSON"] += 1

            except asyncio.TimeoutError:
                pass

    except Exception as error:
        error_count += 1
        stats_by_username[username]["errors"] += 1
        print(f"[ERROR] client={index} username={username} error={error}", flush=True)

    finally:
        if ws:
            try:
                await ws.close()
            except Exception:
                pass

        disconnected_count += 1
        total_disconnect_count += 1
        stats_by_username[username]["disconnected"] += 1
async def report_loop(started_at: float, done_event: asyncio.Event):
    last_received = 0
    last_comment = 0
    last_time = started_at

    while not done_event.is_set():
        await asyncio.sleep(REPORT_INTERVAL)

        now = time.time()
        elapsed = now - started_at
        interval = now - last_time

        current_received = received_count
        current_comment = message_type_count["COMMENT"]

        interval_msg = current_received - last_received
        interval_comment = current_comment - last_comment

        active_clients = connected_count - disconnected_count

        interval_msg_per_sec = interval_msg / interval if interval > 0 else 0
        avg_msg_per_sec = received_count / elapsed if elapsed > 0 else 0

        interval_comment_per_sec = interval_comment / interval if interval > 0 else 0
        avg_comment_per_sec = current_comment / elapsed if elapsed > 0 else 0

        last_received = current_received
        last_comment = current_comment
        last_time = now
def print_final_report(started_at: float):
    duration = time.time() - started_at

    print(f"clients expected      : {TOTAL_CLIENTS}")

    avg_connect_ms = (
        sum(connect_latencies_ms) / len(connect_latencies_ms)
        if connect_latencies_ms
        else 0
    )

    max_connect_ms = max(connect_latencies_ms) if connect_latencies_ms else 0

    print("\n\n========== FINAL PERFORMANCE RESULT ==========")
    print(f"duration              : {duration:.2f}s")
    print(f"clients expected      : {CLIENTS}")
    print(f"clients connected     : {connected_count}")
    print(f"clients disconnected  : {disconnected_count}")
    print(f"total connect         : {total_connect_count}")
    print(f"total disconnect      : {total_disconnect_count}")
    print(f"errors                : {error_count}")
    print(f"total messages        : {received_count}")
    print(f"avg messages/sec      : {received_count / duration:.2f}")
    print(f"total comments        : {message_type_count['COMMENT']}")
    print(f"avg comments/sec      : {message_type_count['COMMENT'] / duration:.2f}")
    print(f"avg connect latency   : {avg_connect_ms:.2f} ms")
    print(f"max connect latency   : {max_connect_ms:.2f} ms")
    print("message types         :", dict(message_type_count))
    print("==============================================")

    print("\n========== RESULT BY USERNAME ==========")

async def main():
    raw_usernames = input(
        "Nhập username live, cách nhau bằng dấu phẩy: "
    ).strip()

    usernames = parse_usernames(raw_usernames)

    if not usernames:
        print("Bạn chưa nhập username nào.")
        return

    started_at = time.time()

    client_tasks = []
    done_event = asyncio.Event()

    reporter_task = asyncio.create_task(report_loop(started_at, done_event))

    base_clients_per_collection = TOTAL_CLIENTS // len(usernames)
    extra_clients = TOTAL_CLIENTS % len(usernames)

    client_index = 0

    for collection_index, username in enumerate(usernames):
        clients_for_this_username = base_clients_per_collection

        if collection_index < extra_clients:
            clients_for_this_username += 1

        for _ in range(clients_for_this_username):
            client_tasks.append(
                asyncio.create_task(
                    fake_browser_client(client_index, username)
                )
            )

            client_index += 1

            await asyncio.sleep(CLIENT_START_DELAY_SECONDS)

        is_last_collection = collection_index == len(usernames) - 1

        if not is_last_collection:
            print(
                f"Đợi {COLLECTION_DELAY_SECONDS}s rồi mới start collection tiếp theo..."
            )
            await asyncio.sleep(COLLECTION_DELAY_SECONDS)

    await asyncio.gather(*client_tasks)

    done_event.set()
    await reporter_task

    print_final_report(started_at)

if __name__ == "__main__":
    asyncio.run(main())