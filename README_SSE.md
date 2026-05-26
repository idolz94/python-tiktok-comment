# TikTok Live SSE Comment Server

Bản này đổi từ WebSocket sang SSE + REST API.

## Chạy local

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-sse.txt
python comment_tiktok_live_sse.py
```

Server mặc định: `http://localhost:8765`

## API

- `GET /events?clientId=abc123`: mở SSE để nhận event realtime.
- `POST /subscribe`: subscribe username.
- `POST /stop`: dừng nhận comment.
- `GET /live-time-status?clientId=abc123`: lấy trạng thái phiên.
- `GET /metrics`: xem metrics server.

## Next.js example

```ts
const clientId = crypto.randomUUID();

const eventSource = new EventSource(
  `http://localhost:8765/events?clientId=${clientId}`
);

eventSource.addEventListener("COMMENT", (event) => {
  const comment = JSON.parse(event.data);
  console.log("COMMENT:", comment);
});

await fetch("http://localhost:8765/subscribe", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    clientId,
    username: "@minxinh_vailozz_2709",
  }),
});
```

## Production

Set CORS domain thật:

```bash
export CORS_ORIGINS="https://your-domain.com,http://localhost:3000"
```

Nếu dùng Nginx trước `/events`, cần tắt buffering.
