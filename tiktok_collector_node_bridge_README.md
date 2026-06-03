# TikTok Collector Node Bridge

Flow:

```txt
Python TikTok Collector -> Node.js Backend -> Supabase/Redis -> SSE -> Next.js Client
```

Python chỉ làm 3 việc:

1. Start/stop TikTokLive collector theo username.
2. Nhận comment TikTok và chuẩn hóa payload.
3. Ghi SQLite outbox rồi POST sang Node.js để Node lưu DB + broadcast SSE.

Python không còn SSE trực tiếp cho client và không còn AI/rule priority.

## Install

```bash
pip install fastapi uvicorn python-dotenv TikTokLive
```

## Run

```bash
cp tiktok_collector_node_bridge.env.example .env
python3 tiktok_collector_node_bridge_updated.py
```

## Start collector

```bash
curl -X POST http://localhost:8765/collectors/start \
  -H "Content-Type: application/json" \
  -H "x-collector-api-key: change_me" \
  -d '{"username":"@theunbeatablequeen26"}'
```

## Stop collector

```bash
curl -X POST http://localhost:8765/collectors/stop \
  -H "Content-Type: application/json" \
  -H "x-collector-api-key: change_me" \
  -d '{"username":"@theunbeatablequeen26"}'
```

## Node endpoint cần có

Python sẽ gửi comment sang:

```txt
POST /api/internal/live-comments/ingest
```

Header:

```txt
x-internal-api-key: change_me
```

Node nên lưu Supabase trước, sau đó broadcast SSE cho Client.
