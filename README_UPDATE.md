# TikTok Live SSE Priority Update

## Files to copy

### Python

1. `comment_tiktok_live_sse.py`
   - Replace your current server file with this file.
   - Keeps your current API:
     - `GET /events?clientId=...`
     - `POST /subscribe`
     - `POST /stop`
     - `GET /live-time-status`
   - Adds:
     - `COMMENT_UPDATED` SSE event
     - `POST /feedback`

2. `comment_priority.py`
   - New file, place next to `comment_tiktok_live_sse.py`.
   - Contains:
     - System/comment noise filter
     - Rule scoring
     - Mock AI async analyzer
     - Order info extraction

### Next.js

Optional example files:

1. `nextjs/src/types/live-comment.ts`
2. `nextjs/src/hooks/useTikTokLiveSSE.ts`
3. `nextjs/app/dashboard/live/page.tsx`

Copy these into your Next.js app if you want a working sample UI.

## Run Python

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install fastapi uvicorn TikTokLive
python comment_tiktok_live_sse.py
```

## Next.js env

```env
NEXT_PUBLIC_PYTHON_LIVE_URL=http://localhost:8765
```

If testing from real phone, use LAN IP:

```env
NEXT_PUBLIC_PYTHON_LIVE_URL=http://192.168.x.x:8765
```

## Flow

1. Next.js opens `GET /events?clientId=...`
2. Next.js calls `POST /subscribe`
3. Python starts TikTokLiveClient
4. On each comment:
   - filter system comments
   - rule scoring
   - send `COMMENT` immediately
   - if comment is ambiguous, run AI in background
5. AI finished:
   - Python sends `COMMENT_UPDATED`
   - Next.js updates the existing comment
6. Seller action:
   - `created_order`
   - `ignored`
   - `marked_wrong`
   - sent to `POST /feedback`
