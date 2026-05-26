# Telegram log setup

## 1. Tạo bot
Vào Telegram tìm `@BotFather`.

Gửi:

```txt
/newbot
```

Làm theo hướng dẫn và lấy `BOT_TOKEN`.

## 2. Lấy chat_id

Nhắn một tin bất kỳ cho bot của bạn trước.

Sau đó mở link này trên browser:

```txt
https://api.telegram.org/bot<BOT_TOKEN>/getUpdates
```

Tìm:

```json
"chat": { "id": 123456789 }
```

Số đó là `TELEGRAM_CHAT_ID`.

## 3. Chạy Python kèm Telegram

```bash
export TELEGRAM_BOT_TOKEN="BOT_TOKEN_CUA_BAN"
export TELEGRAM_CHAT_ID="CHAT_ID_CUA_BAN"

python3 python/read_tiktok_live_multi_room.py
```

## 4. Gửi toàn bộ log

Mặc định chỉ gửi log quan trọng.

Muốn gửi toàn bộ log:

```bash
export TELEGRAM_LOG_ALL=1
python3 python/read_tiktok_live_multi_room.py
```

## 5. Gửi cả comment lên Telegram

Cẩn thận vì live nhiều comment sẽ spam Telegram.

```bash
export TELEGRAM_SEND_COMMENTS=1
python3 python/read_tiktok_live_multi_room.py
```
