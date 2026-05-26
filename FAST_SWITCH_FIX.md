# Fast switch username fix

Khi app đổi từ tài khoản A sang B, bản cũ bị chậm vì Python đợi stop TikTokLive client của A.

Bản này:
- Không chờ stop room cũ khi chuyển username.
- Gửi `SUBSCRIBING` ngay cho app.
- Subscribe username mới ngay.
- Room cũ rỗng tự stop sau `AUTO_STOP_EMPTY_ROOM_DELAY` giây.

Chạy:

```bash
export AUTO_STOP_EMPTY_ROOM_DELAY=30
python3 python/read_tiktok_live_multi_room.py
```
