# True Stream V8

V8 keeps the existing Prime/payment behavior and webhook architecture.

## Main fixes
- Large-file HTTP Range requests use a persistent per-file MTProto-backed cache instead of calling `bot.stream_media()` for every Range request.
- The cache worker keeps one media generator alive and serves later ranges from the cached bytes.
- `bot_stats` is optional; a missing `bot_stats` REST resource falls back to counting `links` instead of breaking link creation/status.
- `/help` remains in the Chat Menu.
- Prime status logic is otherwise kept unchanged from V7.

# Render
Start command: `python app.py`

Keep all secrets in Render environment variables. Do not commit `.env` or Telegram session files.
