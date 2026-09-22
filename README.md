# True Stream V9

V9 keeps the existing Prime/payment behavior and webhook architecture.

## Main fix
- Large files (>20 MB) start a background warm-cache transfer immediately after link creation.
- The first cached bytes are served while the cache is still growing, so playback/download does not wait for the whole Telegram transfer.
- Far Range/seek requests still use the existing MTProto HTTP Range path.
- The cache is intentionally limited to the beginning of each large file; it does not try to download the entire 635 MB/GB file before playback starts.
- `/help` remains in the Chat Menu.
- Prime status/logic is unchanged.

## Optional Render environment variables
- `STREAM_CACHE_WARM_BYTES` — default `16777216` (16 MB)
- `STREAM_CACHE_MAX_FILES` — default `12`
- `STREAM_CACHE_DIR` — default `.stream_cache`

## Render
Start command: `python app.py`

Keep all secrets in Render environment variables. Do not commit `.env` or Telegram session files.
