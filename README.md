# True Stream V6

Telegram File-to-Stream bot using:

- Telegram Bot API webhook for incoming updates/replies
- Pyrogram MTProto for storage-channel access and Telegram media streaming
- Supabase persistence for users, links, Prime memberships and payment proofs
- `/start` welcome command
- `/prime` Prime status and remaining expiry time
- `/status` admin-only bot statistics
- 3 free links per rolling 24 hours
- Prime plans: ₹3/24h, ₹23/10d, ₹50/30d, ₹499/365d, ₹1999/lifetime
- Admin approval/rejection for payment screenshots
- Browser HTML5 streaming
- HTTP Range support for seeking and partial playback
- Direct download
- Android MX Player and VLC intent buttons
- Automatic expiry cleanup for free links

## Webhook

The Bot API receiver uses Telegram webhook delivery instead of `getUpdates` long polling. This avoids the old polling layer and the observed 30–34 second response pattern.

The webhook endpoint is:

`BASE_URL/telegram/webhook`

A secret token is sent in `X-Telegram-Bot-Api-Secret-Token`. Set `WEBHOOK_SECRET` in the environment for an explicit secret, or leave it blank and the app derives a stable secret from `BOT_TOKEN`.

## Setup

1. Run `supabase_schema_v6.sql` in Supabase.
2. Keep secrets in `.env` locally and Render environment variables in production.
3. Make the bot an admin in the storage channel with permission to post/delete messages.
4. Set `BASE_URL` to the public HTTPS Render URL.
5. Deploy with `python app.py`.

Do not commit `.env` or Pyrogram `.session` files.
