# Denni Kvizove Otazky

Automated pipeline that scrapes Czech news daily, extracts facts, and generates 40 quiz questions for [Vyzyvatel](https://vyzyvatel.com).

## How it works

The pipeline runs daily at a scheduled time (default 11:00 Prague time) and goes through 6 steps:

1. **Scraping** — fetches articles from 25 Czech news RSS feeds with per-domain rate limiting, User-Agent rotation, and optional SOCKS5 proxy fallback via Cloudflare WARP
2. **Fact extraction** — gpt-5-mini extracts concrete facts (numbers, names, places, results)
3. **Categorization** — hybrid approach: Python keyword matching for ~80% of facts, gpt-5-mini for the rest. 8 categories (politics, sport, economy, tech, culture, science, society, world)
4. **Question generation** — gpt-5.4 creates 20 multiple-choice + 20 number questions with validation, deduplication, and backfill
5. **Upload** — sends questions to Vyzyvatel API at publish time (default 12:00)
6. **Cleanup** — deletes questions older than 7 days

## Requirements

- Docker
- OpenAI API key(s) with free tier access
- Vyzyvatel API key + set ID
- (optional) Discord webhook for notifications
- (optional) Cloudflare WARP config for proxy fallback

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `OPENAI_API_KEY` | Yes | Primary OpenAI API key |
| `OPENAI_API_KEY2` | No | Secondary API key for token budget distribution |
| `VYZYVATEL_API_KEY` | Yes | Vyzyvatel API key |
| `VYZYVATEL_SET_ID` | Yes | Question set ID |
| `DISCORD_WEBHOOK_URL` | No | Discord webhook for daily reports |
| `DISCORD_DASHBOARD_MSG_ID` | No | Discord message ID to edit with stats dashboard |
| `WG_CONF_BASE64` | No | Base64-encoded WireGuard/WARP config for proxy fallback |
| `DRY_RUN` | No | Set to `true` to test all connections without generating |

## Quick start

```bash
docker build -t daily-quiz .
docker run -d \
  -e OPENAI_API_KEY=sk-... \
  -e OPENAI_API_KEY2=sk-... \
  -e VYZYVATEL_API_KEY=... \
  -e VYZYVATEL_SET_ID=... \
  -e DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/... \
  -e WG_CONF_BASE64=$(base64 -w0 wg.conf) \
  -v quiz-data:/app/daily_questions \
  daily-quiz
```

## Dry run

Test all connections without generating anything:

```bash
docker run --rm -e DRY_RUN=true -e OPENAI_API_KEY=... daily-quiz
```

This tests: wireproxy tunnel, OpenAI API keys, all RSS feeds, scraping (direct + proxy), and Vyzyvatel API.

## Cost

Designed to stay within OpenAI free tier limits with 2 API keys:

- gpt-5-mini: ~100K-300K / 2.5M tokens per key per day
- gpt-5.4: ~55K-160K / 250K tokens per key per day

## License

MIT
