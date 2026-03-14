# Novinky z ČR a světa

Python skript pro neustále aktualizovanou sadu otázek o aktuálním dění do hry [Vyzyvatel.com](https://vyzyvatel.com). Data z 20+ webů zpracovávají modely GPT-5-Mini a GPT-5.4.

**Sada:** [Novinky z ČR a světa](https://vyzyvatel.com/dashboard/sets/5402)

## Jak to funguje

Pipeline se spouští každý den v naplánovaný čas (výchozí 11:00 CET) a prochází 6 kroky:

1. **Scraping** — stáhne články z 25 českých zpravodajských RSS feedů s rate limitingem per doména, rotací User-Agent a volitelným SOCKS5 proxy přes Cloudflare WARP
2. **Extrakce faktů** — GPT-5-Mini vytáhne konkrétní fakta (čísla, jména, místa, výsledky)
3. **Kategorizace** — hybridní přístup: Python keyword matching pro ~80 % faktů, GPT-5-Mini pro zbytek. 8 kategorií (politika, sport, ekonomika, technologie, kultura, věda, společnost, svět)
4. **Generování otázek** — GPT-5.4 vytvoří 20 výběrových + 20 číselných otázek s validací, deduplikací a backfillem
5. **Upload** — ve 12:00 odešle otázky do Vyzyvatel API
6. **Cleanup** — smaže otázky starší než 7 dní

## Požadavky

- Docker
- OpenAI API klíč(e) s free tier přístupem
- Vyzyvatel API klíč + ID sady
- (volitelně) Discord webhook pro notifikace
- (volitelně) Cloudflare WARP config pro proxy fallback

## Environment variables

| Proměnná | Povinná | Popis |
|---|---|---|
| `OPENAI_API_KEY` | Ano | Primární OpenAI API klíč |
| `OPENAI_API_KEY2` | Ne | Sekundární API klíč pro rozložení token budgetu |
| `VYZYVATEL_API_KEY` | Ano | Vyzyvatel API klíč |
| `VYZYVATEL_SET_ID` | Ano | ID sady otázek |
| `DISCORD_WEBHOOK_URL` | Ne | Discord webhook pro denní reporty |
| `DISCORD_DASHBOARD_MSG_ID` | Ne | ID Discord zprávy pro live dashboard |
| `WG_CONF_BASE64` | Ne | Base64-encoded WireGuard/WARP config pro proxy fallback |
| `DRY_RUN` | Ne | `true` pro otestování všech připojení bez generování |

## Spuštění

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

Otestuje všechna připojení bez generování otázek:

```bash
docker run --rm -e DRY_RUN=true -e OPENAI_API_KEY=... daily-quiz
```

Testuje: wireproxy tunel, OpenAI API klíče, všechny RSS feedy, scraping (přímý + proxy) a Vyzyvatel API.

## Náklady

Navrženo pro provoz v rámci OpenAI free tier limitů se 2 API klíči:

- GPT-5-Mini: ~100K–300K / 2.5M tokenů na klíč denně
- GPT-5.4: ~55K–160K / 250K tokenů na klíč denně

## Licence

MIT
