# 📰 Denní Kvízové Otázky

Automatický pipeline, který každý den stáhne české zprávy, vytáhne z nich fakta a vygeneruje 40 kvízových otázek pro [Vyzyvatel](https://vyzyvatel.com). Otázky starší než 7 dní se automaticky mažou — běží to v nekonečném cyklu.

## Jak to funguje

Pipeline se spustí každý den v 11:45 a projde 6 kroky:

1. **Scraping** — stáhne ~400 článků z 23 českých zpravodajských RSS feedů
2. **Extrakce faktů** — gpt-5-mini vytáhne konkrétní fakta (čísla, jména, místa, výsledky)
3. **Kategorizace** — gpt-5-mini roztřídí fakta do 8 kategorií (politika, sport, ekonomika…) a seřadí podle důležitosti
4. **Generování otázek** — gpt-5.4 vytvoří 20 výběrových + 20 číselných otázek pokrývajících všechny kategorie
5. **Upload** — ve 12:00 odešle otázky do Vyzyvatel API
6. **Cleanup** — smaže otázky starší než 7 dní

## Požadavky

- Docker
- OpenAI API klíč(e) s free tier přístupem
- Vyzyvatel API klíč + ID sady
- (volitelně) Discord webhook pro notifikace

## Environment variables

| Proměnná | Povinná | Popis |
|---|---|---|
| `OPENAI_API_KEY` | ✅ | Primární OpenAI API klíč |
| `OPENAI_API_KEY2` | ❌ | Sekundární OpenAI API klíč |
| `VYZYVATEL_API_KEY` | ✅ | API klíč k Vyzyvatel |
| `VYZYVATEL_SET_ID` | ❌ | ID sady otázek (default: 5402) |
| `DISCORD_WEBHOOK_URL` | ❌ | Discord webhook pro denní report |

## Spuštění

```bash
docker build -t daily-quiz .
docker run -d \
  -e OPENAI_API_KEY=sk-... \
  -e OPENAI_API_KEY2=sk-... \
  -e VYZYVATEL_API_KEY=... \
  -e DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/... \
  -v quiz-data:/app/daily_questions \
  daily-quiz
```

Volume `quiz-data` uchovává lockfile (ochrana proti duplicitnímu spuštění), debug logy a historické statistiky.

## Struktura

```
├── Dockerfile
├── entrypoint.sh
├── requirements.txt
├── main.py              # celý pipeline + scheduler
└── daily_questions/     # runtime data (volume)
    ├── .last_run_*      # lockfile
    ├── pipeline_stats.json
    ├── questions_*.json
    └── debug_*.log
```

## Náklady

Při správném nastavení dvou API klíčů je provoz **zdarma** díky OpenAI free tier limitům:

- gpt-5-mini: ~620K / 2.5M tokenů denně
- gpt-5.4: ~55K / 200K tokenů na klíč (2 klíče = 400K celkem)

## Discord notifikace

Po každém runu přijde embed se statusem, počtem otázek, časováním každého kroku, scrape chybami a spotřebou tokenů per klíč.
