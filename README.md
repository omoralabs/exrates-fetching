<p align="center">
  <img src="public/omora.svg" height="80" alt="Omora Labs" />
</p>

# Exchange Rates Fetching

Automatically fetches and stores historical exchange rates for currency pairs tracked in asset values.

## What it does

1. Connects to MotherDuck database containing asset values
2. Identifies dates missing exchange rate data
3. Fetches rates from currency API for configured pairs
4. Stores results in `exchange_rates` table

## Setup

```bash
uv sync
cp .env.example .env
# Add your MOTHERDUCK_TOKEN to .env
```

## Usage

```bash
uv run src/ex_rates_fetching/script.py
```

Script runs idempotently - only fetches missing dates.

## License

This project is licensed under the MIT License.
