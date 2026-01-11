from typing import List

import polars as pl
import requests

from db import DuckDBConnector


def get_exchange_rates_per_date(
    dates: List, currency_pairs: List[dict]
) -> pl.DataFrame:
    """
    Fetch exchange rates for given dates and currency pairs.

    Args:
        dates: List of date strings in format 'YYYY-MM-DD'
        currency_pairs: List of dicts with keys 'id', 'base', 'quote'
                        e.g. [{"id": 1, "base": "usd", "quote": "eur"}]

    Returns:
        List of dicts with 'currency_pair_id', 'value', 'date'
    """
    exchange_rates = []
    processed_dates = 0
    for date in dates:
        for pair in currency_pairs:
            response = requests.get(
                f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date[0]}/v1/currencies/{pair[1]}.json"
            )
            rates = response.json()[pair[1]]

            exchange_rates.append(
                {
                    "currency_pair_id": pair[0],
                    "value": rates[pair[2]],
                    "date": date[0],
                }
            )
        processed_dates += 1
        print(
            f"Processed exchange rates for date {date}. Processed dates: {processed_dates} Remaining dates: {len(dates) - processed_dates}"
        )
    return pl.DataFrame(exchange_rates)


def main():
    print("Starting process...")
    db = DuckDBConnector()

    print("Getting missing exchange rates dates...")
    dates_to_fetch = db.get_dates_missing_exchange_rates()

    if not dates_to_fetch:
        print("No missing exchange rates for the existing dates. DB is up to date.")
        return

    print("Getting currency pairs formatted...")
    currency_pairs = db.get_currency_pairs_formatted()

    print("Fetching exchange rates...")
    ex_rates_per_date = get_exchange_rates_per_date(dates_to_fetch, currency_pairs)

    print("Inserting exchange rates in database...")
    db.insert_exchange_rates(ex_rates_per_date)

    print("Processed completed")


if __name__ == "__main__":
    main()
