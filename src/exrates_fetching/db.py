import os

import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()


class DuckDBConnector:
    def __init__(self):
        self.db_path = "md:assets"
        self._setup_motherduck_token()
        self.conn = duckdb.connect(self.db_path)

    def _setup_motherduck_token(self) -> None:
        """Set up MotherDuck token if available."""
        token = os.getenv("MOTHERDUCK_TOKEN")
        if token:
            os.environ["motherduck_token"] = token

    def get_currency_pairs_formatted(self) -> list:
        """
        Returns currency pairs formatted for exchange rate fetching.

        Returns:
            List of dicts: [{"id": 1, "base": "eur", "quote": "usd"}, ...]
        """
        return self.conn.execute("""
            SELECT
                cp.id,
                LOWER(c1.name) as base,
                LOWER(c2.name) as quote,
            FROM currency_pairs cp
            JOIN currencies c1 ON cp.base_currency_id = c1.id
            JOIN currencies c2 ON cp.quote_currency_id = c2.id
            """).fetchall()

    def insert_exchange_rates(self, df: pl.DataFrame) -> None:
        """
        Inserts fetched exchange rates into table

        Args:
            Dataframe including exchange rates with cols currency_pair_id, value, date

        Raises:
            Exception: If insert fails
        """
        try:
            self.conn.execute("""INSERT INTO exchange_rates SELECT * from df""")
        except Exception:
            raise

    def get_dates_missing_exchange_rates(self) -> list:
        """
        Fetch dates that exist in asset_values but not in exchange_rates
        """
        return self.conn.execute("""
            SELECT DISTINCT strftime(date, '%Y-%m-%d') as date
            FROM asset_values av
            WHERE NOT EXISTS (
                SELECT 1 from exchange_rates er
                WHERE strftime(er.date, '%Y-%m-%d') = strftime(av.date, '%Y-%m-%d')
            )
            """).fetchall()
