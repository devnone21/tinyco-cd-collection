import os
import psycopg2
from typing import List, Dict, Any
from psycopg2.extras import execute_values
from psycopg2 import OperationalError
from loguru import logger
from decorators import timer


class Postgres:
    """class of PostgresSQL DB client"""

    def __init__(self, dbname: str) -> None:
        self.dbname = dbname
        self.db = None
        try:
            self.db = psycopg2.connect(
                host=os.getenv("PGSQL_HOST"),
                database=dbname,
                user=os.getenv("PGSQL_USER"),
                password=os.getenv("PGSQL_PASS"),
            )
        except OperationalError as err:
            logger.error(f"Unable to connect: {err}")
            return
        self.db.autocommit = True

    def close(self) -> None:
        if self.db:
            self.db.close()

    @timer
    def fetch_many(self, table: str, limit: int) -> List:
        with self.db.cursor() as cursor:
            cursor.execute("SELECT * FROM %s;" % table)
            res: List = cursor.fetchmany(limit)
        return res

    @timer
    def upsert_many(self, table, data, page_size: int = 1000) -> int:
        with self.db.cursor() as cursor:
            execute_values(
                cursor,
                f"""
                INSERT INTO {table} VALUES %s ON CONFLICT (id) DO NOTHING; 
                """,
                data, page_size=page_size)
        logger.debug(f'Postgres.{self.dbname}.{table} nInserted: {cursor.rowcount}')
        return cursor.rowcount

    def upsert_many_candles(
            self,
            symbol_id: int,
            timeframe_id: int,
            candles: List[Dict[str, Any]],
    ) -> int:
        data = [(
            symbol_id * 10 + timeframe_id + candle['ctm'],
            symbol_id,
            timeframe_id,
            candle['ctm'],
            candle['ctmString'],
            candle['open'],
            candle['close'],
            candle['high'],
            candle['low'],
            candle['vol'],
        ) for candle in candles]
        table = 'candles'
        return self.upsert_many(table=table, data=data)
