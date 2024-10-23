from initials import Const
from connections import BrokerConnection, DBConnections
from datetime import datetime, date, time, timedelta, timezone
from time import sleep
from base_loggers import logger
logger.service = __name__


class CandlesTime:
    def __init__(self, dbs: DBConnections, symbol: str, timeframe: int) -> None:
        self.dbs = dbs
        self.symbol = symbol
        self.timeframe = timeframe
        self.name = f'real_{symbol}_{timeframe}'
        now = datetime.now(timezone.utc)
        self.max_backdate = now.date() - timedelta(days=12*timeframe)
        self.last_backdate = now.date()
        if timeframe == 30:
            self.max_backdate = max(self.max_backdate, date(2023, 7, 21))

    def query(self):
        mongodb = self.dbs.get_mongo()
        docs = mongodb.find_all('candles_time')
        for doc in docs:
            if doc['candles'] == self.name:
                self.last_backdate = date.fromisoformat(doc['last_backdate'])

    def update(self):
        mongodb = self.dbs.get_mongo()
        doc = {
            '_id': self.name,
            'candles': self.name,
            'last_backdate': self.last_backdate.isoformat()
        }
        mongodb.upsert_one('candles_time', match={'candles': self.name}, data=doc)


class CandlesTask:
    def __init__(self, dbs: DBConnections, broker: BrokerConnection) -> None:
        self.dbs = dbs
        self.broker = broker

    def _get_chart_from_ts(self, ts:int, symbol: str, timeframe: int, tick: int):
        bkr_client = self.broker.client
        res = bkr_client.get_chart_range_request(symbol, timeframe, ts, ts, tick) if bkr_client else {}
        candles = res.get('rateInfos', [])
        # digits = res.get('digits', 0)
        logger.info(f'Got {symbol}_{timeframe} {len(candles)} ticks from {ts}')
        return candles

    def gather_present_candles(self, ct: CandlesTime):
        """get present charts"""
        ts = int(datetime.now(timezone.utc).timestamp())
        return self._get_chart_from_ts(ts, ct.symbol, ct.timeframe, tick=-300)

    def gather_olden_candles(self, ct: CandlesTime):
        """get olden charts"""
        if ct.max_backdate >= ct.last_backdate:
            return []
        sleep(0.5)
        ts = int(datetime.combine(ct.last_backdate, time(0, 0)).timestamp())
        return self._get_chart_from_ts(ts, ct.symbol, ct.timeframe, tick=-500)

    def collect(self, symbol: str, timeframe: int) -> None:
        logger.debug(f'Initialize CandlesTime ({symbol}, {timeframe})')
        ct = CandlesTime(self.dbs, symbol, timeframe)
        ct.query()

        # gather candles
        candles = self.gather_present_candles(ct)
        olden_candles = self.gather_olden_candles(ct)
        candles.extend(olden_candles)

        # return if no new candles
        if not candles:
            return

        # store new candles in Postgres
        pgdb = self.dbs.get_pg()
        logger.debug(f'Using Postgres conn: {pgdb}')
        symbol_id = Const.SYMBOL_ID.get(symbol)
        timeframe_id = Const.PERIOD_ID.get(timeframe)
        rowcount = pgdb.upsert_many_candles(symbol_id, timeframe_id, candles)

        # store new candles in Mongo
        mongodb = self.dbs.get_mongo()
        logger.debug(f'Using Mongo conn: {mongodb}')
        n_inserted = mongodb.insert_list_of_dict(
            collection=f'real_{symbol}_{timeframe}',
            data=[dict(d, **{'_id': d.get('ctm')}) for d in candles]
        )
        # update last backdate
        olden_ts = 0 if not olden_candles else min([int(c['ctm']) for c in olden_candles]) / 1000
        if n_inserted >= 0 and olden_ts > datetime(2020, 7, 1).timestamp():
            ct.last_backdate = date.fromtimestamp(olden_ts) + timedelta(days=1)
            ct.update()

        # summary
        logger.info(
            f'Collect {symbol}_{timeframe}: ' +
            f'PG {rowcount} ticks, ' +
            f'Mongo {n_inserted} ticks, ' +
            f'Backdate: {ct.last_backdate.isoformat()}'
        )


def collect() -> None:
    logger.debug(f'Initialize BrokerConnection()')
    broker = BrokerConnection()
    logger.debug(f'Initialize DBConnection()')
    dbs = DBConnections()

    # Collect candles
    task = CandlesTask(dbs=dbs, broker=broker)
    for symbol_period in Const.SYMBOL_SUBSCRIBE:
        symbol, period = symbol_period
        task.collect(
            symbol=symbol,
            timeframe=period,
        )
        # Delay before next symbol
        sleep(1)

    dbs.close_all()
    broker.logout()


if __name__ == '__main__':
    collect()
