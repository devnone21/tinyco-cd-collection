from initials import Const
from connections import BrokerConnection, DBConnections
from datetime import datetime, date, time, timedelta, timezone
from time import sleep
from loguru import logger


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

    def gather_present_candles(self, ct: CandlesTime):
        # get present charts
        bkr_client = self.broker.client
        ts = int(datetime.now(timezone.utc).timestamp())
        res = bkr_client.get_chart_range_request(ct.symbol, ct.timeframe, ts, ts, -100) if bkr_client else {}
        candles = res.get('rateInfos', [])
        logger.info(f'recv {ct.symbol}_{ct.timeframe} {len(candles)} ticks.')
        return candles

    def gather_backdate_candles(self, ct: CandlesTime):
        # get backdate charts
        bkr_client = self.broker.client
        if ct.max_backdate < ct.last_backdate:
            sleep(0.5)
            ts = int(datetime.combine(ct.last_backdate, time(0, 0)).timestamp())
            res = bkr_client.get_chart_range_request(ct.symbol, ct.timeframe, ts, ts, -500) if bkr_client else {}
            candles = res.get('rateInfos', [])
            min_ts = min([int(c['ctm']) for c in candles]) / 1000
            logger.info(f'recv {ct.symbol}_{ct.timeframe} {len(candles)} backdate ticks.')
            return int(min_ts), candles
        return 0, []

    def collect(self, symbol: str, timeframe: int) -> None:
        logger.debug(f'Init CandlesTime ({symbol}, {timeframe})')
        ct = CandlesTime(self.dbs, symbol, timeframe)
        ct.query()

        # gather candles
        candles = self.gather_present_candles(ct)
        min_ts, _backdate_candles = self.gather_backdate_candles(ct)
        candles.extend(_backdate_candles)
        # min_ts, candles = 0, []

        # return if no new candles
        if not candles:
            return

        # store new candles in Postgres
        pgdb = self.dbs.get_pg()
        logger.debug(f'Get conn: {pgdb}')
        symbol_id = Const.SYMBOL_ID.get(symbol)
        timeframe_id = Const.PERIOD_ID.get(timeframe)
        rowcount = pgdb.upsert_many_candles(symbol_id, timeframe_id, candles)

        # store new candles in Mongo
        mongodb = self.dbs.get_mongo()
        logger.debug(f'Get conn: {mongodb}')
        n_inserted = mongodb.insert_list_of_dict(
            collection=f'real_{symbol}_{timeframe}',
            data=[dict(d, **{'_id': d.get('ctm')}) for d in candles]
        )
        # update last backdate
        if n_inserted >= 0 and min_ts > datetime(2020, 7, 1).timestamp():
            ct.last_backdate = date.fromtimestamp(min_ts) + timedelta(days=1)
            ct.update()
        # summary
        logger.info(
            f'Collect {symbol}_{timeframe}: ' +
            f'PG {rowcount} ticks, ' +
            f'Mongo {n_inserted} ticks, ' +
            f'Backdate: {ct.last_backdate.isoformat()}'
        )


def collect() -> None:
    logger.debug(f'Init BrokerConnection()')
    broker = BrokerConnection()
    logger.debug(f'Init DBConnection()')
    dbs = DBConnections()

    # Collect candles
    task = CandlesTask(dbs=dbs, broker=broker)
    for symbol_period in Const.SYMBOL_DEFAULT:
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
