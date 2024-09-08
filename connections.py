from initials import Const, accounts    # , settings
from XTBApi.api import Client as XTB
from XTBApi.api import STATUS
from XTBApi.exceptions import CommandFailed
from classes import Postgres, Mongo
from loguru import logger

# Get account information
# profile = settings[0]
user = "50155431"
account: dict = accounts.get(user, {})
secret = account.get('pass', '')


class BrokerConnection:
    def __init__(self) -> None:
        self.client = XTB()
        try:
            self.client.login(user, secret, mode='real')
        except CommandFailed as e:
            logger.error(f'Gate is blocked, {e}')
            return
        logger.debug('Enter the Gate.')

    def get(self):
        if not self.client.status == STATUS.LOGGED:
            self.__init__()
        return self.client

    def logout(self):
        self.client.logout()
        logger.debug('Close the Gate.')


class DBConnections:
    dbname = {Mongo: Const.MONGODB, Postgres: Const.PGDB}

    def __init__(self) -> None:
        mongodb = Mongo(Const.MONGODB)
        pgdb = Postgres(Const.PGDB)
        self.dbs = [mongodb, pgdb]

    def get_connection(self, DBClass: type[Mongo | Postgres]):
        # find current connection with match DB type, else None
        conn = next((v for i, v in enumerate(self.dbs) if isinstance(v, DBClass)), None)
        if not conn:
            dbname = self.dbname[DBClass]
            conn = DBClass(dbname)
            self.dbs.append(conn)
        return conn

    def get_mongo(self):
        conn = self.get_connection(Mongo)
        logger.debug(f'Connect conn: {conn}')
        return conn

    def get_pg(self):
        conn = self.get_connection(Postgres)
        logger.debug(f'Connect conn: {conn}')
        return conn

    def close_all(self):
        for conn in self.dbs:
            conn.close()
            logger.debug(f'Close conn: {conn}')


class DBConnection:
    def __init__(self, pgdb: str, mongodb: str) -> None:
        self.pgdb = pgdb
        self.mongodb = mongodb
        self.mongo = Mongo(mongodb)
        self.pg = Postgres(pgdb)

    def get_mongo(self):
        if not self.mongo.db:
            self.mongo = Mongo(self.mongodb)
        return self.mongo

    def get_pg(self):
        if not self.pg.db:
            self.pg = Postgres(self.pgdb)
        return self.pg

    def close_all(self):
        self.mongo.client.close()
        self.pg.close()
