import os
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from loguru import logger
from decorators import timer


class Mongo:
    """class of Mongo DB client"""
    def __init__(self, dbname: str) -> None:
        self.client: MongoClient = MongoClient("mongodb://%s:%s@%s" % (
                os.getenv("MONGODB_USER"),
                os.getenv("MONGODB_PASS"),
                os.getenv("MONGODB_HOST"),
            )
        )
        self.dbname = dbname
        self.db = self.client[dbname]

    def close(self) -> None:
        if isinstance(self.client, MongoClient):
            self.client.close()

    @timer
    def find_all(self, collection: str):
        try:
            db_collection = self.db[collection]
            with db_collection.find() as cursor:
                res = [doc for doc in cursor]
                if res:
                    logger.debug(f'Mongo.{self.dbname}.{collection} found')
            return res
        except TypeError as err:
            logger.error(err)
            return []

    def upsert_one(self, collection: str, match: dict, data: dict):
        n_upsert = -1
        try:
            db_collection = self.db[collection]
            res = db_collection.update_one(
                filter=match, update={'$set': data},
                upsert=True
            )
            n_upsert = res.modified_count
            logger.debug(f'Mongo.{self.dbname}.{collection} upsert: {match}')
        except AttributeError as err:
            logger.error(err)
        finally:
            return n_upsert

    @timer
    def insert_list_of_dict(self, collection: str, data: list):
        n_inserted = -1
        try:
            db_collection = self.db[collection]
            res = db_collection.insert_many(data, ordered=False)
            n_inserted = len(res.inserted_ids)
            logger.debug(f'Mongo.{self.dbname}.{collection} nInserted: {n_inserted}')
        except BulkWriteError as err:
            n_errors = len(err.details.get('writeErrors'))
            n_inserted = int(err.details.get('nInserted'))
            logger.debug(f'Mongo.{self.dbname}.{collection} nInserted: {n_inserted}, writeErrors: {n_errors}')
        except AttributeError as err:
            logger.error(err)
        finally:
            return n_inserted
