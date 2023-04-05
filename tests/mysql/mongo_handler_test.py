import unittest
import pandas as pd
import os, json
from pprint import pprint

from ds_connectors.handlers.mongo_handlers import MongoSourceHandler
from aistac.handlers.abstract_handlers import ConnectorContract, HandlerFactory


class MongoHandlerTest(unittest.TestCase):

    def setUp(self):
        # This is for temporary
        pass

    def tearDown(self):
        pass

    def test_mongo_connector(self):
        uri = 'mongodb://127.0.0.1:27017/test'
        collection = "gsm-offers"
        database = "test"
        query = {}

        self.connector_contract = ConnectorContract(uri=uri, module_name='ds_connectors.handlers.mongo_handlers',
                                                    handler='MongoSourceHandler',
                                                    collection=collection,
                                                    database=database,
                                                    query=query)

        # set mysql connector to connect
        handler = MongoSourceHandler(connector_contract=self.connector_contract)
        result = handler.load_canonical()
        self.assertTrue(isinstance(result, pd.DataFrame))


if __name__ == '__main__':
    unittest.main()
