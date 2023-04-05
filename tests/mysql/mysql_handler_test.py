import unittest
import pandas as pd
import os
from pprint import pprint

from ds_connectors.handlers.mysql_handlers import MySQLSourceHandler
from aistac.handlers.abstract_handlers import ConnectorContract, HandlerFactory


class MySQLHandlerTest(unittest.TestCase):

    def setUp(self):
        # This is for temporary
        pass

    def tearDown(self):
        pass

    def test_mysql_connector_with_query_in_uri(self):
        uri = 'jdbc:mysql://root:admin123@localhost:3306/test?query=select * from names where id="1"'

        self.connector_contract = ConnectorContract(uri=uri, module_name='ds_connectors.handlers.mysql_handlers',
                                                    handler='MySQLSourceHandler')
        handler = MySQLSourceHandler(connector_contract=self.connector_contract)
        result = handler.load_canonical()
        self.assertTrue(isinstance(result, pd.DataFrame))

    def test_mysql_connector_with_query_argument(self):
        uri = 'jdbc:mysql://root:admin123@localhost:3306/test?query=select * from names'
        query = "select * from names"

        self.connector_contract = ConnectorContract(uri=uri, module_name='ds_connectors.handlers.mysql_handlers',
                                                    handler='MySQLSourceHandler', query=query)
        handler = MySQLSourceHandler(connector_contract=self.connector_contract)
        result = handler.load_canonical()
        self.assertTrue(isinstance(result, pd.DataFrame))

    def test_mysql_connector_with_query_false(self):
        uri = 'jdbc:mysql://root:admin123@localhost:3306/test'
        query = "select * from names"

        self.connector_contract = ConnectorContract(uri=uri, module_name='ds_connectors.handlers.mysql_handlers',
                                                    handler='MySQLSourceHandler')
        handler = MySQLSourceHandler(connector_contract=self.connector_contract)
        try:
            result = handler.load_canonical()
        except Exception as e:
            print(e)


if __name__ == '__main__':
    unittest.main()
