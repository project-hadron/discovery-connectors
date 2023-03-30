import unittest
import pandas as pd
import os
from pprint import pprint

from ds_connectors.handlers.mysql_handlers import MySQLSourceHandler
from aistac.handlers.abstract_handlers import ConnectorContract, HandlerFactory


class MySQLHandlerTest(unittest.TestCase):

    def setUp(self):
        # This is for temporary
        uri = "jdbc:mysql://root:admin123@localhost:3306/test?test=test"
        query = "select * from names"

        self.connector_contract = ConnectorContract(uri=uri, query=query,
                                                    module_name='ds_connectors.handlers.mysql_handlers',
                                                    handler='MySQLSourceHandler')

    def tearDown(self):
        pass

    def test_mysql_connector_init(self):
        handler = MySQLSourceHandler(connector_contract=self.connector_contract)
        result = handler.load_canonical()
        print(result)
        self.assertTrue(isinstance(result, pd.DataFrame))


if __name__ == '__main__':
    unittest.main()
