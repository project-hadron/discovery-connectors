import unittest
import pandas as pd
import os
from pprint import pprint

from ds_connectors.handlers.managed_content_handlers import ManagedContentSourceHandler, ManagedContentPersistHandler
from aistac.handlers.abstract_handlers import ConnectorContract, HandlerFactory


class ManagedContentHandlerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_mc_connector_init(self):
        """
        required params: 
            - uri : used as the key for managed content
            - api_endpoint: cortex api endpoint
            - token: cortex token
            - project: cortex project
        """
        cc = ConnectorContract(uri='test.csv', module_name='', handler='', api_endpoint='', token='', project='')
        handler = ManagedContentPersistHandler(cc)
        df = pd.DataFrame(data = {'a': [1,2,3,4,5]})
        handler.persist_canonical(df)
        result = handler.load_canonical()
        self.assertTrue(isinstance(result, pd.DataFrame))

        cc = ConnectorContract(uri='test/test.parquet', module_name='', handler='', api_endpoint='', token='', project='')
        handler = ManagedContentPersistHandler(cc)
        df = pd.DataFrame(data = {'a': [1,2,3,4,5]})
        handler.persist_canonical(df)
        result = handler.load_canonical()
        self.assertTrue(isinstance(result, pd.DataFrame))

        cc = ConnectorContract(uri='test/test.pickle', module_name='', handler='', api_endpoint='', token='', project='')
        handler = ManagedContentPersistHandler(cc)
        df = pd.DataFrame(data = {'a': [1,2,3,4,5]})
        handler.persist_canonical(df)
        result = handler.load_canonical()
        self.assertTrue(isinstance(result, pd.DataFrame))


if __name__ == '__main__':
    unittest.main()
