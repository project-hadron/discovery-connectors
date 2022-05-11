import unittest
import pandas as pd
import os
from pprint import pprint

from ds_connectors.handlers.mc_handlers import McSourceHandler, McPersistHandler
from aistac.handlers.abstract_handlers import ConnectorContract, HandlerFactory


class ManagedContentHandlerTest(unittest.TestCase):

    def setUp(self):
        os.environ["TOKEN"] = "eyJhbGciOiJFZERTQSIsImtpZCI6Im5WalJOdWhPQzc5ZFpPYVMwaGt4U09Bek14Zm1mTWl0SUpLY05fdWQwTGcifQ.eyJzdWIiOiIyNmU5NmU1OC1kYjhjLTQ5NWQtODI3OS1jMjQ1YzNlMjMzMGUiLCJhdWQiOiJjb3J0ZXgiLCJpc3MiOiJjb2duaXRpdmVzY2FsZS5jb20iLCJpYXQiOjE2NTIxMTUwODksImV4cCI6MTY1MjIwMTQ4OX0.WF0vQJ3395wi5_GW2_6kjqfKa8kMl3ujdA-hwHB6SrMgmyHtOoqMBIi-tPWEWwx7AzIIkOcWAFCoF1C4H49uCg"
        os.environ["API_ENDPOINT"] = "https://api.dci-dev.dev-eks.insights.ai"
        os.environ['PROJECT'] = "bptest"

    def tearDown(self):
        del os.environ["TOKEN"]
        del os.environ["API_ENDPOINT"]
        del os.environ['PROJECT']

    def test_mc_connector_init(self):
        """
        required params: 
            - uri : used as the key for managed content
            - api_endpoint: cortex api endpoint
            - token: cortex token
            - project: cortex project
        """
        cc = ConnectorContract(uri='mc://test.csv', module_name='', handler='')
        handler = McPersistHandler(cc)
        df = pd.DataFrame(data = {'a': [1,2,3,4,5]})
        handler.persist_canonical(df)
        result = handler.load_canonical()
        self.assertTrue(handler.has_changed())
        self.assertFalse(handler.has_changed())
        self.assertFalse(handler.has_changed())
        df = pd.DataFrame(data = {'a': [1,2,3,4,5,6]})
        handler.persist_canonical(df)
        self.assertTrue(handler.has_changed())
        self.assertFalse(handler.has_changed())
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertTrue(handler.remove_canonical())

        cc = ConnectorContract(uri='mc://test/test.parquet', module_name='', handler='')
        handler = McPersistHandler(cc)
        df = pd.DataFrame(data = {'a': [1,2,3,4,5,6]})
        handler.persist_canonical(df)
        result = handler.load_canonical()
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertTrue(handler.remove_canonical())

        cc = ConnectorContract(uri='mc://test/test.pickle', module_name='', handler='')
        handler = McPersistHandler(cc)
        df = pd.DataFrame(data = {'a': [1,2,3,4,5,6]})
        handler.persist_canonical(df)
        result = handler.load_canonical()
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertTrue(handler.exists())
        self.assertTrue(handler.has_changed())
        self.assertFalse(handler.has_changed())
        self.assertTrue(handler.remove_canonical())


    
    

if __name__ == '__main__':
    unittest.main()
