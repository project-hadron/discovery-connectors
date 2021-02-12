import unittest
import pandas as pd
import os
from pprint import pprint

from ds_connectors.handlers.s3_handlers import S3SourceHandler, S3PersistHandler
from aistac.handlers.abstract_handlers import ConnectorContract, HandlerFactory


class S3HandlerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_aws_connector_init(self):
        handler = S3PersistHandler(connector_contract=ConnectorContract(uri='s3://project-hadron-cs-repo/factory/healthcare/members', module_name='', handler=''))
        data = {'a': [1,2,3,4,5]}
        handler.persist_canonical(data)
        result = handler.load_canonical()
        self.assertTrue(isinstance(result, dict))
        result = handler.load_canonical(read_params={'as_dataframe': True})
        self.assertTrue(isinstance(result, pd.DataFrame))


if __name__ == '__main__':
    unittest.main()
