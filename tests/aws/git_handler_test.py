import unittest
import pandas as pd
from pprint import pprint
from aistac.handlers.abstract_handlers import ConnectorContract
from ds_connectors.handlers.git_handlers import GitPersistHandler


class S3HandlerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_aws_connector_init(self):
        uri = 'git://github.com/project-hadron/discovery-asset-bank.git'
        handler = GitPersistHandler(connector_contract=ConnectorContract(uri=uri, module_name='', handler=''))
        data = {'a': [1,2,3,4,5]}
        handler.persist_canonical(data)
        result = handler.load_canonical()
        self.assertTrue(isinstance(result, dict))
        result = handler.load_canonical(read_params={'as_dataframe': True})
        self.assertTrue(isinstance(result, pd.DataFrame))


if __name__ == '__main__':
    unittest.main()