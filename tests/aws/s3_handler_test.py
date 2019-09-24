import unittest
import warnings
from pprint import pprint

from ds_foundation.handlers.abstract_handlers import ConnectorContract, HandlerFactory
from ds_foundation.managers.data_properties import DataPropertyManager


class S3HandlerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_runs(self):
        """Basic smoke test"""
        pass

    def test_source_modified_and_list(self):
        connector_contract = ConnectorContract(resource='data/synthetic/synthetic_customer.csv', connector_type='dsv',
                                               location='discovery-connector-scratchpad',
                                               module_name='ds_connectors.handlers.aws_s3_handlers',
                                               handler='S3SourceHandler')
        handler = HandlerFactory.instantiate(connector_contract)
        result = handler.list_bucket(connector_contract.location, starts_with='data/synthetic/sy')
        control = ['data/synthetic/synthetic_agent.csv', 'data/synthetic/synthetic_customer.csv']
        self.assertEqual(control, result)
        result = handler.list_bucket(connector_contract.location, ends_with='er.csv')
        control = ['data/synthetic/synthetic_customer.csv']
        self.assertEqual(control, result)
        result = handler.get_modified()
        control = '2019-08-16 12:18:24+00:00'
        self.assertEqual(control, str(result))

    def test_load_dsv(self):
        connector_contract = ConnectorContract(resource='data/synthetic/synthetic_customer.csv', connector_type='dsv',
                                               location='ds-discovery',
                                               module_name='ds_connectors.handlers.aws_s3_handlers',
                                               handler='S3SourceHandler',
                                               kwargs={'delimiter': ',', 'encoding': 'latin1'})
        handler = HandlerFactory.instantiate(connector_contract)
        result = handler.load_canonical()
        control = ['surname', 'forename', 'gender', 'id', 'balance', 'age', 'start', 'profession', 'online', 'single num', 'weight_num', 'null', 'single cat', 'weight_cat', 'last_login', 'status']
        self.assertEqual(control, list(result))

    def test_load_pickle(self):
        def test_load_dsv(self):
            connector_contract = ConnectorContract(resource='data/synthetic/synthetic_customer.csv',
                                                   connector_type='dsv',
                                                   location='discovery-connector-scratchpad',
                                                   module_name='ds_connectors.handlers.aws_s3_handlers',
                                                   handler='S3SourceHandler',
                                                   kwargs={'delimiter': ',', 'encoding': 'latin1'})
            handler = HandlerFactory.instantiate(connector_contract)
            result = handler.load_canonical()
            control = ['surname', 'forename', 'gender', 'id', 'balance', 'age', 'start', 'profession', 'online',
                       'single num', 'weight_num', 'null', 'single cat', 'weight_cat', 'last_login', 'status']
            self.assertEqual(control, list(result))


if __name__ == '__main__':
    unittest.main()
