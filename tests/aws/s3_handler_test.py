import unittest
import warnings
from pprint import pprint

from ds_connectors.handlers.aws_s3_handlers import AwsS3SourceHandler, AwsS3PersistHandler
from aistac.handlers.abstract_handlers import ConnectorContract, HandlerFactory


class S3HandlerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_runs(self):
        """Basic smoke test"""
        pass

    def test_aws_connector_init(self):
        handler = AwsS3SourceHandler(connector_contract=ConnectorContract(uri='s3://aistac-discovery-persist/persist/synthetic_member_claims.csv', module_name='', handler=''))
        result = handler.exists()


if __name__ == '__main__':
    unittest.main()
