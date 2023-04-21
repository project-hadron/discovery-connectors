import unittest
import os
from pathlib import Path
import shutil
import pandas as pd
from aistac.handlers.abstract_handlers import ConnectorContract
from ds_discovery import SyntheticBuilder
from aistac.properties.property_manager import PropertyManager


class MongodbHandlerTest(unittest.TestCase):


    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        # clean out any old environments
        for key in os.environ.keys():
            if key.startswith('HADRON'):
                del os.environ[key]
        # Local Domain Contract
        os.environ['HADRON_PM_PATH'] = os.path.join('working', 'contracts')
        os.environ['HADRON_PM_TYPE'] = 'json'
        # Local Connectivity
        os.environ['HADRON_DEFAULT_PATH'] = Path('working/data').as_posix()
        # Specialist Component
        try:
            os.makedirs(os.environ['HADRON_PM_PATH'])
        except OSError:
            pass
        try:
            os.makedirs(os.environ['HADRON_DEFAULT_PATH'])
        except OSError:
            pass
        PropertyManager._remove_all()

    def tearDown(self):
        try:
            shutil.rmtree('working')
        except OSError:
            pass

    def test_handler_default(self):
        sb = SyntheticBuilder.from_memory()
        df = self.data(size=1_000)
        sb.set_persist_uri("mongodb://localhost:27017/test")
        sb.remove_canonical(sb.CONNECTOR_PERSIST)
        self.assertFalse(sb.pm.get_connector_handler(sb.CONNECTOR_PERSIST).exists())
        sb.save_persist_canonical(df)
        result = sb.load_persist_canonical()
        self.assertTrue(sb.pm.get_connector_handler(sb.CONNECTOR_PERSIST).exists())
        self.assertEqual((1000, 7), result.shape)
        self.assertEqual(['_id', 'cat', 'num', 'int', 'bool', 'date', 'object'], result.columns.to_list())
        sb.remove_canonical(sb.CONNECTOR_PERSIST)

    def test_handler_query(self):
        sb = SyntheticBuilder.from_memory()
        df = self.data(size=1_000)
        os.environ['collection'] = 'hadron_table'
        uri = "mongodb://localhost:27017/test?collection=${collection}&&find={}&&project={'cat':1, 'num':1, '_id':0}"
        sb.set_persist_uri(uri=uri)
        sb.remove_canonical(sb.CONNECTOR_PERSIST)
        sb.save_persist_canonical(df)
        result = sb.load_persist_canonical()
        self.assertEqual((1000, 2), result.shape)
        self.assertEqual(['cat', 'num'], result.columns.to_list())
        sb.remove_canonical(sb.CONNECTOR_PERSIST)

    def test_handler_find_limit(self):
        sb = SyntheticBuilder.from_memory()
        df = self.data(size=1_000)
        os.environ['collection'] = 'hadron_table'
        uri = "mongodb://localhost:27017/test?collection=${collection}&&find={}&&project={'cat':1, 'num':1, '_id':0}&&limit=2&&skip=2&&sort=[('cat', 1)]"
        sb.set_persist_uri(uri=uri)
        sb.remove_canonical(sb.CONNECTOR_PERSIST)
        sb.save_persist_canonical(df)
        result = sb.load_persist_canonical()
        self.assertEqual((2, 2), result.shape)
        self.assertEqual(['cat', 'num'], result.columns.to_list())
        sb.remove_canonical(sb.CONNECTOR_PERSIST)

    def test_handler_aggregate(self):
        sb = SyntheticBuilder.from_memory()
        df = self.data(size=1_000)
        os.environ['collection'] = 'hadron_table'
        uri = "mongodb://localhost:27017/test?collection=${collection}&&aggregate=[{'$count': 'count'}]"
        sb.set_persist_uri(uri=uri)
        sb.remove_canonical(sb.CONNECTOR_PERSIST)
        sb.save_persist_canonical(df)
        result = sb.load_persist_canonical()
        self.assertEqual((1, 1), result.shape)
        self.assertEqual(1000, result['count'].iloc[0])
        sb.remove_canonical(sb.CONNECTOR_PERSIST)

    def test_connector_contract(self):
        os.environ['HADRON_ADDITION'] = 'myAddition'
        os.environ['collection'] = 'hadron_table'
        uri = "mongodb://localhost:27017/test?collection=${collection}&&aggregate=[{'$match':{'cat':'ACTIVE'}}, '$count': 'count']"
        cc = ConnectorContract(uri=uri, module_name='', handler='', addition='${HADRON_ADDITION}')
        print(f"raw_uri = {cc.raw_uri}")
        print(f"uri = {cc.uri}")
        print(f"raw_kwargs = {cc.raw_kwargs}")
        print(f"address = {cc.address}")
        print(f"schema = {cc.schema}")
        print(f"hostname = {cc.hostname}")
        print(f"port = {cc.port}")
        print(f"username = {cc.username}")
        print(f"password = {cc.password}")
        print(f"path = {cc.path}")
        print(f"database = {cc.path[1:]}")
        print(f"query")
        extra = cc.query.pop('extra', None)
        print(f" extra = {extra}")
        find = cc.query.pop('find', None)
        print(f" mongo_find = {find}")
        aggregate = cc.query.pop('aggregate', None)
        print(f" mongo_aggregate = {aggregate}")
        collection = cc.query.pop('collection', None)
        print(f" collection = {collection}")
        print(f"kwargs")
        addition = cc.kwargs.get('addition', None)
        print(f" addition = {addition}")

    def test_raise(self):
        with self.assertRaises(KeyError) as context:
            env = os.environ['NoEnvValueTest']
        self.assertTrue("'NoEnvValueTest'" in str(context.exception))

    def data(self, size: int=10_000, complete: bool=False):
        sb = SyntheticBuilder.from_memory()
        df = sb.tools.frame_starter(canonical=size)
        # types
        df['cat'] = sb.tools.get_category(['SUSPENDED', 'ACTIVE', 'PENDING', 'INACTIVE'], relative_freq=[1, 99, 10, 40], size=size)
        df['num'] = sb.tools.get_number(0.5, 1.5, size=size)
        df['int'] = sb.tools.get_number(-1000, 1000, relative_freq=[1, 2, 3, 5, 7, 11, 7, 2, 1], size=size)
        df['bool'] = sb.tools.get_category([1, 0], relative_freq=[9, 1], size=size)
        df['date'] = sb.tools.get_datetime(start='2022-12-01', until='2023-03-31', date_format='%Y-%m-%d', ordered=True, size=size)
        df['object'] = sb.tools.get_sample('us_professions', size=size)
        if complete:
            # distributions
            df['normal'] = sb.tools.get_dist_normal(mean=0, std=1, size=size)  # normal
            df['bernoulli'] = sb.tools.get_dist_bernoulli(probability=0.2, size=size)  # bool
            df['gumbel'] = sb.tools.get_distribution(distribution='gumbel', loc=0, scale=0.1, size=size)  # skew
            df['poisson'] = sb.tools.get_distribution(distribution='poisson', lam=3, size=size)  # category
            df['poly'] = sb.tools.correlate_polynomial(df, header='num', coefficient=[6, 0, 1])  # curve

            # impute
            df['cat_null'] = sb.tools.get_category(list('MFU'), relative_freq=[9, 7, 1], quantity=0.9, size=size)
            df['num_null'] = sb.tools.get_number(0., 1., quantity=0.98, size=size)
            df['bool_null'] = sb.tools.get_category(['1', '0'], relative_freq=[1, 20], quantity=0.95, size=size)
            df['date_null'] = sb.tools.get_datetime(start='2022-12-01', until='2023-03-31', date_format='%Y-%m-%d', quantity=0.99, size=size)
            df['object_null'] = sb.tools.get_string_pattern('(ddd)sddd-ddd', quantity=0.85, size=size)

            # #compare
            df['unique'] = sb.tools.get_number(from_value=size, to_value=size * 10, at_most=1, size=size)
            df['date_tz'] = sb.tools.get_datetime(pd.Timestamp('2021-09-01', tz='CET'), pd.Timestamp('2022-01-01', tz='CET'), date_format='%Y-%m-%d', size=size)
            df['correlate'] = sb.tools.correlate_values(df, header='poly', jitter=0.05)
            df['outliers'] = sb.tools.correlate_values(df, header='correlate', jitter=1, choice=4)
            df['dup_num'] = sb.tools.correlate_values(df, header='num')
            df['dup_date'] = sb.tools.correlate_dates(df, header='date')

            # # others
            df['single_num'] = sb.tools.get_number(1, 2, size=size)
            df['single_cat'] = sb.tools.get_category(['CURRENT'], size=size)
            df['nulls'] = sb.tools.get_number(20.0, quantity=0, size=size)
            df['nulls_num'] = sb.tools.get_number(20.0, quantity=0.03, size=size)
            df['nulls_cat'] = sb.tools.get_category(list('XYZ'), quantity=0.01, size=size)

        return df


if __name__ == '__main__':
    unittest.main()
