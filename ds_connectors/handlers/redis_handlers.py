import pandas as pd
from aistac.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract, AbstractPersistHandler, \
    HandlerFactory

__author__ = 'Johan Gielstra'


class RedisSourceHandler(AbstractSourceHandler):
    """ A mongoDB source handler"""

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Hander passing the source_contract dictionary """
        # required module import
        self.redis = HandlerFactory.get_module('redis')
        super().__init__(connector_contract)
        self._modified = 0

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['redis']

    def exists(self) -> bool:
        pass

    def load_canonical(self, **kwargs) -> dict:
        """ returns the canonical dataset based on the source contract
            The canonical in this instance is a dictionary that has the headers as the key and then
            the ordered list of values for that header
        """
        conn = None
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Connector Contract is not valid")
        # this supports redis hmap only...
        cc_params = self.connector_contract.kwargs
        cc_params.update(kwargs)     # Update with any passed though the call

        match = cc_params.get('match', '*')
        count = cc_params.get('count', 1000)
        keys = cc_params.get('keys')
        if not keys or len(keys) == 0:
            raise ValueError("RedisConnector requires an array of 'keys'")
        try:
            conn = self.redis.from_url(self.connector_contract.uri)
            rtn_dict = {'id': []}
            for rowkey in conn.scan_iter(match, count):
                """
                {
                    "col1" = [1,2,3],
                    "col2" = ["a","b","c"]
                }
                """
                rowdata = conn.hgetall(rowkey)
                rtn_dict.get('id').append(rowkey.decode())
                for colkey in keys:
                    if colkey not in rtn_dict:
                        rtn_dict[colkey] = []
                    raw_col_val = rowdata.get(str.encode(colkey))
                    if raw_col_val:
                        col_val = raw_col_val.decode()
                    else:
                        col_val = None
                    rtn_dict.get(colkey).append(col_val)
            conn.close()
            return rtn_dict
        except Exception as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def get_modified(self) -> [int, float, str]:
        return self._modified


class RedisPersistHandler(RedisSourceHandler, AbstractPersistHandler):
    def persist_canonical(self, canonical: pd.DataFrame, **kwargs) -> bool:
        """ persists the canonical dataset """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        return self.backup_canonical(canonical=canonical, uri=self.connector_contract.uri, **kwargs)

    def remove_canonical(self) -> bool:
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract
        raise NotImplementedError("remove_canonical for RedisPersistHandler not yet implemented.")

    def backup_canonical(self, canonical: pd.DataFrame, uri: str, **kwargs) -> bool:
        """ creates a backup of the canonical to an alternative URI  """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract
        persist_params = kwargs if isinstance(kwargs, dict) else _cc.kwargs
        persist_params.update(_cc.parse_query(uri=uri))
        hashprefix = persist_params.get('prefix')
        if hashprefix is None:
            raise ValueError("RedisPersistHandler requires a `prefix` to be provided")
        # use `ifFieldName` to upsert records otherwise assume id is the index of the record in the data frame
        id_field_name = persist_params.get('idFieldName')
        use_index_for_id = False
        to_dict_kwargs = {"orient": "records"}
        if id_field_name is None:
            use_index_for_id = True
        if hashprefix is None:
            raise ValueError("RedisPersistHandler requires a `prefix` to be provided")
        conn = self.redis.from_url(self.connector_contract.uri)
        # TODO if persist_params.get("ordered") is None:
        # ` ordered=persist_params.get("ordered")` is this needed ?
        count = 0
        records = canonical.to_dict(**to_dict_kwargs)
        for rec in records:
            if use_index_for_id:
                idx = count
            else:
                idx = rec[id_field_name]
            conn.hmset(f'{hashprefix}.{idx}', rec)
            count = count+1
        return count == canonical.shape[0]
