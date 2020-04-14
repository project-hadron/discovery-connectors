from gzip import GzipFile
import io
import yaml
import threading
from contextlib import closing
import json
from typing import Optional
import pandas as pd
from .bundles.managed_content.content import ManagedContentClient
from .bundles.managed_content.cortex_helpers import load_token, load_api_endpoint
from aistac.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract, AbstractPersistHandler

try:
    import cPickel as pickle
except ImportError:
    import pickle


__author__ = 'Omar Eid'


class ManagedContentSourceHandler(AbstractSourceHandler):
    """ A Managed Content Source handler"""

    def __init__(self, connector_contract: ConnectorContract):
        """ Initialise the handler passing the source_contract dictionary """
        super().__init__(connector_contract)
        self.token = self._load_token()
        self.api_endpoint = self._load_api_endpoint()
        self.cortex_mc_client = ManagedContentClient(self.api_endpoint, "2", self.token)

    def mc_key(self, connector_contract: Optional[ConnectorContract]=None):
        _cc = connector_contract if connector_contract is not None else self.connector_contract
        key = _cc.path
        return key[1:] if key.startswith("/") else key

    def _load_token(self):
        return load_token(token=self.connector_contract.kwargs.get("token"))

    def _load_api_endpoint(self):
        return load_api_endpoint(endpoint=self.connector_contract.kwargs.get("api_endpoint"))

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['pickle', "csv"]  # , "json" , "tsv"

    def _download_key_from_mc(self, key):
        return self.cortex_mc_client.download(key, retries=2)

    def _load_dict_from_json_in_mc(self, mc_key: str, load_as_df=None, **json_options) -> pd.DataFrame:
        if not self.exists():
            return pd.DataFrame()
        content = self._download_key_from_mc(mc_key).read()
        data = json.load(io.StringIO(content.decode('utf-8')), **json_options)
        if load_as_df:
            data = pd.DataFrame(data)
        return data

    def _load_dict_from_yaml_in_mc(self, mc_key: str) -> pd.DataFrame:
        if not self.exists():
            return pd.DataFrame()
        content = self._download_key_from_mc(mc_key).read()
        return yaml.safe_load(io.StringIO(content.decode('utf-8')))

    def _load_df_from_csv_in_mc(self, mc_key: str, **pandas_options) -> pd.DataFrame:
        if not self.exists():
            return pd.DataFrame()
        content = self._download_key_from_mc(mc_key).read()
        return pd.read_csv(io.StringIO(content.decode('utf-8')), **pandas_options)

    def _load_df_from_pickle_in_mc(self, mc_key: str, **kwargs) -> pd.DataFrame:
        """ loads a pickle file """
        if not self.exists():
            return pd.DataFrame()
        fix_imports = kwargs.pop('fix_imports', True)
        encoding = kwargs.pop('encoding', 'ASCII')
        errors = kwargs.pop('errors', 'strict')
        with threading.Lock():
            with closing(io.BytesIO(self._download_key_from_mc(mc_key).read())) as f:
                return pickle.load(f, fix_imports=fix_imports, encoding=encoding, errors=errors)

    def _load_gz_from_mc(self, mc_key: str) -> [GzipFile, None]:
        if not self.exists():
            return None
        return GzipFile(None, 'rb', fileobj=self._download_key_from_mc(mc_key))

    def load_canonical(self) -> [pd.DataFrame, dict, GzipFile]:
        """ returns the canonical dataset based on the connector contract. This method utilises the pandas
        'pd.read_' methods and directly passes the kwargs to these methods.
        Extra Parameters in the ConnectorContract kwargs:
            - file_type: (optional) the type of the source file. if not set, inferred from the file extension
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Managed Content Connector Contract has not been set")
        _cc = self.connector_contract
        load_params = _cc.kwargs
        load_params.update(_cc.query)  # Update kwargs with those in the uri query
        _, _, _ext = _cc.address.rpartition('.')
        file_type = load_params.get('file_type', _ext if len(_ext) > 0 else 'csv')
        with threading.Lock():
            if file_type.lower() in ['csv']:
                rtn_data = self._load_df_from_csv_in_mc(mc_key=self.mc_key(), **load_params)
            elif file_type.lower() in ['pkl ', 'pickle']:
                rtn_data = self._load_df_from_pickle_in_mc(mc_key=self.mc_key(), **load_params)
            # elif file_type.lower() in ['tsv']:
            #     rtn_data = self._load_df_from_csv_in_mc(self.mc_key(, delimiter='\t', **load_params)
            elif file_type.lower() in ['json']:
                rtn_data = self._load_dict_from_json_in_mc(self.mc_key(), **load_params)
            elif file_type.lower() in ['yaml']:
                rtn_data = self._load_dict_from_yaml_in_mc(self.mc_key())
            elif file_type.lower() in ["gz"]:
                rtn_data = self._load_gz_from_mc(self.mc_key())
            else:
                raise LookupError('The source format {} is not currently supported'.format(file_type))
        return rtn_data

    def exists(self) -> bool:
        """ returns True if the file in mc exists """
        _cc = self.connector_contract
        mc_key = self.mc_key()
        return self.cortex_mc_client.exists(mc_key)

    def get_modified(self) -> [int, float, str]:
        """ returns the amount of documents in the collection
            ... if the counts change ... then the collection was probably modified ...
            ... this assumes that records are never edited/updated ... nor deleted ...
        """
        # Cortex Does Not Currently send back any meta data regarding the file version when checking if it exists ...
        # https://bitbucket.org/cognitivescale/cortex-connections-service/src/d1d2fdae3fc9db398d7873cac58c2dc7db6fb5ff/lib/controllers/content.js#lines-287
        mc_key = self.mc_key()
        uri = f"content/details/{mc_key}"
        resp = self.cortex_mc_client.serviceconnector.request('GET', uri)
        response = resp.json()
        return (response or {}).get("LastModified", 0)


class ManagedContentPersistHandler(ManagedContentSourceHandler, AbstractPersistHandler):
    # A Managed Content persist handler

    def _persist_df_as_pickle(self, canonical: pd.DataFrame, mc_key: str, **kwargs) -> None:
        """dumps a pickle file"""
        protocol = kwargs.pop('protocol', pickle.HIGHEST_PROTOCOL)
        fix_imports = kwargs.pop('fix_imports', True)
        with threading.Lock():
            # https://stackoverflow.com/questions/13223855/what-is-the-http-content-type-to-use-for-a-blob-of-bytes
            pickle_byte_stream = pickle.dumps(canonical, protocol=protocol, fix_imports=fix_imports)
            self.cortex_mc_client.upload_streaming(mc_key, pickle_byte_stream, "application/python-pickle")

    def _persist_df_as_csv(self, canonical: pd.DataFrame, mc_key: str, **kwargs):
        return self.cortex_mc_client.upload_streaming(mc_key, canonical.to_csv(**kwargs), "text/csv", retries=2)

    def _persist_dict_as_json(self, canonical: dict, mc_key: str):
        return self.cortex_mc_client.upload_streaming(mc_key, json.dumps(canonical), "application/json", retries=2)

    def _persist_dict_as_yaml(self, canonical: dict, mc_key: str):
        return self.cortex_mc_client.upload_streaming(mc_key, yaml.dump(canonical), "application/yaml", retries=2)

    def persist_canonical(self, canonical: pd.DataFrame, **kwargs) -> bool:
        """ persists the canonical dataset
        Extra Parameters in the ConnectorContract kwargs:
            - file_type: (optional) the type of the source file. if not set, inferred from the file extension
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _uri = self.connector_contract.uri
        return self.backup_canonical(uri=_uri, canonical=canonical)

    def backup_canonical(self, canonical: [dict, pd.DataFrame], uri: str, ignore_kwargs: bool = False) -> bool:
        """ creates a backup of the canonical to an alternative URI  """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract
        _uri_cc = ConnectorContract(uri, module_name=_cc.module_name, handler=_cc.handler)
        load_params = _cc.kwargs
        load_params.update(_cc.query)  # Update kwargs with those in the uri query
        mc_key = self.mc_key(_uri_cc)
        _, _, _ext = _uri_cc.address.rpartition('.')
        file_type = load_params.get('file_type', _ext if len(_ext) > 0 else 'csv')
        with threading.Lock():
            if file_type.lower() in ['csv']:
                self._persist_df_as_csv(canonical, mc_key=mc_key, **load_params)
            elif file_type.lower() in ['pkl', 'pickle']:
                self._persist_df_as_pickle(canonical, mc_key=mc_key, **load_params)
            # elif file_type.lower() in ['tsv']:
            #     rtn_data = self._load_df_from_csv_in_mc(mc_key, delimiter='\t', **load_params)
            elif file_type.lower() in ['json']:
                self._persist_dict_as_json(canonical=canonical, mc_key=mc_key)
            elif file_type.lower() in ['yaml']:
                self._persist_dict_as_yaml(canonical=canonical, mc_key=mc_key)
            else:
                raise LookupError('The source format {} is not currently supported'.format(file_type))

        return True

    def remove_canonical(self) -> bool:
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract
        raise NotImplementedError("remove_canonical for ManagedContentPersistHandler not yet implemented.")
