from gzip import GzipFile
import io
import os
import yaml
import threading
from contextlib import closing
import json
from typing import Optional, List, Union
import pandas as pd
from .cortex_helpers import load_token, load_api_endpoint
from aistac.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract, AbstractPersistHandler, HandlerFactory

try:
    import cPickel as pickle
except ImportError:
    import pickle


__author__ = 'Bikash Pandey'


class McSourceHandler(AbstractSourceHandler):
    """ A Managed Content Source handler"""

    def __init__(self, connector_contract: ConnectorContract):
        """ Initialise the handler passing the source_contract dictionary """
        super().__init__(connector_contract)
        self.cortex_content = HandlerFactory.get_module('cortex.content')
        self.token = self._load_token()
        self.api_endpoint = self._load_api_endpoint()
        self.project = self._load_project_name()
        self.cortex_mc_client = self.cortex_content.ManagedContentClient(url=self.api_endpoint, token=self.token)
        self._etag = 0
        self._changed_flag = True

    def mc_key(self, connector_contract: Optional[ConnectorContract]=None):
        _cc = connector_contract if connector_contract is not None else self.connector_contract
        schema, bucket, path = _cc.parse_address_elements(_cc.uri)
        if not path:
            return bucket
        return os.path.join(bucket, path.strip('/'))

    def _load_token(self):
        return load_token(token=self.connector_contract.kwargs.get("token", os.environ["TOKEN"]))

    def _load_api_endpoint(self):
        return load_api_endpoint(endpoint=self.connector_contract.kwargs.get("api_endpoint", os.environ["API_ENDPOINT"]))
    
    def _load_project_name(self):
        return self.connector_contract.kwargs.get("project", os.environ["PROJECT"])

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['pickle', "csv", "parquet", "json"]  # , "json" , "tsv"

    def _download_key_from_mc(self, key):
        return self.cortex_mc_client.download(key, retries=2, project=self.project)

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

    def _load_gz_from_mc(self, mc_key: str) -> Union[GzipFile, None]:
        if not self.exists():
            return None
        return GzipFile(None, 'rb', fileobj=self._download_key_from_mc(mc_key))
    
    def _load_df_from_parquet_in_mc(self, mc_key: str, **pandas_options) -> pd.DataFrame:
        if not self.exists():
            return pd.DataFrame()
        content = self._download_key_from_mc(mc_key).read()
        return pd.read_parquet(io.BytesIO(content), **pandas_options)

    def load_canonical(self) -> Union[pd.DataFrame, dict, GzipFile]:
        """ returns the canonical dataset based on the connector contract. This method utilises the pandas
        'pd.read_' methods and directly passes the kwargs to these methods.
        Extra Parameters in the ConnectorContract kwargs:
            - file_type: (optional) the type of the source file. if not set, inferred from the file extension
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Managed Content Connector Contract has not been set")
        _cc = self.connector_contract
        if not isinstance(_cc, ConnectorContract):
            raise ValueError("The Python Source Connector Contract has not been set correctly")
        load_params = _cc.kwargs
        load_params.update(_cc.query)  # Update kwargs with those in the uri query
        load_params.pop('token', None)
        load_params.pop('api_endpoint', None)
        load_params.pop('project', None)
        _, _, _ext = _cc.address.rpartition('.')
        file_type = load_params.get('file_type', _ext if len(_ext) > 0 else 'csv')
        if file_type.lower() not in self.supported_types():
            raise ValueError("The file type {} is not recognised. "
                             "Set file_type parameter to a recognised source type".format(file_type))

        # session
        if _cc.schema not in ['mc']:
            raise ValueError("The Connector Contract Schema has not been set correctly.")
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
            elif file_type.lower() in ['parquet']:
                rtn_data = self._load_df_from_parquet_in_mc(mc_key=self.mc_key(),
                **load_params)
            else:
                raise LookupError('The source format {} is not currently supported'.format(file_type))
        self.reset_changed()
        return rtn_data

    def exists(self) -> bool:
        """ returns True if the file in mc exists """
        _cc = self.connector_contract
        mc_key = self.mc_key()
        return self.cortex_mc_client.exists(key=mc_key, project=self.project)

    def reset_changed(self, changed: bool = False):
        """ manual reset to say the file has been seen. This is automatically called if the file is loaded"""
        changed = changed if isinstance(changed, bool) else False
        self._changed_flag = changed
    
    def has_changed(self) -> bool:
        """ 
            returns if the file has been modified
            uses etag
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Managed Content Connector Contract has not been set")
        _cc = self.connector_contract
        if not isinstance(_cc, ConnectorContract):
            raise ValueError("The Python Source Connector Contract has not been set correctly")
        load_params = _cc.kwargs
        load_params.update(_cc.query)  # Update kwargs with those in the uri query
        load_params.pop('token', None)
        load_params.pop('api_endpoint', None)
        load_params.pop('project', None)
        res = self._download_key_from_mc(self.mc_key())  
        _etag = res.headers['etag']
        if _etag != self._etag:
            self._changed_flag = True
            self._etag = _etag
        else:
            self._changed_flag = False
        return self._changed_flag

class McPersistHandler(McSourceHandler, AbstractPersistHandler):
    # A Managed Content persist handler

    def _persist_df_as_pickle(self, canonical: pd.DataFrame, mc_key: str, **kwargs) -> None:
        """dumps a pickle file"""
        protocol = kwargs.pop('protocol', pickle.HIGHEST_PROTOCOL)
        fix_imports = kwargs.pop('fix_imports', True)
        with threading.Lock():
            # https://stackoverflow.com/questions/13223855/what-is-the-http-content-type-to-use-for-a-blob-of-bytes
            pickle_byte_stream = pickle.dumps(canonical, protocol=protocol, fix_imports=fix_imports)
            self.cortex_mc_client.upload_streaming(key=mc_key, project=self.project, stream=pickle_byte_stream, content_type="application/python-pickle", retries=2)

    def _persist_df_as_csv(self, canonical: pd.DataFrame, mc_key: str, **kwargs):
        file_name = os.path.basename(mc_key)
        canonical.to_csv(file_name)
        with open(file_name, mode="rb") as f_obj:
            res = self.cortex_mc_client.upload_streaming(key=mc_key, project=self.project, stream=f_obj, content_type="application/octet-stream", retries=2)
        return res
    
    def _persist_df_as_parquet(self, canonical: pd.DataFrame, mc_key: str, **kwargs):
        file_name = os.path.basename(mc_key)
        canonical.to_parquet(file_name)
        with open(file_name, mode="rb") as f_obj:
            res = self.cortex_mc_client.upload_streaming(key=mc_key, project=self.project, stream=f_obj, content_type="application/octet-stream", retries=2)
        return res

    def _persist_dict_as_json(self, canonical: dict, mc_key: str):
        if isinstance(canonical, pd.DataFrame):
            canonical = canonical.to_json()
        res = self.cortex_mc_client.upload_streaming(mc_key, project=self.project, stream=json.dumps(canonical), content_type="application/json", retries=2)
        return res

    def _persist_dict_as_yaml(self, canonical: dict, mc_key: str):
        res = self.cortex_mc_client.upload_streaming(mc_key, yaml.dump(canonical), "application/yaml", retries=2)
        return res

    def persist_canonical(self, canonical: pd.DataFrame, **kwargs) -> bool:
        """ persists the canonical dataset
        Extra Parameters in the ConnectorContract kwargs:
            - file_type: (optional) the type of the source file. if not set, inferred from the file extension
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _uri = self.connector_contract.uri
        return self.backup_canonical(uri=_uri, canonical=canonical)

    def backup_canonical(self, canonical: Union[dict, pd.DataFrame], uri: str, ignore_kwargs: bool = False) -> bool:
        """ creates a backup of the canonical to an alternative URI  """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract
        if not isinstance(_cc, ConnectorContract):
            raise ValueError("The Python Source Connector Contract has not been set correctly")
        schema, bucket, path = _cc.parse_address_elements(uri=uri)
        _, _, _ext = path.rpartition('.')
        load_params = _cc.kwargs
        load_params.update(_cc.query)  # Update kwargs with those in the uri query
        load_params.pop('token', None)
        load_params.pop('api_endpoint', None)
        load_params.pop('project', None)
        mc_key = self.mc_key()
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
            elif file_type.lower() in ['parquet']:
                self._persist_df_as_parquet(canonical=canonical, mc_key=mc_key)
            else:
                raise LookupError('The source format {} is not currently supported'.format(file_type))

        return True

    def remove_canonical(self) -> bool:
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Managed Content Connector Contract has not been set")
        _cc = self.connector_contract
        if not isinstance(_cc, ConnectorContract):
            raise ValueError("The Python Source Connector Contract has not been set correctly")
        load_params = _cc.kwargs
        load_params.update(_cc.query)  # Update kwargs with those in the uri query
        load_params.pop('token', None)
        load_params.pop('api_endpoint', None)
        load_params.pop('project', None)
        self.cortex_mc_client.delete(self.mc_key(), project=self.project)
        if not self.exists():
            return True
        return False
