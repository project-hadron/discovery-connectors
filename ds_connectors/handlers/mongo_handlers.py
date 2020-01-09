from pymongo import MongoClient
from ds_foundation.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract, AbstractPersistHandler

__author__ = 'Darryl Oatridge, Omar Eid'


class MongoSourceHandler(AbstractSourceHandler):
    """ A mongoDB source handler"""

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Hander passing the source_contract dictionary """
        super().__init__(connector_contract)

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['mongo']

    def load_canonical(self) -> dict:
        """ returns the canonical dataset based on the source contract
            The canonical in this instance is a dictionary that has the headers as the key and then
            the ordered list of values for that header
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The PandasSource Connector Contract has not been set")
        _cc = self.connector_contract
        load_params = _cc.kwargs
        load_params.update(_cc.query)  # Update kwargs with those in the uri query
        return {}

    def exists(self) -> bool:
        """ Returns True is the file exists """

    def get_modified(self) -> [int, float, str]:
        pass

class MongoPersistHandler(MongoSourceHandler, AbstractPersistHandler):
    # a mongoDB persist handler

    def persist_canonical(self, canonical: dict) -> bool:
        """ persists the canonical dataset

        Extra Parameters in the ConnectorContract kwargs:
            - file_type: (optional) the type of the source file. if not set, inferred from the file extension
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _uri = self.connector_contract.uri
        return self.backup_canonical(uri=_uri, canonical=canonical)

    def backup_canonical(self, canonical: dict, uri: str, ignore_kwargs: bool=False) -> bool:
        """ creates a backup of the canonical to an alternative URI  """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract
        _address = _cc.parse_address(uri=uri)
        persist_params = {} if ignore_kwargs else _cc.kwargs
        persist_params.update(_cc.parse_query(uri=uri))

    def remove_canonical(self) -> bool:
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract

