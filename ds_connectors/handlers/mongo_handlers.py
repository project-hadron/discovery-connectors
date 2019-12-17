from pymongo import MongoClient
from ds_foundation.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract, AbstractPersistHandler

__author__ = 'Darryl Oatridge, Neil Pasricha'


class MongoSourceHandler(AbstractSourceHandler):
    """ A mongoDB source handler"""

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Hander passing the source_contract dictionary """
        super().__init__(connector_contract)
        self._modified = 0

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['mongo']

    def load_canonical(self) -> dict:
        """ returns the canonical dataset based on the source contract
            The canonical in this instance is a dictionary that has the headers as the key and then
            the ordered list of values for that header
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Connector Contract is not valid")
        resource = self.connector_contract.resource
        connector_type = self.connector_contract.connector_type
        location = self.connector_contract.location
        collection = self.connector_contract.kwargs.get('collection')
        filters = self.connector_contract.kwargs.get('filters')
        if connector_type.lower() not in self.supported_types():
            raise ValueError("The source type '{}' is not supported. see supported_types()".format(connector_type))
        client = MongoClient(location)
        db = client[resource]
        stream = db[collection]
        cursor = stream.find(filters)
        rtn_dict = {}
        for line in list(cursor):
            for k, v in line.items():
                if k not in rtn_dict:
                    rtn_dict[k] = []
                rtn_dict.get(k).append(v)
        return rtn_dict

    def get_modified(self) -> [int, float, str]:
        return self._modified


class MongoPersistHandler(AbstractPersistHandler):
    # a mongoDB persist handler

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Hander passing the connector_contract dictionary """
        super().__init__(connector_contract)
        self._modified = 0

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['mongo']

    def get_modified(self) -> [int, float, str]:
        """ returns if the file has been modified"""
        # TODO:
        raise NotImplemented("This is not yet implemented")

    def exists(self) -> bool:
        """ Returns True is the file exists """
        # TODO:
        raise NotImplemented("This is not yet implemented")

    def load_canonical(self) -> dict:
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Connector Contract is not valid")
        resource = self.connector_contract.resource
        connector_type = self.connector_contract.connector_type
        location = self.connector_contract.location
        collection = self.connector_contract.kwargs.get('collection')
        filters = self.connector_contract.kwargs.get('filters')
        if connector_type.lower() not in self.supported_types():
            raise ValueError("The source type '{}' is not supported. see supported_types()".format(connector_type))
        client = MongoClient(location)
        db = client[resource]
        stream = db[collection]
        cursor = stream.find(filters)
        rtn_dict = {}
        for line in list(cursor):
            for k, v in line.items():
                if k not in rtn_dict:
                    rtn_dict[k] = []
                rtn_dict.get(k).append(v)
        return rtn_dict

    def persist_canonical(self, canonical: dict) -> bool:
        """ persists either the canonical dataset"""
        resource = self.connector_contract.resource
        connector_type = self.connector_contract.connector_type
        location = self.connector_contract.location
        collection = self.connector_contract.kwargs.get('collection')
        if connector_type.lower() not in self.supported_types():
            raise ValueError("The source type '{}' is not supported. see supported_types()".format(connector_type))
        client = MongoClient(location)
        db = client[resource]
        stream = db[collection]
        # TODO:
        raise NotImplemented("This is not yet implemented")

    def remove_canonical(self) -> bool:
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Connector Contract has not been set correctly")
        resource = self.connector_contract.resource
        connector_type = self.connector_contract.connector_type
        location = self.connector_contract.location
        collection = self.connector_contract.kwargs.get('collection')
        # TODO:
        raise NotImplemented("This is not yet implemented")

    def backup_canonical(self, max_backups=None):
        """ creates a backup of the current source contract resource"""
        if not isinstance(self.connector_contract, ConnectorContract):
            return
        max_backups = max_backups if isinstance(max_backups, int) else 10
        resource = self.connector_contract.resource
        location = self.connector_contract.location
        # TODO:
        raise NotImplemented("This is not yet implemented")
