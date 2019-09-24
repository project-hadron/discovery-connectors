from pymongo import MongoClient
from ds_foundation.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract

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
