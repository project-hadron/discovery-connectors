# Developing Mongo Persist Handler
import pandas as pd
from aistac.handlers.abstract_handlers import AbstractSourceHandler, AbstractPersistHandler
from aistac.handlers.abstract_handlers import HandlerFactory, ConnectorContract

__author__ = 'Darryl Oatridge, Omar Eid'


class MongoSourceHandler(AbstractSourceHandler):
    """ A mongoDB source handler"""

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Hander passing the source_contract dictionary """
        # required module import
        self.mongo = HandlerFactory.get_module('pymongo')
        super().__init__(connector_contract)
        database = self.connector_contract.kwargs.get("database")
        self._mongo_database = self.mongo.MongoClient(self.connector_contract.uri)[database]
        self._mongo_collection = self._mongo_database[self.connector_contract.kwargs.get("collection")]
        self._file_state = 0
        self._changed_flag = True

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['mongo']

    def load_canonical(self, **kwargs) -> pd.DataFrame:
        """ returns the canonical dataset based on the source contract
            The canonical in this instance is a dictionary that has the headers as the key and then
            the ordered list of values for that header
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The PandasSource Connector Contract has not been set")

        _cc = self.connector_contract

        load_params = _cc.kwargs
        load_params.update(_cc.query)  # Update kwargs with those in the uri query
        load_params.update(kwargs)     # Update with any passed though the call
        if load_params.get("aggregate") is not None:
            return pd.DataFrame(list(self._mongo_collection.aggregate(_cc.kwargs.get("aggregate", []))))
        elif load_params.get("query") is not None:
            base_query = self._mongo_collection.find(_cc.kwargs.get("find", load_params.get("query", {})),
                                                     _cc.kwargs.get("project"))
            if _cc.kwargs.get("limit") is not None:
                base_query.limit(_cc.kwargs.get("limit"))
            if _cc.kwargs.get("skip") is not None:
                base_query.skip(_cc.kwargs.get("skip"))
            if _cc.kwargs.get("sort") is not None:
                base_query.sort(_cc.kwargs.get("sort"))
            return pd.DataFrame(list(base_query))
        return pd.DataFrame()

    def exists(self) -> bool:
        """ returns True if the collection exists """
        _cc = self.connector_contract
        return _cc.kwargs.get("collection") in self._mongo_database.list_collection_names()

    def has_changed(self) -> bool:
        """ returns the amount of documents in the collection
            ... if the counts change ... then the collection was probably modified ...
            ... this assumes that records are never edited/updated ... nor deleted ...
        """
        _cc = self.connector_contract
        state = self._mongo_collection.count_documents(_cc.kwargs.get("find", {}))
        if state != self._file_state:
            self._changed_flag = True
            self._file_state = state
        return self._changed_flag

    def reset_changed(self, changed: bool = False):
        """ manual reset to say the file has been seen. This is automatically called if the file is loaded"""
        changed = changed if isinstance(changed, bool) else False
        self._changed_flag = changed


class MongoPersistHandler(MongoSourceHandler, AbstractPersistHandler):
    # a mongoDB persist handler

    def persist_canonical(self, canonical: pd.DataFrame, **kwargs) -> bool:
        """ persists the canonical dataset
        Extra Parameters in the ConnectorContract kwargs:
            - file_type: (optional) the type of the source file. if not set, inferred from the file extension
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _uri = self.connector_contract.uri
        return self.backup_canonical(uri=_uri, canonical=canonical, **kwargs)

    def backup_canonical(self, canonical: pd.DataFrame, uri: str, **kwargs) -> bool:
        """ creates a backup of the canonical to an alternative URI  """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract
        _address = _cc.parse_address(uri=uri)
        persist_params = kwargs if isinstance(kwargs, dict) else _cc.kwargs
        persist_params.update(_cc.parse_query(uri=uri))

        if _cc.kwargs.get("ordered") is None:
            resp = self._mongo_collection.insert_many(canonical.to_dict(orient="records"))
        else:
            resp = self._mongo_collection.insert_many(canonical.to_dict(orient="records"),
                                                      ordered=_cc.kwargs.get("ordered"))
        # print(dir(resp))
        return len(resp.inserted_ids) == canonical.shape[0]

    def remove_canonical(self) -> bool:
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract
        raise NotImplementedError("remove_canonical for MongoPersistHandler not yet implemented.")
