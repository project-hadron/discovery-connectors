import traceback

from aistac.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract
from aistac.handlers.abstract_handlers import HandlerFactory, AbstractPersistHandler
import pandas as pd
import oracledb
from sqlalchemy import create_engine, MetaData
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import declarative_base
from sqlalchemy.types import Integer, Float

__author__ = 'Darryl and Sekhar'


class OracleSourceHandler(AbstractSourceHandler):
    """ This handler class uses both SQLAlchemy and oracledb. Together, SQLAlchemy and oracledb provide a powerful
    toolset for working with databases and faster connectivity. SQLAlchemy allows developers to interact with MySQL
    using Python code, while MySQL provides the database functionality needed to store and retrieve data efficiently.

        URI example
            uri = "oracle://name:password@host:port/service_name?param2=param1&param2=param2"

        params:
            query: (optional) a source query added as a parameter or kward
            table: (optional) the sql table assuming full dataframe persist. By default 'hadron_default' is used
            if_exists: (optional) How to behave if the table already exists.‘fail’, ‘replace’, ‘append’. Default replace

    """

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Handler passing the Connector Contract """
        # required module import
        # self.oracledb = HandlerFactory.get_module('oracledb')
        self.sqlalchemy = HandlerFactory.get_module('sqlalchemy')
        self.sqlalchemy_schema = HandlerFactory.get_module('sqlalchemy.schema')
        super().__init__(connector_contract)
        # reset to use dialect
        _kwargs = {**self.connector_contract.kwargs, **self.connector_contract.query}
        self._sql_query = _kwargs.pop('query', "")
        self._sql_table = _kwargs.pop('table', "hadron_default")
        self._if_exists = _kwargs.pop('if_exists', 'replace')
        # add dialect
        address = self.connector_contract.address.replace('oracle://', '')
        user = self.connector_contract.username
        pwd = self.connector_contract.password
        port = self.connector_contract.port
        hostname = self.connector_contract.hostname
        service_name = self.connector_contract.path.split("/")[-1]
        pool = oracledb.create_pool(user=user, password=pwd,
                                    host=hostname, port=port, service_name=service_name,
                                    min=1, max=4, increment=1)

        self._engine = create_engine("oracle+oracledb://", creator=pool.acquire, poolclass=NullPool)

        self._update_time = pd.Timestamp("1970-01-01")
        self._changed_flag = True

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['oracle']

    def exists(self) -> bool:
        """If the table exists"""
        try:
            meta = self.sqlalchemy_schema.MetaData()
            meta.reflect(bind=self._engine)
            if self._sql_table in meta.tables.keys():
                return True
            return False
        except oracledb.Error:
            return False

    def has_changed(self) -> bool:
        """ if the table has changed. Only works with certain implementations"""
        # TODO: Add in change logic here
        database = str(self.connector_contract.path[1:])
        table = str(self._sql_table)
        query = f"SELECT UPDATE_TIME " \
                f"FROM information_schema.tables " \
                f"WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{table}';"
        with self._engine.connect() as con:
            result = con.execute(query)
        last_update = pd.to_datetime((next(result))[0])
        if last_update is None or self._update_time < last_update:
            self._changed_flag = True
        else:
            self._changed_flag = False
        return self._changed_flag

    def reset_changed(self, changed: bool = False):
        """ manual reset to say the table has been seen. This is automatically called if the file is loaded"""
        changed = changed if isinstance(changed, bool) else False
        self._changed_flag = changed

    def load_canonical(self, **kwargs) -> dict:
        """ returns the canonical dataset based on the Connector Contract """
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Connector Contract is not valid")
        try:
            with self._engine.connect() as con:
                query = self._sql_query if len(self._sql_query) > 0 else f"SELECT * FROM {self._sql_table}"
                rtn_df = pd.read_sql(query, con=con, **kwargs)
            return rtn_df
        except oracledb.Error as error:
            raise ConnectionError(f"Failed to load the canonical to MySQL because {error}")


class OraclePersistHandler(OracleSourceHandler, AbstractPersistHandler):
    # a MySQL persist handler

    def persist_canonical(self, canonical: pd.DataFrame, **kwargs) -> bool:
        """ persists the canonical dataset"""
        return self.backup_canonical(canonical=canonical, table=self._sql_table, **kwargs)

    def backup_canonical(self, canonical: pd.DataFrame, table: str, **kwargs) -> bool:
        """ creates a backup of the canonical to an alternative table  """
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        try:
            _if_exists = self._if_exists
            _params = kwargs
            _if_exists = _params.pop('if_exists', self._if_exists)
            with self._engine.connect() as con:
                dtypes_dict = canonical.dtypes.apply(lambda x: x.name).to_dict()
                new_types = {}
                for col, type in dtypes_dict.items():
                    if "float64" == type:
                        new_types[col] = Float
                    if "int64" == type:
                        new_types[col] = Integer

                canonical.to_sql(con=con, name=table, if_exists="replace", dtype=new_types, index=False, **_params)
            return True
        except oracledb.Error as error:
            traceback.print_exc()
            raise ConnectionError(f"Failed to save the canonical to Oracle because {error}")

    def remove_canonical(self) -> bool:
        """removes the table and content"""
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract
        try:
            Base = declarative_base()
            metadata = MetaData()
            metadata.reflect(bind=self._engine)
            if self._sql_table in metadata.tables:
                table = metadata.tables[self._sql_table]
                if table is not None:
                    Base.metadata.drop_all(self._engine, [table], checkfirst=True)
        except oracledb.Error:
            return False
        return True
