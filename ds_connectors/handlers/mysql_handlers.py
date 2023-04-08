from aistac.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract
from aistac.handlers.abstract_handlers import HandlerFactory, AbstractPersistHandler
import pandas as pd

__author__ = 'Darryl and Sekhar'


class MysqlSourceHandler(AbstractSourceHandler):
    """ This handler class uses both SQLAlchemy and pymysql. Together, SQLAlchemy and pymysql provide a powerful
    toolset for working with databases and faster connectivity. SQLAlchemy allows developers to interact with MySQL
    using Python code, while MySQL provides the database functionality needed to store and retrieve data efficiently.

        URI example
            uri = "mysql://name:password@host:port/database?param2=param1&param2=param2"

        params:
            query: (optional) a source query added as a parameter or kward
            table: (optional) the sql table assuming full dataframe persist. By default 'hadron_default' is used
            if_exists: (optional) How to behave if the table already exists.‘fail’, ‘replace’, ‘append’. Default replace

    """

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Handler passing the Connector Contract """
        # required module import
        self.pymysql = HandlerFactory.get_module('pymysql')
        self.sqlalchemy = HandlerFactory.get_module('sqlalchemy')
        self.sqlalchemy_schema = HandlerFactory.get_module('sqlalchemy.schema')
        super().__init__(connector_contract)
        # reset to use dialect
        _kwargs = {**self.connector_contract.kwargs, **self.connector_contract.query}
        self._sql_query = _kwargs.pop('query', "")
        self._sql_table = _kwargs.pop('table', "hadron_default")
        self._if_exists = _kwargs.pop('if_exists', 'replace')
        # add dialect
        address = self.connector_contract.address.replace('mysql:', 'mysql+pymysql:')
        self._engine = self.sqlalchemy.create_engine(address, **_kwargs)
        self._update_time = pd.Timestamp("1970-01-01")
        self._changed_flag = True

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['mysql']

    def exists(self) -> bool:
        """If the table exists"""
        try:
            meta = self.sqlalchemy_schema.MetaData()
            meta.reflect(bind=self._engine)
            if self._sql_table in meta.tables.keys():
                return True
            return False
        except self.pymysql.Error:
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
            connect = self._engine.connect()
            query = self._sql_query if len(self._sql_query) > 0 else f"SELECT * FROM {self._sql_table}"
            rtn_df = pd.read_sql(query, con=connect, **kwargs)
            connect.close()
            return rtn_df
        except self.pymysql.Error as error:
            raise ConnectionError(f"Failed to load the canonical to MySQL because {error}")


class MysqlPersistHandler(MysqlSourceHandler, AbstractPersistHandler):
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
            connect = self._engine.connect()
            canonical.to_sql(con=connect, name=table, if_exists=_if_exists, index=False, **_params)
            connect.close()
            return True
        except self.pymysql.Error as error:
            raise ConnectionError(f"Failed to save the canonical to MySQL because {error}")

    def remove_canonical(self) -> bool:
        """removes the table and content"""
        if not isinstance(self.connector_contract, ConnectorContract):
            return False
        _cc = self.connector_contract
        try:
            query = f"DROP TABLE IF EXISTS {self._sql_table};"
            with self._engine.connect() as con:
                con.execute(query)
        except self.pymysql.Error:
            return False
        return True
