from aistac.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract, HandlerFactory

__author__ = 'Johan Gielstra'


class PostgresSourceHandler(AbstractSourceHandler):
    """ A Postgres Source Handler"""

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Hander passing the source_contract dictionary """
        # required module import
        self.psycopg2 = HandlerFactory.get_module('psycopg2')
        super().__init__(connector_contract)
        self._file_state = 0
        self._changed_flag = True

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['postgresql', 'postgres']

    def exists(self) -> bool:
        return True

    def has_changed(self) -> bool:
        """ returns if the file has been modified"""
        # TODO: Add in change logic here
        state = None
        if state != self._file_state:
            self._changed_flag = True
            self._file_state = state
        return self._changed_flag

    def reset_changed(self, changed: bool = False):
        """ manual reset to say the file has been seen. This is automatically called if the file is loaded"""
        changed = changed if isinstance(changed, bool) else False
        self._changed_flag = changed

    def load_canonical(self, **kwargs) -> dict:
        """ returns the canonical dataset based on the source contract
            The canonical in this instance is a dictionary that has the headers as the key and then
            the ordered list of values for that header
        """
        conn = None
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Connector Contract is not valid")
        database = self.connector_contract.path[1:]
        host = self.connector_contract.hostname
        connector_type = self.connector_contract.schema
        # jdbc url ?? nicer than individual parameters
        query = self.connector_contract.kwargs.get('query')
        user = self.connector_contract.username
        password = self.connector_contract.password
        port = self.connector_contract.port or '5432'
        if connector_type.lower() not in self.supported_types():
            raise ValueError("The source type '{}' is not supported. see supported_types()".format(connector_type))
        try:
            conn = self.psycopg2.connect(database=database, host=host, port=port, user=user, password=password)
            cur = conn.cursor()
            cur.execute(query)
            colnames = [desc[0] for desc in cur.description]
            rtn_dict = {}
            row = cur.fetchone()  # look into fetchMany versus 1-1
            """
            {
                "col1" = [1,2,3],
                "col2" = ["a","b","c"]
            }
            """
            while row is not None:
                for idx, col in enumerate(colnames):
                    if col not in rtn_dict:
                        rtn_dict[col] = []
                    rtn_dict.get(col).append(row[idx])
                row = cur.fetchone()
            cur.close()
            return rtn_dict
        except (Exception, self.psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
