import psycopg2
from aistac.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract

__author__ = 'Johan Gielstra'


class PostgresSourceHandler(AbstractSourceHandler):
    """ A Postgres Source Handler"""

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Hander passing the source_contract dictionary """
        super().__init__(connector_contract)
        self._modified = 0

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['postgresql', 'postgres']

    def exists(self) -> bool:
        return True

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
            conn = psycopg2.connect(database=database, host=host, port=port, user=user, password=password)
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
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def get_modified(self) -> [int, float, str]:
        return self._modified
