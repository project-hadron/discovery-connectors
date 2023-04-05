import traceback

from aistac.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract, HandlerFactory
import pandas as pd
import re
import pymysql.cursors

__author__ = 'Johan Gielstra'


class MySQLSourceHandler(AbstractSourceHandler):
    """ A MySQL Source Handler"""

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Handler passing the source_contract dictionary """
        # required module import
        self.pymysql = HandlerFactory.get_module('pymysql')
        super().__init__(connector_contract)
        self._host, self._port, self._database, self._user, self._password = self.__get_connector_details()
        self._file_state = 0
        self._changed_flag = True

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['jdbc', 'mysql']

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

        try:
            conn = self.pymysql.connect(host=self._host,
                                        user=self._user,
                                        password=self._password,
                                        database=self._database,
                                        port=int(self._port), cursorclass=pymysql.cursors.DictCursor)
            # TODO nested modules(pymysql.cursors) are not being loaded using HandlerFactory.get_module('pymysql'),
            #  had to import manually in imports

            # gets query from uri if exists
            query=""
            if "query" in self.connector_contract.query:
                query = self.connector_contract.query['query']

            # check query is passed explicitly other than in uri
            if self.connector_contract.kwargs.get('query'):
                query = self.connector_contract.kwargs.get('query')

            if not query:
                raise ValueError("Query is not specified in either URI nor Connector Contract")

            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result)
                return df
        except (Exception, self.pymysql.Error) as error:
            print(error)
            # traceback.print_exc()
        finally:
            if conn is not None:
                conn.close()
                # print('Database connection closed.')

    def __get_connector_details(self):
        """
        gets connector details like host, port, username, password, etc from uri
        sample uri: "jdbc:mysql://root:password@localhost:3306/database?query=select* from test"
        """
        connector_type = self.connector_contract.schema
        path = self.connector_contract.path

        if not self.__use_mysql_uri_regex(path):
            raise ValueError("Uri path must match following pattern, "
                             "jdbc:mysql://user:password@host:port/database?query=select* from test")

        rest_of_uri_str = path[path.rindex("@") + 1:]
        credentials_str = path[0:path.rindex("@"):].split("//")[1]

        # gets credentials from uri
        username = credentials_str.split(":")[0]
        password = credentials_str.split(":")[1]

        # gets host details
        host = rest_of_uri_str.split(":")[0]
        port = rest_of_uri_str.split(":")[1].split("/")[0]
        database = rest_of_uri_str.split(":")[1].split("/")[1].split("?")[0]

        if connector_type.lower() not in self.supported_types():
            raise ValueError("The source type '{}' is not supported. see supported_types()".format(connector_type))
        return host, port, database, username, password

    def __use_mysql_uri_regex(self, uri):
        """
        checks uri matches to mysql patten
        """
        pattern = re.compile(r"[A-Za-z]+:.*//[A-Za-z0-9]+:.*@.*:[0-9]+/[A-Za-z0-9]+", re.IGNORECASE)
        return pattern.match(uri)
