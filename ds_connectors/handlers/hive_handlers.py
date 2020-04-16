import pandas as pd
from aistac.handlers.abstract_handlers import AbstractSourceHandler, ConnectorContract, HandlerFactory

__author__ = 'Darryl Oatridge, Neil Pasricha'


class HiveSourceHandler(AbstractSourceHandler):
    """ A Hive source handler"""

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Handler passing the source_contract dictionary """
        self.pyhive = HandlerFactory.get_module('pyhive.hive')
        super().__init__(connector_contract)
        self._modified = 0

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['hive']

    def load_canonical(self, **kwargs) -> [dict, pd.DataFrame]:
        """ returns the canonical dataset based on the source contract
            The canonical in this instance is a dictionary that has the headers as the key and then
            the ordered list of values for that header
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Connector Contract is not valid")
        database = self.connector_contract.path
        host = self.connector_contract.hostname
        user = self.connector_contract.username
        password = self.connector_contract.password
        auth = self.connector_contract.get_key_value('auth', '')
        configuration = self.connector_contract.get_key_value('configuration', '')
        kerberos_service_name = self.connector_contract.get_key_value('kerberos_service_name', '')
        thrift_transport = self.connector_contract.get_key_value('thrift_transport', '')
        canonical = self.connector_contract.get_key_value('canonical', 'dict')
        query = self.connector_contract.query
        host_name, port = host.rsplit(sep=':')
        conn = self.pyhive.hive.Connection(host=host_name, port=port, username=user, password=password,
                                           database=database, configuration=configuration, auth=auth,
                                           kerberos_service_name=kerberos_service_name,
                                           thrift_transport=thrift_transport, **kwargs)
        # return a pandas DataFrame
        if canonical.lower().endswith('pandas'):
            return pd.read_sql(query, conn)
        # default return a dictionary
        cursor = conn.cursor()
        cursor.execute(query)

        columns = [i[0] for i in cursor.description]
        rows = cursor.fetchall()
        rtn_dict = {}
        for row in rows:
            for index in range(len(row)):
                if columns[index] not in rtn_dict:
                    rtn_dict[columns[index]] = []
                rtn_dict.get(columns[index]).append(row[index])
        cursor.close()
        return rtn_dict

    def get_modified(self) -> [int, float, str]:
        return self._modified

    def exists(self) -> bool:
        raise NotImplementedError("This fuction is not yet implemented for Hive")

