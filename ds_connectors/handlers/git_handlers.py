import os
import shutil
from typing import Any
from aistac.handlers.abstract_handlers import AbstractSourceHandler, AbstractPersistHandler, ConnectorContract, \
    HandlerFactory
from prompt_toolkit.shortcuts.progress_bar import Progress

__author__ = 'Darryl Oatridge'


class GitSourceHandler(AbstractSourceHandler):

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Handler passing the connector_contract dictionary """
        self.git = HandlerFactory.get_module('git')
        cc_params = connector_contract.kwargs
        cc_params.update(connector_contract.query)  # Update kwargs with those in the uri query
        branch_name = cc_params.pop('branch_name', 'master')
        local_repo = cc_params.pop('repo_dir', './git_repo')
        try:
            self.repo = self.git.Repo(local_repo)
        except self.git.NoSuchPathError as err:
            try:
                if os.path.exists(local_repo) and os.path.isdir(local_repo):
                    shutil.rmtree(local_repo)
                self.repo = self.git.Repo.clone_from(url=connector_contract.uri, to_path=local_repo,
                                                     branch=branch_name, depth=1)
            except self.git.GitCommandError as err:
                raise ConnectionError(err)
        super().__init__(connector_contract)

    def supported_types(self) -> list:
        """ The source types supported with this module"""
        return ['parquet', 'csv', 'json', 'pickle', 'yaml']

    def exists(self) -> bool:
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Pandas Connector Contract has not been set")
        _cc = self.connector_contract

    def get_modified(self) -> [int, float, str]:
        """ returns the modified state of the connector resource"""
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Pandas Connector Contract has not been set")
        _cc = self.connector_contract

    def load_canonical(self, **kwargs) -> Any:
        """ returns the canonical dataset based on the connector contract.

        Extra Parameters in the ConnectorContract kwargs:
            - file_type: (optional) the type of the source file. if not set, inferred from the file extension
            - read_params: (optional) value pair dict of parameters to pass to the read methods. Underlying
                           read methods the parameters are passed to are all pandas 'read_*', e.g. pd.read_csv
        """
        if not isinstance(self.connector_contract, ConnectorContract):
            raise ValueError("The Pandas Connector Contract has not been set")
        _cc = self.connector_contract
        load_params = _cc.kwargs
        load_params.update(_cc.query)  # Update kwargs with those in the uri query
        load_params.update(kwargs)  # Update with any passed though the call
        _, _, _ext = _cc.address.rpartition('.')
        file_type = load_params.pop('file_type', _ext if len(_ext) > 0 else 'csv')
        self.repo.config

    @staticmethod
    def example_get_remote(self):
        import os
        import git
        import shutil
        import tempfile

        # Create temporary dir
        t = tempfile.mkdtemp()
        # Clone into temporary dir
        git.Repo.clone_from('https://github.com/Gigas64/discovery-skeleton.git', t, branch='master', depth=1)
        # Copy desired file from temporary dir
        shutil.move(os.path.join(t, 'setup.py'), '.')
        # Remove temporary dir
        shutil.rmtree(t)


class GitPersistHandler(GitSourceHandler, AbstractPersistHandler):

    def persist_canonical(self, canonical: Any, **kwargs) -> bool:
        pass

    def remove_canonical(self, **kwargs) -> bool:
        pass

    def backup_canonical(self, canonical: Any, uri: str, **kwargs) -> bool:
        pass

