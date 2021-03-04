# Copyright (C) databricks-cicd 2021 man40 (man40dev@gmail.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from os import path as op
from configparser import ConfigParser
from textwrap import indent

_log = logging.getLogger(__name__)


class ConfBase:
    def __repr__(self):
        return '\n'.join(f'{k}: {self._indent(v)}' for k, v in self.__dict__.items() if not k.startswith('_'))

    @staticmethod
    def _parse_int(value) -> int:
        return eval(value) if isinstance(value, str) else int(value)

    @staticmethod
    def _parse_list(value) -> list:
        return [] if value is None else [v for v in value.split('\n') if v]

    @staticmethod
    def _indent(obj):
        if isinstance(obj, ConfBase):
            return f'\n{indent(str(obj), "  ")}'
        return obj


class Conf(ConfBase):
    def __init__(self, cmd_args: dict, config_file: str):
        default_config_file = op.join(op.dirname(__file__), 'default.ini')

        parser = ConfigParser()
        parser.read(default_config_file)

        override_config_file = config_file
        if override_config_file:
            assert op.isfile(override_config_file), f'Config file was not found in: {override_config_file}'
            parser.read(override_config_file)

        parser.read_dict(cmd_args)

        self._section = 'global'
        self.workspace_host = parser[self._section].get('workspace_host')
        self.deploying_user_name = parser[self._section].get('deploying_user_name')
        self.deploying_user_id = None
        self.local_path = parser[self._section].get('local_path')
        self.dry_run = parser[self._section].getboolean('dry_run')
        self.name_prefix = parser[self._section].get('name_prefix')
        self.deploy_safety_limit = self._parse_int(parser[self._section].get('deploy_safety_limit'))
        self.rate_limit_timeout = self._parse_int(parser[self._section].get('rate_limit_timeout'))
        self.rate_limit_attempts = self._parse_int(parser[self._section].get('rate_limit_attempts'))
        self.workspace = ConfWorkspace(parser)
        self.instance_pools = ConfInstancePools(parser)
        self.clusters = ConfClusters(parser)
        self.jobs = ConfJobs(parser)
        self.dbfs = ConfDBFS(parser)


class ConfWorkspace(ConfBase):
    def __init__(self, parser: ConfigParser):
        self._section = 'workspace'
        self.deploy = parser[self._section].getboolean('deploy')
        self.local_sub_dir = parser[self._section].get('local_sub_dir')
        self.target_path = parser[self._section].get('target_path')
        self.transfer_block_size = self._parse_int(parser[self._section].get('transfer_block_size'))
        assert self.target_path != '/', 'Cannot deploy in the workspace root folder!'


class ConfInstancePools(ConfBase):
    def __init__(self, parser: ConfigParser):
        self._section = 'instance_pools'
        self.deploy = parser[self._section].getboolean('deploy')
        self.local_sub_dir = parser[self._section].get('local_sub_dir')
        self.ignore_attributes = self._parse_list(parser[self._section].get('ignore_attributes'))
        self.strip_attributes = self._parse_list(parser[self._section].get('strip_attributes'))


class ConfClusters(ConfBase):
    def __init__(self, parser: ConfigParser):
        self._section = 'clusters'
        self.deploy = parser[self._section].getboolean('deploy')
        self.local_sub_dir = parser[self._section].get('local_sub_dir')
        self.ignore_attributes = self._parse_list(parser[self._section].get('ignore_attributes'))
        self.ignore_attributes_with_instance_pool = self._parse_list(
            parser[self._section].get('ignore_attributes_with_instance_pool'))
        self.strip_attributes = self._parse_list(parser[self._section].get('strip_attributes'))


class ConfJobs(ConfBase):
    def __init__(self, parser: ConfigParser):
        self._section = 'jobs'
        self.deploy = parser[self._section].getboolean('deploy')
        self.local_sub_dir = parser[self._section].get('local_sub_dir')
        self.strip_attributes = self._parse_list(parser[self._section].get('strip_attributes'))


class ConfDBFS(ConfBase):
    def __init__(self, parser: ConfigParser):
        self._section = 'dbfs'
        self.deploy = parser[self._section].getboolean('deploy')
        self.local_sub_dir = parser[self._section].get('local_sub_dir')
        self.compare_contents = parser[self._section].getboolean('compare_contents')
        self.target_path = parser[self._section].get('target_path')
        self.transfer_block_size = eval(parser[self._section].get('transfer_block_size'))
        assert self.target_path != '/', 'Cannot deploy in the dbfs root folder!'
