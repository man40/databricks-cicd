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
import base64
import json
from os import path as op
from abc import abstractmethod
from collections import OrderedDict
from databricks_cicd.utils import Context, Item, is_different
from databricks_cicd.utils.api import Endpoints
from databricks_cicd.utils.local import Local

_log = logging.getLogger(__name__)


class DeployHelperBase:
    def __init__(self, context: Context):
        self._c = context
        self._remote_items_stale = True
        self._target_path = ''
        self._local_items = None
        self._remote_items = None
        self._ls_local()

    @property
    def local_items(self) -> dict:
        if self._local_items is None:
            self._local_items = self._ls_local()
        return self._local_items

    @local_items.setter
    def local_items(self, value):
        self._local_items = value

    @property
    def remote_items(self) -> dict:
        if self._remote_items is None or self._remote_items_stale is True:
            self._remote_items = self._ls()
            self._remote_items_stale = False
        return self._remote_items

    @remote_items.setter
    def remote_items(self, value):
        self._remote_items = value

    def remote_path(self, path):
        return f'{self._target_path}{path}'

    def common_path(self, path):
        return path[len(self._target_path):]

    @abstractmethod
    def _ls(self, path=None):
        pass

    @abstractmethod
    def _ls_local(self):
        pass

    @abstractmethod
    def _create(self, local_item: Item, path):
        pass

    def _update(self, local_item: Item, remote_item: Item):
        return self._create(local_item, remote_item.path)

    @abstractmethod
    def _delete(self, remote_item: Item):
        pass

    def _mkdirs(self, path):
        pass

    def _get_remote(self, remote_item: Item, overwrite=False):
        pass

    @staticmethod
    def get_local(local_item: Item, overwrite=False):
        if overwrite or local_item.content is None:
            local_item.content = Local.load_binary(local_item.path)

    def _diff(self, local_item: Item, remote_item: Item):
        self.get_local(local_item)
        return is_different(local_item.content, remote_item.content)

    def get_single_item(self, name):
        if self._remote_items_stale:
            return self._ls(name).get(name)
        return self.remote_items.get(name)

    def deploy(self):
        for o in self.local_items:
            remote_path = self.remote_path(o)
            local_item = self.local_items[o]  # type: Item
            remote_item = self.remote_items.get(o)  # type: Item
            if local_item.is_dir:
                if remote_item is None:
                    _log.info('Creating remote %s: %s', local_item.kind, remote_path)
                    self._mkdirs(remote_path)
            elif remote_item is None:
                _log.info('Creating remote %s: %s', local_item.kind, remote_path)
                self._create(local_item, remote_path)
            elif self._diff(local_item, remote_item):
                _log.info('Overwriting remote %s: %s', local_item.kind, remote_path)
                self._update(local_item, remote_item)

        for o in sorted(set(self.remote_items) - set(self.local_items), reverse=True):
            remote_item = self.remote_items[o]
            _log.info('Deleting remote %s: %s', self.remote_items[o].kind, self.remote_path(o))
            self._delete(remote_item)


class WorkspaceHelper(DeployHelperBase):
    def __init__(self, context: Context):
        super().__init__(context)
        self._target_path = f'{context.conf.workspace.target_path}/'.replace('//', '/')

    def _ls(self, path=None):
        if path is None:
            path = self._target_path
        _objects = OrderedDict()
        for obj in self._c.api.call(Endpoints.workspace_list, body={'path': path}).json().get('objects', {}):
            if obj['object_type'] == 'DIRECTORY':
                _objects = dict(_objects, **self._ls(obj['path']))
            _objects[self.common_path(obj['path'])] = Item(
                path=obj['path'],
                kind=obj['object_type'].lower(),
                language=obj.get('language', ''),
                is_dir=obj['object_type'] == 'DIRECTORY')
        return _objects

    def _ls_local(self):
        self.local_items = Local.workspace_ls(op.join(self._c.conf.local_path, self._c.conf.workspace.local_sub_dir))

    def _create(self, local_item: Item, path):
        self._remote_items_stale = True
        self.get_local(local_item)
        return self._c.api.call(Endpoints.workspace_import, body={
            'path': path,
            'language': local_item.language,
            'overwrite': True,
            'content': base64.b64encode(local_item.content).decode("utf-8")})

    def _delete(self, remote_item: Item):
        self._remote_items_stale = True
        return self._c.api.call(Endpoints.workspace_delete, body={'path': remote_item.path})

    def _mkdirs(self, path):
        self._remote_items_stale = True
        return self._c.api.call(Endpoints.workspace_mkdirs, body={'path': path})

    def _get_remote(self, remote_item: Item, overwrite=False):
        if overwrite or remote_item.content is None:
            response = self._c.api.call(Endpoints.workspace_export,
                                        body={'path': remote_item.path, 'format': 'SOURCE'})
            remote_item.content = base64.b64decode(response.json()['content'])

    def _diff(self, local_item: Item, remote_item: Item):
        self.get_local(local_item)
        self._get_remote(remote_item)
        return (local_item.content.decode("utf-8").replace('\r', '').replace('\n', '') !=
                remote_item.content.decode("utf-8").replace('\r', '').replace('\n', ''))


class InstancePoolsHelper(DeployHelperBase):
    def __init__(self, context: Context):
        super().__init__(context)
        self._target_path = context.conf.name_prefix

    def _ls(self, path=None):
        instance_pools = json.loads(self._c.api.call(Endpoints.instance_pools_list, body={}).text)
        return {self.common_path(i['instance_pool_name']): Item(path=i['instance_pool_id'],
                                                                kind='instance pool',
                                                                content=i)
                for i in instance_pools.get('instance_pools', {})
                if i['default_tags']['DatabricksInstancePoolCreatorId'] == self._c.conf.deploying_user_id
                and i['instance_pool_name'].startswith(self._c.conf.name_prefix)}

    def _ls_local(self):
        self.local_items = Local.files_ls(
            op.join(self._c.conf.local_path, self._c.conf.instance_pools.local_sub_dir), ['.json'], 'instance pool')

    def _create(self, local_item: Item, path):
        self._remote_items_stale = True
        self.get_local(local_item)
        return self._c.api.call(Endpoints.instance_pools_create, body=local_item.content)

    def _update(self, local_item: Item, remote_item: Item):
        self._remote_items_stale = True
        self.get_local(local_item)
        local_item.content['instance_pool_id'] = remote_item.path
        return self._c.api.call(Endpoints.instance_pools_edit, body=local_item.content)

    def _delete(self, remote_item: Item):
        self._remote_items_stale = True
        return self._c.api.call(Endpoints.instance_pools_delete, body={'instance_pool_id': remote_item.path})

    def get_local(self, local_item: Item, overwrite=False):
        if overwrite or local_item.content is None:
            local_item.content = Local.load_json(local_item.path)
            for attribute in self._c.conf.instance_pools.strip_attributes:
                local_item.content.pop(attribute, None)

    def _diff(self, local_item: Item, remote_item: Item):
        self.get_local(local_item)
        return is_different(local_item.content, remote_item.content, self._c.conf.instance_pools.ignore_attributes)


class ClustersHelper(DeployHelperBase):
    def __init__(self, context: Context, instance_pools: InstancePoolsHelper):
        super().__init__(context)
        self._target_path = context.conf.name_prefix
        self._instance_pools = instance_pools

    def _ls(self, path=None):
        clusters = json.loads(self._c.api.call(Endpoints.clusters_list, body={}).text)
        return {self.common_path(i['cluster_name']): Item(path=i['cluster_id'],
                                                          kind='cluster',
                                                          content=i)
                for i in clusters.get('clusters', {})
                if i['creator_user_name'] == self._c.conf.deploying_user_name
                and i['cluster_name'].startswith(self._c.conf.name_prefix)}

    def _ls_local(self):
        self.local_items = Local.files_ls(
            op.join(self._c.conf.local_path, self._c.conf.clusters.local_sub_dir), ['.json'], 'cluster')

    def _create(self, local_item: Item, path):
        self._remote_items_stale = True
        self.get_local(local_item)
        return self._c.api.call(Endpoints.clusters_create, body=local_item.content)

    def _update(self, local_item: Item, remote_item: Item):
        self._remote_items_stale = True
        self.get_local(local_item)
        local_item.content['cluster_id'] = remote_item.path
        return self._c.api.call(Endpoints.clusters_edit, body=local_item.content)

    def _delete(self, remote_item: Item):
        self._remote_items_stale = True
        return self._c.api.call(Endpoints.clusters_delete, body={'cluster_id': remote_item.path})

    def get_local(self, local_item: Item, overwrite=False):
        if overwrite or local_item.content is None:
            local_item.content = Local.load_json(local_item.path)
            for attribute in self._c.conf.clusters.strip_attributes:
                local_item.content.pop(attribute, None)
            c = local_item.content
            if c.get('instance_pool_name') and self._instance_pools:
                c['instance_pool_id'] = self._instance_pools.get_single_item(
                    self._instance_pools.remote_path(c['instance_pool_name'])).path
                c.pop('instance_pool_name', None)

    def _diff(self, local_item: Item, remote_item: Item):
        self.get_local(local_item)
        ignore_attributes = self._c.conf.clusters.ignore_attributes
        if local_item.content.get('instance_pool_id'):
            ignore_attributes += self._c.conf.clusters.ignore_attributes_with_instance_pool
        return is_different(local_item.content, remote_item.content, ignore_attributes)


class JobsHelper(DeployHelperBase):
    def __init__(self, context: Context, clusters: ClustersHelper):
        super().__init__(context)
        self._target_path = context.conf.name_prefix
        self._clusters = clusters

    def _ls(self, path=None):
        jobs = json.loads(self._c.api.call(Endpoints.jobs_list, body={}).text)
        return {self.common_path(i['settings']['name']): Item(path=i['job_id'],
                                                              kind='job',
                                                              content=i['settings'])
                for i in jobs.get('jobs')
                if i['creator_user_name'] == self._c.conf.deploying_user_name
                and i['settings']['name'].startswith(self._c.conf.name_prefix)}

    def _ls_local(self):
        self.local_items = Local.files_ls(
            op.join(self._c.conf.local_path, self._c.conf.jobs.local_sub_dir), ['.json'], 'job')

    def _create(self, local_item: Item, path):
        self._remote_items_stale = True
        self.get_local(local_item)
        return self._c.api.call(Endpoints.jobs_create, body=local_item.content)

    def _update(self, local_item: Item, remote_item: Item):
        self._remote_items_stale = True
        self.get_local(local_item)
        return self._c.api.call(Endpoints.jobs_reset, body={'job_id': remote_item.path,
                                                            'new_settings': local_item.content})

    def _delete(self, remote_item: Item):
        self._remote_items_stale = True
        return self._c.api.call(Endpoints.jobs_delete, body={'job_id': remote_item.path})

    def get_local(self, local_item: Item, overwrite=False):
        if overwrite or local_item.content is None:
            local_item.content = Local.load_json(local_item.path)
            for attribute in self._c.conf.jobs.strip_attributes:
                local_item.content.pop(attribute, None)
            c = local_item.content
            if c.get('existing_cluster_name') and self._clusters:
                c['existing_cluster_id'] = self._clusters.get_single_item(
                    self._clusters.remote_path(c['existing_cluster_name'])).path
                c.pop('existing_cluster_name', None)


class DBFSHelper(DeployHelperBase):
    def __init__(self, context: Context):
        super().__init__(context)
        self._target_path = f'{context.conf.dbfs.target_path}/'.replace('//', '/')

    def _ls(self, path=None):
        if path is None:
            path = self._target_path
        _objects = OrderedDict()
        for obj in self._c.api.call(Endpoints.dbfs_list, body={'path': path}).json().get('files', {}):
            if obj['is_dir']:
                _objects = dict(_objects, **self._ls(obj['path']))
            _objects[self.common_path(obj['path'])] = Item(
                path=obj['path'],
                kind='dbfs directory' if obj['is_dir'] else 'dbfs file',
                is_dir=obj['is_dir'],
                size=obj['file_size'])
        return _objects

    def _ls_local(self):
        self.local_items = Local.dbfs_ls(op.join(self._c.conf.local_path, self._c.conf.dbfs.local_sub_dir))

    def _create(self, local_item: Item, path):
        self._remote_items_stale = True
        handle = self._c.api.call(Endpoints.dbfs_create, body={'path': path, 'overwrite': True}).json().get('handle')
        file_size = op.getsize(local_item.path)
        block_size = self._c.conf.dbfs.transfer_block_size
        with open(local_item.path, 'rb') as f:
            for position in range(0, file_size, block_size):
                self._c.api.call(
                    Endpoints.dbfs_add_block,
                    body={'handle': handle,
                          'data': base64.b64encode(f.read(self._c.conf.dbfs.transfer_block_size)).decode("utf-8")})
                _log.info('%s bytes trnasferred', min(position + block_size, file_size))
        self._c.api.call(Endpoints.dbfs_close, body={'handle': handle})
        # self.get_local(local_item)
        # return self._c.api.call(Endpoints.dbfs_put, body={
        #     'path': path,
        #     'overwrite': True,
        #     'contents': base64.b64encode(local_item.content).decode("utf-8")})

    def _delete(self, remote_item: Item):
        self._remote_items_stale = True
        return self._c.api.call(Endpoints.dbfs_delete, body={'path': remote_item.path})

    def _mkdirs(self, path):
        self._remote_items_stale = True
        return self._c.api.call(Endpoints.dbfs_mkdirs, body={'path': path})

    def _get_remote(self, remote_item: Item, overwrite=False):
        if overwrite or remote_item.content is None:
            response = self._c.api.call(Endpoints.dbfs_read, body={'path': remote_item.path})
            remote_item.content = base64.b64decode(response.json()['content'])

    def _diff(self, local_item: Item, remote_item: Item):
        return local_item.size != remote_item.size


class UsersHelper(DeployHelperBase):
    def __init__(self, context: Context):
        super().__init__(context)
        self._target_path = context.conf.name_prefix

    def _ls(self, path=None):
        query = f'?filter=userName+eq+{path}' if path else None
        users = json.loads(self._c.api.call(Endpoints.users_list, body={}, query=query).text)
        return {i['userName']: Item(path=i['id'], kind='user', content=i)
                for i in users.get('Resources', {})}

    def _ls_local(self):
        pass  # TODO:

    def _create(self, local_item: Item, path):
        pass  # TODO:

    def _delete(self, remote_item: Item):
        pass  # TODO:
