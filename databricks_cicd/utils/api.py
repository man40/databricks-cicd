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
import json
import time
import requests
from databricks_cicd.conf import Conf

NOTEBOOK_LANGUAGES = {'': '', 'PYTHON': '.py', 'SCALA': '.scala', 'SQL': '.sql', 'R': '.r'}
NOTEBOOK_EXTENSIONS = {v: k for k, v in NOTEBOOK_LANGUAGES.items()}

_log = logging.getLogger(__name__)


class Endpoint:
    def __init__(self, method: str, url: str, is_write: bool):
        self.method = method
        self.url = url
        self.is_write = is_write


class Endpoints:
    workspace_list = Endpoint('get', '2.0/workspace/list', False)
    workspace_export = Endpoint('get', '2.0/workspace/export', False)
    workspace_import = Endpoint('post', '2.0/workspace/import', True)
    workspace_delete = Endpoint('post', '2.0/workspace/delete', True)
    workspace_mkdirs = Endpoint('post', '2.0/workspace/mkdirs', True)

    jobs_list = Endpoint('get', '2.0/jobs/list', False)
    jobs_create = Endpoint('post', '2.0/jobs/create', True)
    jobs_reset = Endpoint('post', '2.0/jobs/reset', True)
    jobs_delete = Endpoint('post', '2.0/jobs/delete', True)

    instance_pools_list = Endpoint('get', '2.0/instance-pools/list', False)
    instance_pools_create = Endpoint('post', '2.0/instance-pools/create', True)
    instance_pools_edit = Endpoint('post', '2.0/instance-pools/edit', True)
    instance_pools_delete = Endpoint('post', '2.0/instance-pools/delete', True)

    clusters_list = Endpoint('get', '2.0/clusters/list', False)
    clusters_create = Endpoint('post', '2.0/clusters/create', True)
    clusters_edit = Endpoint('post', '2.0/clusters/edit', True)
    clusters_delete = Endpoint('post', '2.0/clusters/permanent-delete', True)

    dbfs_list = Endpoint('get', '2.0/dbfs/list', False)
    dbfs_read = Endpoint('get', '2.0/dbfs/read', False)
    dbfs_put = Endpoint('post', '2.0/dbfs/put', True)
    dbfs_delete = Endpoint('post', '2.0/dbfs/delete', True)
    dbfs_mkdirs = Endpoint('post', '2.0/dbfs/mkdirs', True)
    dbfs_create = Endpoint('post', '2.0/dbfs/create', False)  # False - to prevent triggering safety limit
    dbfs_close = Endpoint('post', '2.0/dbfs/close', True)
    dbfs_add_block = Endpoint('post', '2.0/dbfs/add-block', False)  # False - to prevent triggering safety limit

    users_list = Endpoint('get', '2.0/preview/scim/v2/Users', False)


class API:
    def __init__(self, conf: Conf, access_token: str):
        self._conf = conf
        self._access_token = access_token
        self._deploy_safety_limit = conf.deploy_safety_limit

    def _call(self, endpoint: Endpoint, body, query):
        url = f'{endpoint.url}?{query}' if query else endpoint.url
        body_wo_content = {a: body[a] for a in body if a not in ['content', 'contents', 'data']}
        if endpoint.is_write:
            self._deploy_safety_limit -= 1
            assert self._deploy_safety_limit >= 0, 'Deploy safety limit reached. Aborting...'
            if self._conf.dry_run:
                _log.warning('dry_run mode. Skipping: %s, body_wo_content: %s', url, body_wo_content)
                return None
        _log.debug('Calling %s, body_wo_content: %s', url, body_wo_content)
        if isinstance(body, str):
            body = json.loads(body)
        return requests.request(
            endpoint.method, 'https://' + self._conf.workspace_host + '/api/' + url,
            headers={'Authorization': 'Bearer ' + self._access_token}, json=body, )

    def call(self, endpoint, body, query=None):
        attempts_left = self._conf.rate_limit_attempts
        while attempts_left > 0:
            _response = self._call(endpoint, body, query)
            if _response is None or _response.ok:
                return _response
            if _response.reason == 429 and self._conf.rate_limit_timeout > 0:
                _log.warning('Databricks API rate limit reached. Waiting for %ss', self._conf.rate_limit_timeout)
                time.sleep(self._conf.rate_limit_timeout)
            else:
                raise RuntimeError(f'Error in response: {_response.text}')
            attempts_left -= 1
        raise RuntimeError(f'Maximum Databricks API call attempt count of {self._conf.rate_limit_attempts} reached.')
