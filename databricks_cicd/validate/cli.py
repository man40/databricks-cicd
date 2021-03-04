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
import click
import databricks_cicd.utils.helpers as helpers
from databricks_cicd import CONTEXT_SETTINGS
from databricks_cicd.conf import Conf
from databricks_cicd.utils import Context, display_log, Item
from databricks_cicd.utils.local import Local

_log = logging.getLogger(__name__)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Validates source files.')
@click.option('--local_path', '-lp', show_default=True, default='.',
              help='Root path of all source files to be deployed.')
@click.option('--config_file', '-c', show_default=True, default=None,
              help='Path to the config file.')
@click.option('--verbose', show_default=True, default=False, is_flag=True,
              help='Shows debug messages.')
def validate_cli(**kwargs):
    display_log(logging.DEBUG if kwargs['verbose'] else logging.INFO)

    conf = Conf({
        'global': {'local_path': kwargs['local_path']},
        },
        kwargs['config_file'])

    _log.info('''databricks-cicd validate initialized. Current configuration:
---------------------------------------------------------------------------------------------------------
%s
---------------------------------------------------------------------------------------------------------''', conf)

    context = Context(conf)

    workspace = helpers.WorkspaceHelper(context)
    if conf.workspace.deploy:
        _log.info('Validating workspace...')
        for i in workspace.local_items:
            local_item = workspace.local_items[i]  # type: Item
    #         TODO: check if references to other notebooks are valid

    instance_pools = helpers.InstancePoolsHelper(context)
    if conf.instance_pools.deploy:
        _log.info('Validating instance pools...')
        for i in instance_pools.local_items:
            local_item = instance_pools.local_items[i]  # type: Item
            instance_pools.get_local(local_item)
            name_attribute = 'instance_pool_name'
            if local_item.content.get(name_attribute) != Local.get_file_name(local_item.path):
                _log.error('%s')
            print(local_item.content)

    _log.info('All done!')
