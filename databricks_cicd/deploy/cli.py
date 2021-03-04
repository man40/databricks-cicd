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
from databricks_cicd.utils import Context, display_log
from databricks_cicd.utils.api import API

_log = logging.getLogger(__name__)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deploys local resources, such as notebooks, jobs, clusters to target Databricks workspace.')
@click.option('--token', '-t', required=True,
              help='Access token, used to connect to Databricks workspace.')
@click.option('--workspace', '-w', required=True,
              help='Databricks workspace host to connect to.')
@click.option('--user', '-u', required=True,
              help='The user who creates all objects.')
@click.option('--local_path', '-lp', show_default=True, default='.',
              help='Root path of all source files to be deployed.')
@click.option('--target_path', '-tp', required=True,
              help='Target path for workspace and dbfs.')
@click.option('--name_prefix', '-np', show_default=True, default=None,
              help='Prefix for object names, like jobs, clusters, etc.')
@click.option('--config_file', '-c', show_default=True, default=None,
              help='Path to the config file.')
@click.option('--dry_run', '-np', show_default=True, default=False, is_flag=True,
              help='Pretend run, without modifying the target.')
@click.option('--verbose', show_default=True, default=False, is_flag=True,
              help='Shows debug messages.')
def deploy_cli(**kwargs):
    display_log(logging.DEBUG if kwargs['verbose'] else logging.INFO)

    conf = Conf({
        'global': {'workspace_host': kwargs['workspace'],
                   'local_path': kwargs['local_path'],
                   'name_prefix': kwargs['name_prefix'] if kwargs['name_prefix'] else '',
                   'deploying_user_name': kwargs['user'],
                   'dry_run': str(kwargs['dry_run'])},
        'workspace': {'target_path': kwargs['target_path']},
        'dbfs': {'target_path': kwargs['target_path']},
        },
        kwargs['config_file'])

    api = API(conf, kwargs['token'])
    context = Context(conf, api)
    users = helpers.UsersHelper(context)
    conf.deploying_user_id = users.get_single_item(conf.deploying_user_name).path

    _log.info('''databricks-cicd deploy initialized. Current configuration:
---------------------------------------------------------------------------------------------------------
%s
---------------------------------------------------------------------------------------------------------''', conf)

    # conf.dry_run = True

    workspace = helpers.WorkspaceHelper(context)
    if conf.workspace.deploy:
        _log.info('Deploying workspace...')
        workspace.deploy()

    instance_pools = helpers.InstancePoolsHelper(context)
    if conf.instance_pools.deploy:
        _log.info('Deploying instance pools...')
        instance_pools.deploy()

    clusters = helpers.ClustersHelper(context, instance_pools)
    if conf.clusters.deploy:
        _log.info('Deploying clusters...')
        clusters.deploy()

    jobs = helpers.JobsHelper(context, clusters)
    if conf.jobs.deploy:
        _log.info('Deploying jobs...')
        jobs.deploy()

    dbfs = helpers.DBFSHelper(context)
    if conf.dbfs.deploy:
        _log.info('Deploying dbfs...')
        dbfs.deploy()

    _log.info('All done!')
