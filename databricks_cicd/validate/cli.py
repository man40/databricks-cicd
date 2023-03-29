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

from os import sep, path as op
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
    _log.info('Validating workspace...')
    if conf.validate.workspace_python_notebook_header:
        _log.info('  workspace_python_notebook_header')
        for i in workspace.local_items:
            local_item = workspace.local_items[i]  # type: Item
            if local_item.language == 'PYTHON':
                file_header = Local.head(local_item.path)
                assert file_header == '# Databricks notebook source\n', \
                    'Invalid Python notebook header "{}".'.format(file_header)

    instance_pools = helpers.InstancePoolsHelper(context)

    clusters = helpers.ClustersHelper(context, instance_pools)
    _log.info('Validating clusters...')
    if conf.validate.clusters_no_nested_folders:
        _log.info('  clusters_no_nested_folders')
        for i in clusters.local_items:
            assert len(i.split(sep)) == 1, 'Cluster file {} is in a subfolder. ' \
                'All cluster files should be in a single folder without nesting.'.format(clusters.local_items[i].path)
    if conf.validate.clusters_name:
        _log.info('  clusters_name')
        for i in clusters.local_items:
            local_item = clusters.local_items[i]  # type: Item
            clusters.get_local(local_item, curate_item=False)
            cluster_name = local_item.content['cluster_name']
            assert len(cluster_name) > 0, 'File {0} has no cluster_name.'.format(local_item.path)
            assert op.splitext(op.basename(local_item.path))[0] == cluster_name, \
                'File {} has different name than the cluster "{}" defined in it.'.format(
                    local_item.path, cluster_name)

    jobs = helpers.JobsHelper(context, clusters, workspace)
    _log.info('Validating jobs...')
    if conf.validate.jobs_no_nested_folders:
        _log.info('  jobs_no_nested_folders')
        for i in jobs.local_items:
            assert len(i.split(sep)) == 1, 'Job file {} is in a subfolder. ' \
                'All job files should be in a single folder without nesting.'.format(jobs.local_items[i].path)
    if conf.validate.jobs_name:
        _log.info('  jobs_name')
        for i in jobs.local_items:
            local_item = jobs.local_items[i]  # type: Item
            jobs.get_local(local_item, curate_item=False)
            job_name = local_item.content.get('name', '')
            assert len(job_name) > 0, 'File {0} has no job name.'.format(local_item.path)
            assert op.splitext(op.basename(local_item.path))[0] == job_name, \
                'File {} has different name than the job "{}" defined in it.'.format(local_item.path, job_name)
    if conf.validate.jobs_existing_clusters:
        _log.info('  jobs_existing_clusters')
        for i in jobs.local_items:
            jobs.validate_existing_cluster_name(jobs.local_items[i])
    if conf.validate.jobs_notebook_paths:
        _log.info('  jobs_notebook_paths')
        for i in jobs.local_items:
            jobs.validate_notebook_path(jobs.local_items[i])
    _log.info('All done!')
