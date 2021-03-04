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
from os import path as op, walk as os_walk
import json
from collections import OrderedDict
from databricks_cicd.utils import Item
from databricks_cicd.utils.api import NOTEBOOK_EXTENSIONS

_log = logging.getLogger(__name__)


class Local:
    @staticmethod
    def _common_name(current_path, base_path):
        return op.relpath(op.splitext(current_path)[0], base_path).replace(op.sep, '/')

    @staticmethod
    def _common_dbfs_name(current_path, base_path):
        return op.relpath(current_path, base_path).replace(op.sep, '/')

    @staticmethod
    def workspace_ls(path) -> OrderedDict:
        _objects = OrderedDict()
        if path is not None:
            for cur_path, dirs, files in os_walk(path):
                for f in files:
                    if op.splitext(f)[1] in NOTEBOOK_EXTENSIONS:
                        _objects[Local._common_name(op.join(cur_path, f), path)] = Item(
                            path=op.join(cur_path, f),
                            kind='workspace notebook',
                            language=NOTEBOOK_EXTENSIONS[op.splitext(f)[1]],
                            is_dir=False)
                for d in dirs:
                    _objects[Local._common_name(op.join(cur_path, d), path)] = Item(
                        path=op.join(cur_path, d),
                        kind='workspace directory',
                        is_dir=True)
        return _objects

    @staticmethod
    def dbfs_ls(path) -> OrderedDict:
        _objects = OrderedDict()
        if path is not None:
            for cur_path, dirs, files in os_walk(path):
                for f in files:
                    _objects[Local._common_dbfs_name(op.join(cur_path, f), path)] = Item(
                        path=op.join(cur_path, f),
                        kind='dbfs file',
                        size=op.getsize(op.join(cur_path, f)),
                        is_dir=False)
                for d in dirs:
                    _objects[Local._common_dbfs_name(op.join(cur_path, d), path)] = Item(
                        path=op.join(cur_path, d),
                        kind='dbfs directory',
                        is_dir=True)
        return _objects

    @staticmethod
    def files_ls(path, extensions=None, kind=None) -> OrderedDict:
        _files = OrderedDict()
        if path is not None:
            for cur_path, _, files in os_walk(path):
                for f in files:
                    if extensions is None or op.splitext(f)[1] in extensions:
                        _files[Local._common_name(op.join(cur_path, f), path)] = Item(
                            path=op.join(cur_path, f),
                            kind=kind,
                            size=op.getsize(op.join(cur_path, f)),
                            is_dir=False)
        return _files

    @staticmethod
    def load_json(path) -> dict:
        with open(path, 'rb') as f:
            return json.loads(f.read())

    @staticmethod
    def load_binary(path) -> bytes:
        with open(path, 'rb') as f:
            return f.read()

    @staticmethod
    def get_file_name(path) -> str:
        return op.splitext(op.basename(path))[0]
