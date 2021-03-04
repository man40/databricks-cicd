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
import sys
from databricks_cicd.conf import Conf
from databricks_cicd.utils.api import API

_log = logging.getLogger(__name__)


class Item:
    """
    Used as a descriptor of a deployable item to the remote (local or remote).
    It is used for any object type (Job, Directory, Notebook, File, Cluster, etc.).
    """

    def __init__(self, path: str, kind: str, is_dir=False, size: int = None, language: str = None, content=None):
        self.is_dir = is_dir
        self.kind = kind
        self.path = path
        self.language = language
        self.size = size
        self.content = content


class Context:
    def __init__(self, config: Conf, _api: API = None):
        self.api = _api
        self.conf = config


def is_different(left, right, ignore_keys=None):
    """
    Compares two dictionaries, disregarding order on keys and lists.
    Also missing attribute will be treated the same as empty dict or list.
    This is used to compare local with remote objects where the order of the attributes does not matter.
    :param left: compare left with right
    :param right: compare left with right
    :param ignore_keys: list. Ignores dict keys, while comparing
    :return:
    """
    if left in (None, [], {}) and right in (None, [], {}):
        return False
    if left is None or right is None:
        _log.debug('%s is diff from %s', left, right)
        return True
    if isinstance(left, dict) and isinstance(right, dict):
        if ignore_keys is None:
            ignore_keys = []
        for key in (set(left) | set(right)) - set(ignore_keys):
            if is_different(left.get(key), right.get(key)):
                _log.debug('%s is diff from %s', left.get(key), right.get(key))
                return True
        return False
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return True
        sorted_right = sorted(right)
        for i, value in enumerate(sorted(left)):
            if is_different(value, sorted_right[i]):
                _log.debug('%s is diff from %s', value, sorted_right[i])
                return True
        return False
    if left == right:
        return False
    if not isinstance(left, type(right)) \
            and isinstance(left, (float, int)) \
            and isinstance(right, (float, int)) \
            and float(left) == float(right):
        return False
    return True


def display_log(level):
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    for name in ['databricks_cicd', '__main__']:
        logging.getLogger(name).setLevel(level)
        logging.getLogger(name).addHandler(stream_handler)
