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

import io
from setuptools import setup, find_packages
import databricks_cicd


setup(
    name='databricks-cicd',
    version=databricks_cicd.__version__,
    packages=find_packages(exclude=['tests', 'tests.*']),
    package_data={'': ['*.ini']},
    include_package_data=True,
    install_requires=io.open('requirements.txt', encoding='utf-8').read(),
    entry_points='''
        [console_scripts]
        cicd=databricks_cicd.cli:cli
    ''',
    zip_safe=False,
    author='Manol Manolov',
    author_email='man40dev@gmail.com',
    description='CICD tool for testing and deploying to Databricks',
    long_description=io.open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='databricks cicd',
)
