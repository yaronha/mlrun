# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .base import DataStore, FileStats
import mlrun
import dask.dataframe as dd
import os


class DaskStore(DataStore):
    def __init__(self, parent, schema='dask', name='dask', endpoint=''):
        endpoint.replace(':', '/')
        super().__init__(parent, name, schema, endpoint)
        function = mlrun.import_function('db://' + endpoint)
        self._client = function.client

    def _secret(self, key):
        return None

    @property
    def client(self):
        """dask client"""
        return self._client

    def _get_item(self, key):
        return self.client.get_dataset(key)

    def get(self, key, size=None, offset=0):
        return self._get_item(key)

    def put(self, key, data, append=False):
        client = self.client
        if not isinstance(data, dd.core.DataFrame):
            raise ValueError('data must be a valid dask dataframe')
        client.persist(data)
        client.datasets[key] = data

    def upload(self, key, src_path):
        _, file_ext = os.path.splitext(src_path)
        if file_ext == '.csv':
            df = dd.read_csv(src_path)
        elif file_ext in ['pq', 'parquet']:
            df = dd.read_parquet(src_path)
        else:
            raise ValueError(f'unsupported format suffix ({file_ext})')
        self.put(key, df)

    def stat(self, key):
        return FileStats(size=len(self._get_item(key)))

    def listdir(self, key):
        return self.client.list_datasets()

    def as_df(self, key, columns=None, df_module=None, format='', **kwargs):
        df = self._get_item(key)
        # todo: filter columns
        return df
