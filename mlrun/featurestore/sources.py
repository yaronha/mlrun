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

from .model.base import DataSource


def get_source_driver(source):
    """initialize the source driver"""
    return source_kind_to_driver[source.kind](source)


class BaseSourceDriver:
    def __init__(self, source: DataSource):
        self._source = source

    def to_step(self):
        import storey

        return storey.Source()

    def get_table_object(self):
        """get storey Table object"""
        return None


class CSVSourceDriver(BaseSourceDriver):
    def to_step(self):
        import storey

        attributes = self._source.attributes or {}
        return storey.ReadCSV(paths=self._source.path, header=True, **attributes,)


class DFSourceDriver(BaseSourceDriver):
    def __init__(self, df, key_column=None, time_column=None):
        self._df = df
        self._key_column = key_column
        self._time_column = time_column

    def to_step(self):
        import storey

        return storey.DataframeSource(
            dfs=self._df, key_column=self._key_column, time_column=self._time_column,
        )


source_kind_to_driver = {
    "csv": CSVSourceDriver,
    "dataframe": DFSourceDriver,
}
