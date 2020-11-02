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
import inspect
from urllib.parse import urlparse

import yaml
from mlrun.datastore import store_manager
from .infer import infer_schema_from_df, get_df_stats
from .pipeline import run_ingestion_pipeline

from .vector import (
    OfflineVectorResponse,
    OnlineVectorService,
    FeatureVector,
)
from mlrun.featurestore.mergers.local import LocalFeatureMerger
from .featureset import FeatureSet
from .model import DataTarget, TargetTypes, FeatureClassKind
from .ingest import write_to_target_store


def store_client(data_prefix="", project=None, secrets=None):
    return FeatureStoreClient(data_prefix, project, secrets)


class FeatureStoreClient:
    def __init__(self, data_prefix="", project=None, secrets=None):
        self._api = None
        self.data_prefix = data_prefix or "./store"
        self.nosql_path_prefix = ""
        self._data_stores = store_manager.set(secrets)
        self._fs = {}
        self._default_ingest_targets = [TargetTypes.parquet]
        self.project = project
        self.parameters = {}

    def get_data_stores(self):
        return self._data_stores

    def _get_db_path(self, kind, name, project=None, version=None):
        project = project or self.project or "default"
        if version:
            name = f"{name}-{version}"
        return f"{self.data_prefix}/{project}/{kind}/{name}"

    def _get_target_path(self, kind, featureset, suffix=""):
        name = featureset.metadata.name
        version = featureset.metadata.tag
        project = featureset.metadata.project or self.project or "default"
        if kind == TargetTypes.nosql:
            data_prefix = nosql_path(self.nosql_path_prefix or self.data_prefix)
        else:
            data_prefix = self.data_prefix
        if version:
            name = f"{name}-{version}"
        return f"{data_prefix}/{project}/{kind}/{name}{suffix}"

    def ingest(
        self,
        featureset: FeatureSet,
        source,
        targets=None,
        namespace=None,
        return_df=True,
        infer_schema=False,
        with_stats=False,
        with_histogram=False,
        with_preview=False,
    ):
        """Read local DataFrame, file, or URL into the feature store"""
        targets = targets or self._default_ingest_targets
        namespace = namespace or inspect.stack()[1][0].f_globals
        entity_list = list(featureset.spec.entities.keys())
        if not entity_list:
            raise ValueError("Entity columns are not defined for this feature set")

        df = run_ingestion_pipeline(self, featureset, source, targets, namespace)
        if infer_schema:
            infer_schema_from_df(source, featureset.spec, entity_list, False)
        if with_stats:
            get_df_stats(df, featureset.status, with_histogram, with_preview)
        if return_df:
            return df

    def run_ingestion_job(
        self, featureset, source_path, targets=None, parameters=None, function=None
    ):
        """Start MLRun ingestion job to load data into the feature store"""
        pass

    def deploy_ingestion_service(
        self, featureset, source_path, targets=None, parameters=None, function=None
    ):
        """Start real-time Nuclio function which loads data into the feature store"""
        pass

    def get_features_metadata(self, features):
        """return metadata (schema & stats) for requested features"""
        pass

    def get_offline_features(
        self,
        features,
        entity_rows=None,
        entity_timestamp_column=None,
        watch=True,
        store_target=None,
    ):

        merger = LocalFeatureMerger()
        vector = FeatureVector(self, features=features)
        vector.parse_features()
        featuresets, feature_dfs = vector.load_featureset_dfs()
        df = merger.merge(
            entity_rows, entity_timestamp_column, featuresets, feature_dfs
        )
        return OfflineVectorResponse(self, df=df)

    def get_online_feature_service(self, features):
        vector = FeatureVector(self, features=features)
        vector.parse_features()
        return OnlineVectorService(self, vector)

    def get_feature_set(self, name, project=None):
        # todo: if name has "/" split to project/name
        target = self._get_db_path(FeatureClassKind.FeatureSet, name, project)
        body = self._data_stores.object(url=target + ".yaml").get()
        obj = yaml.load(body, Loader=yaml.FullLoader)
        return FeatureSet.from_dict(obj)

    def get_feature_vector(self, name, project=None):
        pass

    def save_object(self, obj):
        """save feature set/vector or other definitions into the DB"""
        if obj.kind not in [
            FeatureClassKind.FeatureSet,
            FeatureClassKind.FeatureVector,
        ]:
            raise NotImplementedError(f"object kind not supported ({obj.kind})")
        target = self._get_db_path(obj.kind, obj.metadata.name, obj.metadata.project,)
        self._data_stores.object(url=target + ".yaml").put(obj.to_yaml())


def nosql_path(url):
    parsed_url = urlparse(url)
    scheme = parsed_url.scheme.lower()
    if scheme != "v3io":
        raise ValueError("url must start with v3io://[host]/{container}/{path}")

    endpoint = parsed_url.hostname
    if parsed_url.port:
        endpoint += ":{}".format(parsed_url.port)
    # todo: use endpoint
    return parsed_url.path.strip("/")