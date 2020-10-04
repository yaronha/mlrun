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

import json
from io import BytesIO
from urllib.request import urlopen


class BaseModelRouter:
    """base model router class"""

    def __init__(self, context, state, **kwargs):
        self.context = context
        self.routes = {}
        for key, child in state.items():
            if hasattr(child, "run"):
                child = child.run
            self.routes[key] = child
        self.url_prefix = kwargs.get("url_prefix", "/v2/models")
        self.health_prefix = kwargs.get("health_prefix", "/v2/health")

    def parse_event(self, event):
        parsed_event = {"data": []}
        try:
            if not isinstance(event.body, dict):
                body = json.loads(event.body)
            else:
                body = event.body
            if "data_url" in body:
                # Get data from URL
                url = body["data_url"]
                self.context.logger.debug_with("downloading data", url=url)
                data = urlopen(url).read()
                sample = BytesIO(data)
                parsed_event["data"].append(sample)
            else:
                parsed_event = body

        except Exception as e:
            #  if images convert to bytes
            if getattr(event, "content_type", "").startswith("image/"):
                sample = BytesIO(event.body)
                parsed_event["data"].append(sample)
                parsed_event["content_type"] = event.content_type
            else:
                raise ValueError("Unrecognized request format: %s" % e)

        return parsed_event

    def post_init(self):
        # Verify that models are loaded
        assert (
            len(self.routes) > 0
        ), "No models were loaded!\n Please register child models"
        self.context.logger.info(f"Loaded {list(self.routes.keys())}")

    def get_metadata(self):
        """return the model router/host details"""

        return {"name": self.__class__.__name__, "version": "v2", "extensions": []}

    def do_event(self, event, *args, **kwargs):
        """handle incoming events, event is nuclio event class"""

        event = self.preprocess(event)
        event.body = self.parse_event(event)
        urlpath = getattr(event, "path", "")
        method = event.method or "POST"

        # if health check or "/" return Ok + metadata
        if method == "GET" and (
            urlpath == "/" or urlpath.startswith(self.health_prefix)
        ):
            setattr(event, "terminated", True)
            event.body = self.get_metadata()
            return event

        # check for legal path prefix
        if urlpath and not urlpath.startswith(self.url_prefix):
            raise ValueError(
                f"illegal path prefix {urlpath}, must start with {self.url_prefix}"
            )

        return self.postprocess(self._do(event))

    def _do(self, event):
        return event

    def preprocess(self, event):
        """run tasks before processing the event"""
        return event

    def postprocess(self, event):
        """run tasks after processing the event"""
        return event


class ModelRouter(BaseModelRouter):
    def _select_child(self, body, urlpath):
        subpath = None
        model = ""
        if urlpath:
            # process the url <prefix>/<model>[/versions/<ver>]/operation
            subpath = ""
            urlpath = urlpath[len(self.url_prefix) :].strip("/")
            if not urlpath:
                return "", None, ""
            segments = urlpath.split("/")
            model = segments[0]
            if len(segments) > 2 and segments[1] == "versions":
                model = model + ":" + segments[2]
                segments = segments[2:]
            if len(segments) > 1:
                subpath = "/".join(segments[1:])

        model = model or body.get("model", list(self.routes.keys())[0])
        subpath = body.get("operation", subpath)
        if subpath is None:
            subpath = "infer"

        if model not in self.routes:
            models = "| ".join(self.routes.keys())
            raise ValueError(f"model {model} doesnt exist, available models: {models}")

        return model, self.routes[model], subpath

    def _do(self, event):
        name, child, subpath = self._select_child(event.body, event.path)
        if not child:
            # if model wasn't specified return model list
            setattr(event, "terminated", True)
            event.body = {"models": list(self.routes.keys())}
            return event

        self.context.logger.debug(
            f"router run model {name}, body={event.body}, op={subpath}"
        )
        event.path = subpath
        response = child(event)
        event.body = response.body if response else None
        return event