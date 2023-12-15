"""services"""

from tornado import httputil
import json

import edge


def insert_edge(ctx: dict, req: httputil.HTTPServerRequest) -> str:
    executor: edge.Executor = ctx["executor"]
    edge_form = json.loads(req.body)
    return dict(id=executor.insert_edge(edge_form))


def new_point(ctx: dict, req: httputil.HTTPServerRequest) -> str:
    executor: edge.Executor = ctx["executor"]
    return dict(id=executor.new_point())
