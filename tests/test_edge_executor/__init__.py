"""test edge.executor"""

from .test_insert_edge import test as test_insert_edge


def test(config: dict):
    test_insert_edge(config)
