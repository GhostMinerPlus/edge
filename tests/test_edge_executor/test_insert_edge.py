"""test edge.executor"""

import logging
import mysql.connector

from edge import insert_edge, new_point

log = logging.getLogger(__name__)


def test(config: dict):
    try:
        point = new_point()
        with mysql.connector.connect(**config["db_config"]) as conn:
            with conn.cursor() as cursor:
                id = insert_edge(cursor, dict(context=point, source="", code="", target=""))
                assert id != point
            conn.rollback()
    except Exception:
        log.error("failed", exc_info=True)
    else:
        log.info("success")
