"""executor"""

import uuid
from mysql.connector.connection import MySQLConnection, MySQLCursor


def insert_edge(cursor: MySQLCursor, edge_form: dict) -> str:
    edge_form["id"] = new_point()

    cursor.execute(
        "insert into edge_t (id, context, source, code, target) \
            values (%(id)s, %(context)s, %(source)s, %(code)s, %(target)s)",
        edge_form,
    )

    return edge_form["id"]


def new_point() -> str:
    return str(uuid.uuid4())


class Executor:
    def __init__(self, conn: MySQLConnection):
        self.__conn = conn

    def insert_edge(self, edge_form: dict) -> str:
        with self.__conn.cursor() as cursor:
            id = insert_edge(cursor, edge_form)
        self.__conn.commit()
        return id

    def new_point(self) -> str:
        return new_point()
