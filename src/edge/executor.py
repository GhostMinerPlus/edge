"""executor"""

import uuid
from mysql.connector.connection import MySQLConnection


def insert_edge(conn: MySQLConnection, edge_form: dict) -> str:
    edge_form["id"] = new_point()

    with conn.cursor() as cursor:
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
        self.conn = conn

    def insert_edge(self, edge_form: dict) -> str:
        id = insert_edge(self.conn, edge_form)
        self.conn.commit()
        return id

    def new_point(self) -> str:
        return new_point()
