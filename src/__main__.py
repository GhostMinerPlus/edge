"""Main"""

import logging
from sys import argv
import toml
import mysql.connector
import tornado
import asyncio

import edge

import util
import service


async def main(config: dict):
    conn = mysql.connector.connect(**config["db_config"])
    ctx = {"executor": edge.Executor(conn)}

    app = tornado.web.Application(
        [
            (
                "/%s/insert_edge" % (config["name"]),
                util.PostHandler,
                dict(ctx=ctx, fn=service.insert_edge),
            ),
            (
                "/%s/new_point" % (config["name"]),
                util.PostHandler,
                dict(ctx=ctx, fn=service.new_point),
            ),
        ]
    )
    app.listen(config["port"])
    logging.info("serving at :%d/%s", config["port"], config["name"])
    await asyncio.Event().wait()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = toml.load("./config.toml")
    asyncio.run(main(config))
