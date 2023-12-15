"""Main"""

import logging
from sys import argv

import toml

import edge


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    config = toml.load("./config.toml")

    logging.info("hello, this is %s", config["name"])

    line_path = argv[1]
    clazz = getattr(edge, line_path)
    line = clazz()
    line.run()
