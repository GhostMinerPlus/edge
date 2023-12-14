"""Main"""

from sys import path

path.append("./src")

import logging

import toml


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    config = toml.load("./config.toml")
