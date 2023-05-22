import logging
import sys

LOG_LEVEL = logging.DEBUG
LOG_FORMAT = (
    "[%(asctime)s] %(levelname)s :: %(filename)s :: %(funcName)s :: %(message)s"
)


def create_logger():
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        stream=sys.stdout,
        force=True,
    )
    logging.getLogger().setLevel(LOG_LEVEL)
