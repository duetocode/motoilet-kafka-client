import os
import logging

PREFIX = "LOG_LEVEL_"


def setup():

    # global logging format and level as INFO
    logging.basicConfig(
        format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
        level=os.getenv("LOG_LEVEL", "INFO"),
    )

    logger = logging.getLogger("motoilet_logging")
    logger.setLevel(logging.INFO)

    # go over all environment variables with prefix LOG_LEVEL
    for key, value in os.environ.items():
        if not key.startswith(PREFIX):
            continue

        # get the logger name
        logger_name = key[len(PREFIX) :]
        # ignore empty
        if not logger_name:
            continue

        logger.info("Set logger %s to level %s", logger_name, value)
        # get the logger
        logger = logging.getLogger(logger_name)
        # set the logger level
        logger.setLevel(value)
