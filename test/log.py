import motoilet_logging.motoilet_logging as motoilet_logging
import logging


def main():
    motoilet_logging.setup()
    logging.getLogger("ABC").info("Hello, There!")


if __name__ == "__main__":
    main()
