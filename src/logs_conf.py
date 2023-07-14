from datetime import datetime
import logging
import os

if not os.path.exists("logs"):
    os.mkdir("logs")

LOGFORMAT = "%(asctime)s : %(levelname)s | %(message)s"

logging.basicConfig(
    filename=os.path.join("logs", f"run-{datetime.strftime(datetime.now(), '%Y-%m-%d')}.log"),
    filemode="w",
    level = logging.DEBUG,
    format = LOGFORMAT
)

stream = logging.StreamHandler()
stream.setLevel(logging.DEBUG)
stream.setFormatter(logging.Formatter(LOGFORMAT))

logging.getLogger("").addHandler(stream)

LOG_MAIN = logging.getLogger("MAIN")
LOG_GET = logging.getLogger("API")
LOG_TRANSFORM = logging.getLogger("TRANSFORM")
LOG_FS = logging.getLogger("LOAD")