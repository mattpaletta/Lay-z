# Initialize our logger
import logging
import sys

root = logging.getLogger()
root.setLevel(logging.NOTSET)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.NOTSET)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)s]')
ch.setFormatter(formatter)
root.addHandler(ch)

