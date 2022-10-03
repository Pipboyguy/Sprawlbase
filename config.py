import configparser
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), "config.ini"))

HOST = config["SPRAWLHUB"]["HOST"]
PORT = config["SPRAWLHUB"]["PORT"]
SCHEMA = config["SPRAWLHUB"]["SCHEMA"]
USER = config["SPRAWLHUB"]["USER"]
PASSWORD = config["SPRAWLHUB"]["PASSWORD"]
VERSION = config["SPRAWLHUB"]["VERSION"]

CONN_STR = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{SCHEMA}"

# create reusable engine and session
ENGINE = create_engine(CONN_STR)
SESSION = sessionmaker(bind=ENGINE)
SCOPED_SESSION = scoped_session(SESSION)
