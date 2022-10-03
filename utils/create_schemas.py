import configparser
import os

import psycopg2
from faker import Faker

config = configparser.ConfigParser()
fake = Faker("en_US")

config.read(os.path.join(os.getcwd(), "config.ini"))

HOST = config["SPRAWLHUB"]["HOST"]
PORT = config["SPRAWLHUB"]["PORT"]
SCHEMA = config["SPRAWLHUB"]["SCHEMA"]
USER = config["SPRAWLHUB"]["USER"]
PASSWORD = config["SPRAWLHUB"]["PASSWORD"]

if __name__ == "__main__":
    with psycopg2.connect(
        host=HOST, port=PORT, dbname=SCHEMA, user=USER, password=PASSWORD
    ) as conn:
        conn.set_session(autocommit=True)

        with conn.cursor() as cur:
            with open(
                os.path.join(os.getcwd(), "sql/table_definitions.sql"), "r"
            ) as sql:
                cur.execute(sql.read())
