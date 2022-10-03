import configparser
import csv
import os
import random
import sys
import uuid
from datetime import timedelta
from random import choice, randint

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

N_USERS = int(config["SEED"]["N_USERS"])
N_WORKERS = int(config["SEED"]["N_WORKERS"])
N_SCHEDULERS = int(config["SEED"]["N_SCHEDULERS"])
N_PROVIDERS = int(config["SEED"]["N_PROVIDERS"])
N_CLIENTS = int(config["SEED"]["N_CLIENTS"])
N_TRANSACTIONS = int(config["SEED"]["N_TRANSACTIONS"])

if __name__ == "__main__":
    with psycopg2.connect(
        host=HOST, port=PORT, dbname=SCHEMA, user=USER, password=PASSWORD
    ) as conn:
        with conn.cursor() as cur:

            for _ in range(N_USERS):
                first_name = fake.first_name()
                last_name = fake.last_name()
                free_mail_domain = fake.free_email_domain()
                user_mail = (
                    first_name[0]
                    + last_name
                    + str(uuid.uuid4().hex)
                    + "@"
                    + free_mail_domain
                )

                cur.execute(
                    "INSERT INTO USERS (nationalid, NAME, surname, bankaccountnumber, accounttype, branchcode, "
                    "bankname, email)  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        fake.ssn(),
                        first_name,
                        last_name,
                        fake.bban(),
                        choice(["CURRENT", "CREDIT"]),
                        fake.aba(),
                        choice(
                            [
                                "First National Bank",
                                "ABSA",
                                "Bidvest",
                                "Capitec",
                                "FirstRand",
                                "Imperial",
                                "Investec",
                                "Mercantile Bank",
                                "Nedbank",
                                "Sasfin",
                                "Standard Bank",
                                "TymeBank",
                            ]
                        ),
                        user_mail.lower(),
                    ),
                )

            # read phones testdata into memory
            with open(
                os.path.join(
                    os.getcwd(), "sql/Mobile-Phones-Database-SAMPLE.csv"
                ),
                "r",
            ) as phones_file:
                reader = csv.DictReader(phones_file)
                phonesdata = dict()
                for row in reader:
                    for header, value in row.items():
                        try:
                            phonesdata[header].append(value)
                        except KeyError:
                            phonesdata[header] = [value]

            for _ in range(N_WORKERS):
                lat, long, _, _, _ = fake.location_on_land()
                cur.execute(
                    "INSERT INTO workers (userid, host, workertype, latitude, longitude) VALUES (%s, %s, %s, %s, "
                    "%s)",
                    (
                        randint(1, N_USERS),
                        fake.ipv4_public(),
                        choice(phonesdata["Phone"]),
                        lat,
                        long,
                    ),
                )

            for _ in range(N_SCHEDULERS):
                lat, long, _, _, _ = fake.location_on_land()
                cur.execute(
                    "INSERT INTO schedulers (host, port, latitude, longitude) VALUES (%s, %s, %s, %s)",
                    (fake.ipv4_public(), 60706, lat, long),
                )

            for _ in range(N_PROVIDERS):
                company = fake.company()
                user = fake.first_name()
                mail = (
                    user
                    + "@"
                    + "".join(e for e in company if e.isalnum())
                    + ".com"
                )

                cur.execute(
                    "INSERT INTO providers (providername, balance, email, bankaccountnumber, accounttype, branchcode, "
                    "bankname"
                    ") VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        company,
                        round(random.uniform(0, 10000), 3),
                        mail.lower(),
                        fake.bban(),
                        choice(["CURRENT", "CREDIT"]),
                        fake.aba(),
                        choice(
                            [
                                "First National Bank",
                                "ABSA",
                                "Bidvest",
                                "Capitec",
                                "FirstRand",
                                "Imperial",
                                "Investec",
                                "Mercantile Bank",
                                "Nedbank",
                                "Sasfin",
                                "Standard Bank",
                                "TymeBank",
                            ]
                        ),
                    ),
                )

            for _ in range(N_CLIENTS):
                lat, long, _, _, _ = fake.location_on_land()
                cur.execute(
                    "INSERT INTO clients (providerid, schedulerid, host, latitude, longitude) VALUES (%s, %s, %s, %s, "
                    "%s)",
                    (
                        randint(1, N_PROVIDERS),
                        randint(1, N_SCHEDULERS),
                        fake.ipv4_public(),
                        lat,
                        long,
                    ),
                )

            for _ in range(N_TRANSACTIONS):
                startdate = fake.date_time_between(
                    start_date="-5y", end_date="-1d"
                )
                enddate = startdate + timedelta(
                    minutes=randint(0, 20), seconds=randint(0, 60)
                )
                cur.execute(
                    "INSERT INTO transactions (starttime, endtime, clientid, workerid, creditsearned) VALUES (%s, "
                    "%s, %s, %s, %s)",
                    (
                        startdate,
                        enddate,
                        randint(1, N_CLIENTS),
                        randint(1, N_WORKERS),
                        round(random.uniform(0, 50), 3),
                    ),
                )

            conn.commit()

    sys.exit(0)
