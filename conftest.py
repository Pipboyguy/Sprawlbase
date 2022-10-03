import json
import random
import shutil
import uuid
from pathlib import Path
from random import choice, uniform
from tempfile import TemporaryDirectory

import dask
import distributed.client
import pytest
from dask.distributed import Client, LocalCluster
from faker import Faker

from config import SCOPED_SESSION
from model import (
    ClientModel,
    ClientORM,
    ProviderModel,
    ProviderORM,
    SchedulerModel,
    SchedulerORM,
    TransactionModel,
    TransactionORM,
    UserModel,
    UserORM,
    WorkerModel,
    WorkerORM,
)


@pytest.fixture(scope="class")
def local_cluster():
    local_cluster_ = LocalCluster()
    yield local_cluster_
    local_cluster_.close()


@pytest.fixture(scope="class")
def daskclient(local_cluster):
    daskclient = Client(local_cluster)
    yield daskclient
    daskclient.shutdown()
    daskclient.close()


@pytest.fixture(scope="session")
def fake_generator():
    fake = Faker("en_US")
    yield fake


@pytest.fixture(scope="function")
def temp_test_data(daskclient):
    with TemporaryDirectory() as td:
        td_path = Path(td)

        # make test dataset
        b = dask.datasets.make_people()
        b.map(json.dumps).to_textfiles(str(td_path) + "/*.json")

        yield td_path
        shutil.rmtree(td_path)


@pytest.fixture(scope="function")
def dummy_user(fake_generator):
    first_name = "dummyuser"
    last_name = "dummyuser"
    free_mail_domain = fake_generator.free_email_domain()
    user_mail = (
        first_name[0]
        + last_name
        + str(uuid.uuid4().hex)
        + "@"
        + free_mail_domain
    )

    dummy_user = UserModel(
        id=None,
        name=first_name,
        surname=last_name,
        nationalid=fake_generator.ssn(),
        email=user_mail.lower(),
        bankaccountnumber=fake_generator.bban(),
        accounttype=choice(["CURRENT", "CREDIT"]),
        branchcode=fake_generator.aba(),
        bankname=choice(
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
    )

    with SCOPED_SESSION() as session:
        dummy_user_orm = UserORM(**json.loads(dummy_user.json()))
        session.add(dummy_user_orm)
        session.commit()

        yield dummy_user_orm

        session.delete(dummy_user_orm)
        session.commit()


@pytest.fixture(scope="function")
def dummy_provider(fake_generator):
    provider_name = "dummyprovider"
    user = "dummyemployee"
    mail = (
        user + "@" + "".join(e for e in provider_name if e.isalnum()) + ".com"
    )

    dummy_provider = ProviderModel(
        id=None,
        providername=provider_name,
        email=mail.lower(),
        bankaccountnumber=fake_generator.bban(),
        accounttype=choice(["CURRENT", "CREDIT"]),
        branchcode=fake_generator.aba(),
        bankname=choice(
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
        balance=uniform(1000, 2000),
    )

    with SCOPED_SESSION() as session:
        dummy_provider_orm = ProviderORM(**json.loads(dummy_provider.json()))
        session.add(dummy_provider_orm)
        session.commit()

        yield dummy_provider_orm

        session.delete(dummy_provider_orm)
        session.commit()


def extract_scheduler_details(client: distributed.client.Client) -> (str, int):
    socket_address = client.scheduler.address.split(":")
    host = socket_address[1].replace("/", "")
    port = int(socket_address[2])
    return host, port


@pytest.fixture(scope="function")
def dummy_scheduler(daskclient):

    host, port = extract_scheduler_details(daskclient)

    dummy_scheduler = SchedulerModel(
        id=None,
        host=host,
        port=port,
    )

    with SCOPED_SESSION() as session:
        dummy_scheduler_orm = SchedulerORM(
            **json.loads(dummy_scheduler.json())
        )
        session.add(dummy_scheduler_orm)
        session.commit()

        yield dummy_scheduler_orm

        session.delete(dummy_scheduler_orm)
        session.commit()


@pytest.fixture(scope="function")
def dummy_client(dummy_provider, dummy_scheduler, daskclient):
    dummy_client = ClientModel(
        id=None,
        providerid=dummy_provider.id,
        schedulerid=dummy_scheduler.id,
        host="127.0.0.1",
    )

    with SCOPED_SESSION() as session:
        dummy_client_orm = ClientORM(**json.loads(dummy_client.json()))
        session.add(dummy_client_orm)
        session.commit()

        yield dummy_client_orm

        session.delete(dummy_client_orm)
        session.commit()


@pytest.fixture(scope="function")
def dummy_worker(dummy_user, dummy_client, daskclient):
    dummy_worker = WorkerModel(
        id=None,
        userid=dummy_user.id,
        host="127.0.0.1",
    )

    with SCOPED_SESSION() as session:
        dummy_worker_orm = WorkerORM(**json.loads(dummy_worker.json()))
        session.add(dummy_worker_orm)
        session.commit()

        yield dummy_worker_orm

        session.delete(dummy_worker_orm)
        session.commit()


@pytest.fixture(scope="function")
def dummy_transaction(dummy_client, dummy_worker):
    dummy_transaction = TransactionModel(
        uuid=None,
        clientid=dummy_client.id,
        workerid=dummy_worker.id,
        creditsearned=random.lognormvariate(mu=3, sigma=0.2),
    )

    with SCOPED_SESSION() as session:
        dummy_transaction_orm = TransactionORM(
            **json.loads(dummy_transaction.json())
        )
        session.add(dummy_transaction_orm)
        session.commit()

        yield dummy_transaction_orm

        session.delete(dummy_transaction_orm)
        session.commit()


@pytest.fixture(scope="function")
def dask_sql_context(daskclient):
    from dask_sql.context import Context

    c = Context()
    yield c
    c.stop_server()
