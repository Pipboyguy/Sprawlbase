"""Tests

Testsuite for sprawlhub server
"""
import json
import numbers
import random
import re
from pathlib import Path

import dask.bag as db
import dask.dataframe as ddf
import dask.datasets
import dask_ml.datasets
import dask_ml.ensemble
import pandas as pd
import pytest
import sklearn.linear_model
from dask_sql.integrations.fugue import DaskSQLExecutionEngine
from fugue_sql import FugueSQLWorkflow
from httpx import AsyncClient
from sqlalchemy.exc import IntegrityError

from config import SCOPED_SESSION
from main import app
from model import TransactionModel, TransactionORM


class TestSundry:
    @pytest.mark.anyio
    async def test_version(self):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.get("/version")
        assert r.status_code == 200
        assert re.search(r"(\d\.\d\.\d)", r.text) is not None


class TestWork:
    def test_connect_to_dask_scheduler(self, daskclient):
        assert daskclient is not None

    def test_can_read_json(self, daskclient, temp_test_data):
        b = db.read_text(str(temp_test_data) + "/*.json").map(json.loads)
        assert len(b.take(2)) == 2

    def test_data_filter(self, daskclient, temp_test_data):
        b = db.read_text(str(temp_test_data) + "/*.json").map(json.loads)
        b.map(lambda record: record["occupation"]).take(2)
        assert len(b.take(2)) == 2

    def test_pandas(self, daskclient):
        diamonds_data_path = Path("testdata/diamonds.csv")
        diamonds_df = ddf.read_csv(diamonds_data_path, assume_missing=True)
        assert diamonds_df["price"].min().compute() == 326.0


class TestAPI:
    @pytest.mark.anyio
    async def test_list_users(self):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.get("/user_ids")
        assert r.status_code == 200
        users = eval(r.text)
        assert isinstance(users, list)
        num_users = len(users)
        if not isinstance(num_users, numbers.Number) or num_users == 0:
            raise TypeError(
                f"Number of users in database not a number or empty: {num_users}."
            )

    @pytest.mark.anyio
    async def test_list_providers(self):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.get("/provider_ids")
        assert r.status_code == 200
        providers = eval(r.text)
        assert isinstance(providers, list)
        num_providers = len(providers)
        if not isinstance(num_providers, numbers.Number) or num_providers == 0:
            raise TypeError(
                f"Number of providers in database not a number or empty: {num_providers}."
            )

    @pytest.mark.anyio
    async def test_get_user_data(self, dummy_user):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.get("/user", params=dict(userid=dummy_user.id))
        assert r.status_code == 200
        assert r.json()["id"] == dummy_user.id

    @pytest.mark.anyio
    async def test_get_provider_data(self, dummy_provider):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.get(
                "/provider", params=dict(providerid=dummy_provider.id)
            )
        assert r.status_code == 200
        assert r.json()["id"] == dummy_provider.id

    @pytest.mark.anyio
    async def test_read_user_balance(self, dummy_user):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.get(
                "/user_balance", params=dict(userid=dummy_user.id)
            )
        assert r.status_code == 200
        amount = float(r.text)
        if not isinstance(amount, numbers.Number):
            raise TypeError(f"Credit Balance not a number: {amount}.")
        if float(amount) < 0.0:
            raise ValueError(f"Credit Balance is negative: ({amount}).")

    @pytest.mark.anyio
    async def test_read_provider_balance(self, dummy_provider):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.get(
                "/provider_balance", params=dict(providerid=dummy_provider.id)
            )
        assert r.status_code == 200
        amount = float(r.text)
        if not isinstance(amount, numbers.Number):
            raise TypeError(f"Credit Balance not a number: {amount}.")
        if float(amount) < 0.0:
            raise ValueError(f"Credit Balance is negative: ({amount}).")

    @pytest.mark.anyio
    async def test_remove_user(self, dummy_user):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.delete("/user", params=dict(userid=dummy_user.id))
        assert r.status_code == 200
        assert int(r.content) == dummy_user.id

    @pytest.mark.anyio
    async def test_remove_provider(self, dummy_provider):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.delete(
                "/provider", params=dict(providerid=dummy_provider.id)
            )
        assert r.status_code == 200
        assert int(r.content) == dummy_provider.id

    @pytest.mark.anyio
    async def test_register_user(self, dummy_user):

        register_user_data = dict(
            name=dummy_user.name,
            surname=dummy_user.surname,
            nationalid=dummy_user.nationalid,
            email=dummy_user.email,
            bankaccountnumber=dummy_user.bankaccountnumber,
            accounttype=dummy_user.accounttype,
            branchcode=dummy_user.branchcode,
            bankname=dummy_user.bankname,
        )

        try:
            async with AsyncClient(app=app, base_url="http://localhost") as ac:
                await ac.put("/user", json=register_user_data)
        except IntegrityError:
            # test PASSED
            return

        # test did NOT pass
        raise Exception("Same user was created twice!")

    @pytest.mark.anyio
    async def test_register_provider_integrity(self, dummy_provider):

        register_provider_data = dict(
            providername=dummy_provider.providername,
            email=dummy_provider.email,
            bankaccountnumber=dummy_provider.email,
            accounttype=dummy_provider.accounttype,
            branchcode=dummy_provider.branchcode,
            bankname=dummy_provider.bankname,
        )

        try:
            async with AsyncClient(app=app, base_url="http://localhost") as ac:
                await ac.put("/provider", json=register_provider_data)
        except IntegrityError:
            # test PASSED
            return

        # test did NOT pass
        raise Exception("Same provider was created twice!")

    @pytest.mark.anyio
    async def test_register_provider_payload(self, fake_generator):

        company = fake_generator.company()
        user = fake_generator.first_name()
        mail = user + "@" + "".join(e for e in company if e.isalnum()) + ".com"

        provider_details = {
            "providername": company,
            "email": mail,
            "bankaccountnumber": fake_generator.bban(),
            "branchcode": fake_generator.aba(),
            "bankname": "testbank",
        }

        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.put("/provider", json=provider_details)
        assert r.status_code == 200
        assert int(r.content) > 0

    @pytest.mark.anyio
    async def test_register_scheduler(self, dummy_scheduler):

        register_scheduler_data = dict(
            schedulerid=dummy_scheduler.id,
            host=dummy_scheduler.host,
            port=dummy_scheduler.port,
            longitude=dummy_scheduler.longitude,
            latitude=dummy_scheduler.latitude,
        )

        try:
            async with AsyncClient(app=app, base_url="http://localhost") as ac:
                await ac.put("/scheduler", json=register_scheduler_data)
        except IntegrityError:
            # test PASSED
            return

        # test did NOT pass
        raise Exception("Same scheduler was created twice!")

    @pytest.mark.anyio
    async def test_register_transaction(self, dummy_transaction):
        register_transaction_data = dict(
            uuid=dummy_transaction.uuid,
            clientid=dummy_transaction.clientid,
            workerid=dummy_transaction.workerid,
            creditsearned=dummy_transaction.creditsearned,
        )

        try:
            async with AsyncClient(app=app, base_url="http://localhost") as ac:
                await ac.put("/transaction", json=register_transaction_data)
        except IntegrityError:
            # test PASSED
            return

        # test did NOT pass
        raise Exception("Same transaction was created twice!")

    @pytest.mark.anyio
    async def test_user_balance_updates(
        self, dummy_client, dummy_worker, dummy_user
    ):

        gross_amount_earned = random.lognormvariate(mu=3, sigma=0.2)
        dummy_transaction = TransactionModel(
            clientid=dummy_client.id,
            workerid=dummy_worker.id,
            creditsearned=gross_amount_earned,
            uuid=None,
        )

        net_amount_earned = (
            1 - dummy_transaction.brokeragefeeperc
        ) * gross_amount_earned
        correct_user_balance_after_transaction = (
            dummy_user.balance + net_amount_earned
        )

        with SCOPED_SESSION() as session:
            dummy_transaction_orm = TransactionORM(
                **json.loads(dummy_transaction.json())
            )
            session.add(dummy_transaction_orm)
            session.commit()

            assert dummy_user.balance == correct_user_balance_after_transaction

    @pytest.mark.anyio
    async def test_user_provider_updates(
        self, dummy_provider, dummy_worker, dummy_client
    ):

        gross_amount_earned = random.lognormvariate(mu=3, sigma=0.2)
        dummy_transaction = TransactionModel(
            clientid=dummy_client.id,
            workerid=dummy_worker.id,
            creditsearned=gross_amount_earned,
            uuid=None,
        )

        correct_provider_balance_after_transaction = (
            dummy_provider.balance - gross_amount_earned
        )

        with SCOPED_SESSION() as session:
            dummy_transaction_orm = TransactionORM(
                **json.loads(dummy_transaction.json())
            )
            session.add(dummy_transaction_orm)
            session.commit()

            assert (
                dummy_provider.balance
                == correct_provider_balance_after_transaction
            )


class TestIntegration:
    """
    Tests integration between all components of system.
    """

    def test_work_triggers_transaction(
        self, dummy_user, dummy_provider, daskclient
    ):
        # do non-trivial work
        X, y = dask_ml.datasets.make_classification(
            n_samples=1_000_000,
            n_informative=10,
            shift=2,
            scale=2,
            chunks=100_000,
        )
        subestimator = sklearn.linear_model.RidgeClassifier()
        clf = dask_ml.ensemble.BlockwiseVotingClassifier(
            subestimator, classes=[0, 1]
        )
        clf.fit(X, y)
        preds = clf.predict(X).compute()
        assert preds is not None

    def test_dask_sql(self, daskclient, dask_sql_context):
        df = dask.datasets.timeseries()

        dask_sql_context.create_table("timeseries", df)

        result = dask_sql_context.sql(
            """
            SELECT
                name, SUM(x) AS "sum"
            FROM timeseries
            WHERE x > 0.5
            GROUP BY name
        """
        )
        res = result.compute()
        assert res is not None

    def test_fugue_sql(self, daskclient, dask_sql_context):
        data = [
            ["A", "2020-01-01", 10],
            ["A", "2020-01-02", 20],
            ["A", "2020-01-03", 30],
            ["B", "2020-01-01", 20],
            ["B", "2020-01-02", 30],
            ["B", "2020-01-03", 40],
        ]
        schema = "id:str,date:date,value:int"

        # schema: *, cumsum:int
        def cumsum(df: pd.DataFrame) -> pd.DataFrame:
            df["cumsum"] = df["value"].cumsum()
            return df

        # Run the DAG on the DaskSQLExecutionEngine by dask-sql
        with FugueSQLWorkflow(DaskSQLExecutionEngine) as dag:
            df = dag.df(data, schema)
            dag(
                """
            SELECT *
            FROM df
            TRANSFORM PREPARTITION BY id PRESORT date ASC USING cumsum
            TAKE 5 ROWS
            PRINT
            """
            )
            assert df is not None

    @pytest.mark.anyio
    async def test_sql_engine_works(self):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.post(
                "/sql",
                params={
                    "sql": "SELECT 10 as number;",
                    "datasources": {"test": "test"},
                },
            )
        assert r.status_code == 200
        assert eval(r.content)[0]["number"] == 10

    @pytest.mark.anyio
    async def test_sql(self, dask_sql_context):
        async with AsyncClient(app=app, base_url="http://localhost") as ac:
            r = await ac.post(
                "/sql",
                params={
                    "sql": "SELECT count(*) from tamil_sentiment as length;",
                    "datasources": {
                        "tamil_sentiment": "https://archive.ics.uci.edu/ml/machine-learning-databases/00610/Tamil_first_ready_for_sentiment.csv"
                    },
                },
            )

            assert r.status_code == 200
            assert eval(r.content)[0]["length"] == 15743
