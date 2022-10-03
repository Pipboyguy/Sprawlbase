import json
import uuid
from typing import Dict, List

from fastapi import FastAPI

from config import SCOPED_SESSION, VERSION
from engine import query_csv_dataset
from model import (
    ProviderModel,
    ProviderORM,
    SchedulerModel,
    SchedulerORM,
    TransactionModel,
    TransactionORM,
    UserModel,
    UserORM,
)

app = FastAPI(title="Sprawlhub", version=VERSION)


@app.get("/version")
async def version() -> str:
    return VERSION


@app.get("/user_ids")
async def list_user_ids() -> List[int]:
    with SCOPED_SESSION() as session:
        user_dict_list = session.query(UserORM.id).all()
    return [user["id"] for user in user_dict_list]


@app.get("/user_balance")
async def read_user_balance(userid: int) -> float | None:
    with SCOPED_SESSION() as session:
        user_balance = (
            session.query(UserORM.balance)
            .filter(UserORM.id == userid)
            .scalar()
        )
    return user_balance


@app.get("/user")
async def get_user_data(userid: int) -> Dict | None:
    with SCOPED_SESSION() as session:
        user = (
            session.query(UserORM).filter(UserORM.id == userid).one_or_none()
        )
    return user


@app.put("/user")
async def register_user(user: UserModel) -> int | None:
    """
    Creates user in database, new registration page.
    :param user: User defined as pydantic class
    :return: userID upon successful creation, else none
    """
    with SCOPED_SESSION() as session:
        try:
            user_orm = UserORM(**json.loads(user.json()))

            session.add(user_orm)
            session.commit()

            return user_orm.id
        except Exception as e:
            # TODO: log error with details
            raise e


@app.delete("/user")
async def remove_user(userid: int) -> int | None:
    """
    Deletes user in DB.
    :param userid: Integer. user ID
    :return: id if successful else NULL
    """
    with SCOPED_SESSION() as session:
        try:
            if (
                user_orm := session.query(UserORM)
                .filter(UserORM.id == userid)
                .one()
            ):
                session.delete(user_orm)
            session.commit()
            return userid
        except Exception as e:
            # TODO: log error with details
            raise e


@app.get("/provider")
async def get_provider_data(providerid: int) -> Dict | None:
    with SCOPED_SESSION() as session:
        provider = (
            session.query(ProviderORM)
            .filter(ProviderORM.id == providerid)
            .one_or_none()
        )
    return provider


@app.put("/provider")
async def register_provider(provider: ProviderModel) -> int | None:
    """
    Creates provider in database.
    :param provider: provider defined as pydantic class
    :return: providerID upon successful creation, else none
    """
    with SCOPED_SESSION() as session:
        try:
            provider_orm = ProviderORM(**json.loads(provider.json()))

            session.add(provider_orm)
            session.commit()

            return provider_orm.id
        except Exception as e:
            # TODO: log error with details
            raise e


@app.get("/provider_ids")
async def list_provider_ids() -> List[int]:
    with SCOPED_SESSION() as session:
        provider_dict_list = session.query(ProviderORM.id).all()
    return [provider["id"] for provider in provider_dict_list]


@app.get("/provider_balance")
async def read_provider_balance(providerid: int) -> float | None:
    with SCOPED_SESSION() as session:
        provider_balance = (
            session.query(ProviderORM.balance)
            .filter(ProviderORM.id == providerid)
            .scalar()
        )
    return provider_balance


@app.delete("/provider")
async def remove_provider(providerid: int) -> int | None:
    """
    Deletes provider in DB.
    :param providerid: Integer. provider ID
    :return: id if successful else NULL
    """
    with SCOPED_SESSION() as session:
        try:
            if (
                provider_orm := session.query(ProviderORM)
                .filter(ProviderORM.id == providerid)
                .one()
            ):
                session.delete(provider_orm)
            session.commit()
            return providerid
        except Exception as e:
            # TODO: log error with details
            raise e


@app.put("/scheduler")
async def register_scheduler(scheduler: SchedulerModel) -> int | None:
    """
    Creates scheduler in database
    :param scheduler: Scheduler defined as pydantic class
    :return:  schedulerID upon successful creation, else none
    """
    with SCOPED_SESSION() as session:
        try:
            scheduler_orm = SchedulerORM(**json.loads(scheduler.json()))

            session.add(scheduler_orm)
            session.commit()

            return scheduler_orm.id
        except Exception as e:
            # TODO: log error with details
            raise e


@app.delete("/scheduler")
async def remove_scheduler(schedulerid: int) -> int | None:
    """
    Deletes scheduler in DB.
    :param schedulerid: Integer. scheduler ID
    :return: id if successful else NULL
    """
    with SCOPED_SESSION() as session:
        try:
            if (
                scheduler_orm := session.query(SchedulerORM)
                .filter(SchedulerORM.id == schedulerid)
                .one()
            ):
                session.delete(scheduler_orm)
            session.commit()
            return schedulerid
        except Exception as e:
            # TODO: log error with details
            raise e


@app.put("/transaction")
async def register_transaction(
    transaction: TransactionModel,
) -> uuid.UUID | None:
    """
    Creates transaction in database.
    :type transaction: TransactionModel
    """
    with SCOPED_SESSION() as session:
        try:
            transaction_orm = TransactionORM(**json.loads(transaction.json()))

            session.add(transaction_orm)
            session.commit()

            return transaction_orm.uuid
        except Exception as e:
            # TODO: log error with details
            raise e


@app.post("/sql")
def run_sql(
    sql: str, datasources: Dict[str, str], datasource_type: str = "csv"
) -> List[Dict]:
    """
    Executes SQL against available workers and returns result

    :param datasource_type:
    :type sql: SQL to run
    :param datasources: dictionary of datasource names to register and url locations
    :return: result as list of records
    """
    if datasource_type == "csv":
        res = query_csv_dataset(sql=sql, datasources=datasources)
    else:
        res = 'Please choose a datasource_type that is in ["csv"]'

    return res
