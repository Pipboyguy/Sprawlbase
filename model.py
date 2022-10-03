import datetime
import uuid
from zoneinfo import ZoneInfo

from pydantic import BaseModel
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Identity,
    Integer,
    Interval,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.dialects.postgresql import UUID as pguuid
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class UserORM(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String)
    surname = Column(String)
    bankaccountnumber = Column(String, nullable=False)
    branchcode = Column(String, nullable=False)
    bankname = Column(String, nullable=False)
    nationalid = Column(String, unique=True)
    email = Column(String, unique=True, nullable=False)
    accounttype = Column(String)
    balance = Column(Float, nullable=False, default=0.0)
    lastchanged = Column(
        DateTime,
        nullable=False,
        default=datetime.datetime.now(tz=ZoneInfo("Africa/Johannesburg")),
    )


class UserModel(BaseModel):
    id: int | None
    name: str | None
    surname: str | None
    bankaccountnumber: str
    branchcode: str
    bankname: str
    nationalid: str | None
    email: str
    accounttype: str | None
    balance: float | None


class ProviderORM(Base):
    __tablename__ = "providers"

    id = Column(Integer, primary_key=True, nullable=False)
    providername = Column(String)
    email = Column(String, unique=True, nullable=False)
    bankaccountnumber = Column(String, nullable=False)
    branchcode = Column(String, nullable=False)
    bankname = Column(String, nullable=False)
    accounttype = Column(String)
    balance = Column(Float, nullable=False, default=0.0)
    lastchanged = Column(
        DateTime,
        nullable=False,
        default=datetime.datetime.now(tz=ZoneInfo("Africa/Johannesburg")),
    )


class ProviderModel(BaseModel):
    id: int | None
    providername: str
    email: str
    bankaccountnumber: str
    branchcode: str
    bankname: str
    accounttype: str | None
    balance: float | None


class TransactionORM(Base):
    __tablename__ = "transactions"

    uuid = Column(
        pguuid, Identity(on_null=True), primary_key=True, nullable=False
    )
    transactiontype = Column(String, nullable=False, default="compute")
    clientid = Column(
        Integer,
        nullable=False,
    )
    workerid = Column(Integer, nullable=False)
    starttime = (
        Column(
            DateTime,
            nullable=False,
            default=datetime.datetime.now(tz=ZoneInfo("Africa/Johannesburg")),
        ),
    )

    endtime = (
        Column(
            DateTime,
            nullable=False,
            default=datetime.datetime.now(tz=ZoneInfo("Africa/Johannesburg")),
        ),
    )

    duration = Column(
        Interval, nullable=False, server_default="endtime - starttime"
    )
    creditsearned = Column(Float, nullable=False, default=0.0)
    brokeragefeeperc = Column(Float, nullable=False, default=0.04)


class TransactionModel(BaseModel):
    uuid: uuid.UUID | None
    clientid: int
    workerid: int
    starttime: datetime.datetime | None
    endtime: datetime.datetime | None
    duration: datetime.timedelta | None
    creditsearned: float
    brokeragefeeperc: float | None = 0.04


class SchedulerORM(Base):
    __tablename__ = "schedulers"

    id = Column(Integer, primary_key=True, nullable=False)
    host = Column(INET, nullable=False)
    port = Column(Integer, nullable=False, default=60706)
    longitude = Column(Float)
    latitude = Column(Float)
    UniqueConstraint("host", name="uix_h")


class SchedulerModel(BaseModel):
    id: int | None
    host: str
    port: int = 60706
    longitude: float | None
    latitude: float | None


class ClientORM(Base):
    __tablename__ = "clients"

    id = Column(Integer, primary_key=True, nullable=False)
    providerid = Column(Integer, nullable=False)
    schedulerid = Column(Integer, nullable=False)
    host = Column(
        INET,
        nullable=False,
    )
    longitude = Column(Float)
    latitude = Column(Float)
    UniqueConstraint("host", name="uih_c")


class ClientModel(BaseModel):
    id: int | None
    providerid: int
    schedulerid: int
    host: str
    longitude: float | None
    latitude: float | None


class WorkerORM(Base):
    __tablename__ = "workers"

    id = Column(Integer, primary_key=True, nullable=False)
    userid = Column(Integer, nullable=False)
    host = Column(
        INET,
        nullable=False,
    )
    longitude = Column(Float)
    latitude = Column(Float)
    UniqueConstraint("host", name="uiw_c")


class WorkerModel(BaseModel):
    id: int | None
    userid: int
    host: str
    longitude: float | None
    latitude: float | None
