-- SPRAWLBASE Database Definitions --
--  Create initial table schemas

-- The following two extensions need to be added as a superuser.
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- CREATE EXTENSION IF NOT EXISTS postgis;
-- CREATE EXTENSION citext;

DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS providers CASCADE;
DROP TABLE IF EXISTS schedulers CASCADE;
DROP TABLE IF EXISTS clients CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS workers CASCADE;

CREATE TABLE users
(
    id
                      SERIAL
        CONSTRAINT
            users_pk
            PRIMARY
                KEY,
    nationalid
                      TEXT
        UNIQUE,
    name
                      TEXT,
    surname
                      TEXT,
    email             CITEXT UNIQUE                  NOT NULL,
    bankaccountnumber TEXT                           NOT NULL,
    accounttype       TEXT,
    branchcode        TEXT                           NOT NULL,
    bankname          TEXT                           NOT NULL,
    balance           DOUBLE PRECISION DEFAULT 0.0   NOT NULL,
    lastchanged       TIMESTAMP        DEFAULT NOW() NOT NULL
);

CREATE TABLE workers
(
    id         SERIAL
        CONSTRAINT workers_pk
            PRIMARY KEY,
    userid     INTEGER     NOT NULL
        CONSTRAINT workers_users_userid_fk
            REFERENCES users
            ON UPDATE CASCADE ON DELETE CASCADE
            DEFERRABLE,
    host       INET UNIQUE NOT NULL,
    workertype TEXT,
    longitude  FLOAT DEFAULT NULL,
    latitude   FLOAT DEFAULT NULL,
    location   GEOGRAPHY GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED
);


CREATE TABLE providers
(
    id                SERIAL
        CONSTRAINT providers_pk
            PRIMARY KEY,
    providername      TEXT                           NOT NULL,
    email             CITEXT UNIQUE,
    bankaccountnumber TEXT                           NOT NULL,
    accounttype       TEXT,
    branchcode        TEXT                           NOT NULL,
    bankname          TEXT                           NOT NULL,
    balance           DOUBLE PRECISION DEFAULT 0.0   NOT NULL,
    lastchanged       TIMESTAMP        DEFAULT NOW() NOT NULL
);

CREATE TABLE schedulers
(
    id        SERIAL      NOT NULL
        CONSTRAINT schedulers_pk
            PRIMARY KEY,
    host      INET UNIQUE NOT NULL,
    port      INT         NOT NULL,
    longitude FLOAT DEFAULT NULL,
    latitude  FLOAT DEFAULT NULL,
    location  GEOGRAPHY GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED
);


CREATE TABLE clients
(
    id          SERIAL      NOT NULL
        CONSTRAINT clients_pk
            PRIMARY KEY,
    providerid  INT         NOT NULL
        CONSTRAINT clients_providers_providerid_fk
            REFERENCES providers
            ON UPDATE CASCADE ON DELETE CASCADE
            DEFERRABLE,
    schedulerid INT         NOT NULL
        CONSTRAINT clients_schedulers_schedulerid_fk
            REFERENCES schedulers
            ON UPDATE CASCADE ON DELETE CASCADE
            DEFERRABLE,
    host        INET UNIQUE NOT NULL,
    longitude   FLOAT DEFAULT NULL,
    latitude    FLOAT DEFAULT NULL,
    location    GEOGRAPHY GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED
);

CREATE TABLE transactions
(
    uuid             UUID                        NOT NULL DEFAULT uuid_generate_v1()
        CONSTRAINT transactions_pk
            PRIMARY KEY,
    transactiontype  TEXT      DEFAULT 'compute' NOT NULL,
    clientid         INT                         NOT NULL
        CONSTRAINT transactions_clients_clientid_fk
            REFERENCES clients
            ON UPDATE CASCADE ON DELETE CASCADE
            DEFERRABLE,
    workerid         INT                         NOT NULL
        CONSTRAINT transactions_workers_workerid_fk
            REFERENCES workers
            ON UPDATE CASCADE ON DELETE CASCADE
            DEFERRABLE,
    starttime        TIMESTAMP DEFAULT NOW()     NOT NULL,
    endtime          TIMESTAMP DEFAULT NOW()     NOT NULL,
    duration         INTERVAL GENERATED ALWAYS AS (
                         endtime - starttime
                         ) STORED,
    creditsearned    FLOAT     DEFAULT 0.0       NOT NULL,
    brokeragefeeperc FLOAT     DEFAULT 0.04      NOT NULL,
    brokeragefee     FLOAT GENERATED ALWAYS AS (
                         creditsearned * brokeragefeeperc
                         ) STORED,
    netcreditsearned FLOAT GENERATED ALWAYS AS (
                         creditsearned * (1.0 - brokeragefeeperc)
                         ) STORED
);

CREATE UNIQUE INDEX providers_id_uindex
    ON providers (id);
CREATE UNIQUE INDEX users_nationalid_uindex
    ON users (nationalid);
CREATE UNIQUE INDEX users_id_uindex
    ON users (id);
CREATE UNIQUE INDEX workers_id_uindex
    ON workers (id);
CREATE UNIQUE INDEX schedulers_id_uindex
    ON schedulers (id);
CREATE UNIQUE INDEX transactions_uuid_uindex
    ON transactions (uuid);
CREATE INDEX transactions_clientid_index
    ON transactions (clientid);
CREATE INDEX transactions_workerid_index
    ON transactions (workerid);
CREATE INDEX transactions_workerid_clientid_index
    ON transactions (workerid, clientid);
CREATE UNIQUE INDEX clients_id_uindex
    ON clients (id);
CREATE UNIQUE INDEX ON users ((LOWER(email)));
CREATE UNIQUE INDEX ON providers ((LOWER(email)));
-- spatial index to calculate closes distances between entities
CREATE INDEX ON schedulers USING gist (location);
CREATE INDEX ON workers USING gist (location);
CREATE INDEX ON clients USING gist (location);

ALTER TABLE providers
    OWNER TO sprawlhub;
ALTER TABLE users
    OWNER TO sprawlhub;
ALTER TABLE workers
    OWNER TO sprawlhub;
ALTER TABLE transactions
    OWNER TO sprawlhub;
ALTER TABLE schedulers
    OWNER TO sprawlhub;

-- Update provider and user balances after new transaction is added
CREATE OR REPLACE FUNCTION update_balances_on_txn()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS
$$
BEGIN

    UPDATE providers
    SET balance = balance - new.creditsearned
    FROM clients
    WHERE clients.id = new.clientid
      AND providers.id = clients.providerid;

    UPDATE users
    SET balance = balance + new.netcreditsearned
    FROM workers
    WHERE workers.id = new.workerid
      AND users.id = workers.userid;

    RETURN NEW;
END
    ;
$$;


CREATE TRIGGER update_balances_on_txn_trigger
    AFTER INSERT
    ON transactions
    FOR EACH ROW
EXECUTE PROCEDURE update_balances_on_txn();

COMMENT ON TABLE users IS 'User ID table along with Banking Details - Dimension Table';
COMMENT ON TABLE workers IS 'Workers ID table - Dimension Table';
COMMENT ON TABLE providers IS 'Providers ID table along with Balance - Dimension Table';
COMMENT ON TABLE schedulers IS 'Schedulers ID table with ip addresses and ports - Dimension Table';
COMMENT ON TABLE transactions IS 'Ledger that records all transactions, should be immutable - Fact Table';
COMMENT ON TABLE clients IS 'Table to keep track of dask client IPs and which provider is using which - Dimension Table';
