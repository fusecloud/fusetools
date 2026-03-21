"""Database connections and ETL operations."""

from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any, Optional

# MARK: - Private Helpers


def _dump_sql(obj: str, filepath: str) -> None:
    with open(filepath, "w") as f:
        f.write(obj)


# ═══════════════════════════════════════════════════════════
# MARK: - Connections
# ═══════════════════════════════════════════════════════════


# MARK: - Oracle Connections
class Oracle:
    """Oracle database connection helpers."""

    @classmethod
    def eng_oracle(cls, usr: str, pwd: str, dbname: str) -> Any:
        """
        Creates a SQLAlchemy engine for an Oracle database.

        :param usr: Oracle database username.
        :param pwd: Oracle database password.
        :param dbname: Oracle database name.
        :return: SQLAlchemy engine object.
        """
        from sqlalchemy import create_engine

        eng = create_engine("oracle://" + usr + ":" + pwd + f"@{dbname}", encoding="utf-8", echo=False)
        return eng

    @classmethod
    def eng_oracle_addr(cls, usr: str, pwd: str, host: str, port: str, dbname: str) -> Any:
        """
        Creates a SQLAlchemy engine for an Oracle database using address details.

        :param usr: Oracle database username.
        :param pwd: Oracle database password.
        :param host: Oracle database host.
        :param port: Oracle database port.
        :param dbname: Oracle database service name.
        :return: SQLAlchemy engine object.
        """
        from sqlalchemy import create_engine

        eng = create_engine(f"""oracle+cx_oracle://{usr}:{pwd}@(DESCRIPTION =(SOURCE_ROUTE = YES)(ADDRESS_LIST =
            (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))
            (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))
            (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))
            (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))
            (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))
            (CONNECT_DATA =(SERVICE_NAME = {dbname})))""")
        return eng

    @classmethod
    def con_oracle(cls, usr: str, pwd: str, dbname: str) -> Any:
        """
        Creates a cx_Oracle connection to an Oracle database.

        :param usr: Oracle database username.
        :param pwd: Oracle database password.
        :param dbname: Oracle database name.
        :return: cx_Oracle connection object.
        """
        import cx_Oracle

        conn = cx_Oracle.connect(usr + "/" + pwd + f"@{dbname}", encoding="utf-8")
        return conn


# MARK: - Postgres Connections
class Postgres:
    """Postgres database connection helpers."""

    @classmethod
    def eng_postgres(
        cls,
        usr: str,
        pwd: str,
        port: str,
        db: str,
        ssl_str: str = "",
        host: Optional[str] = None,
    ) -> Any:
        """
        Creates a SQLAlchemy engine for a Postgres database.

        :param usr: Postgres database username.
        :param pwd: Postgres database password.
        :param port: Postgres database port.
        :param db: Postgres database name.
        :param ssl_str: SSL connection string.
        :param host: Postgres database host.
        :return: SQLAlchemy engine object.
        """
        from sqlalchemy import create_engine

        host_name = host if host else "localhost"
        engine = create_engine(f"postgresql://{usr}:{pwd}@{host_name}:{port}/{db}{ssl_str}")
        return engine

    @classmethod
    def con_postgres(
        cls,
        host: str,
        db: str,
        usr: str,
        pwd: str,
        port: Optional[int] = None,
        retries: int = 3,
        retry_delay: int = 5,
    ) -> tuple[Any, Any]:
        """
        Creates a psycopg2 connection and cursor to a Postgres database with retry logic.

        :param host: Postgres database host.
        :param db: Postgres database name.
        :param usr: Postgres database username.
        :param pwd: Postgres database password.
        :param port: Postgres database port.
        :param retries: Number of connection retries.
        :param retry_delay: Base delay in seconds between retries (exponential backoff).
        :return: Tuple of (cursor, connection).
        """
        import psycopg2

        last_exc: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                conn = psycopg2.connect(
                    host=host,
                    database=db,
                    user=usr,
                    password=pwd,
                    port=port,
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=60,
                    keepalives_interval=15,
                    keepalives_count=5,
                )
                cursor = conn.cursor()
                return cursor, conn
            except psycopg2.OperationalError as e:
                last_exc = e
                if attempt < retries:
                    wait = retry_delay * (2**attempt)
                    print(f"Postgres connection attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
        raise last_exc


# MARK: - TeraData Connections
class TeraData:
    """Teradata database connection helpers."""

    @classmethod
    def conn_td(cls, host: str) -> Any:
        """
        Creates a Teradata connection via ODBC.

        :param host: Teradata host DSN.
        :return: Teradata connection object.
        """
        import teradata

        udaExec = teradata.UdaExec(appName="test", version="1.0", logConsole=False)
        conn = udaExec.connect(method="odbc", dsn=host, autocommit=True, transactionMode="Teradata")
        return conn

    @classmethod
    def conn_pyodbc(cls, drivername: str, host: str, usr: str, pwd: str) -> Any:
        """
        Creates a pyodbc connection to a Teradata database.

        :param drivername: ODBC driver name.
        :param host: Teradata host.
        :param usr: Database username.
        :param pwd: Database password.
        :return: pyodbc connection object.
        """
        import pyodbc

        link = f"DRIVER={drivername};DBCNAME={host};UID={usr};PWD={pwd}"
        conn = pyodbc.connect(link, autocommit=True)
        return conn

    @classmethod
    def conn_sa(cls, drivername: str, host: str, usr: str, pwd: str) -> Any:
        """
        Creates a SQLAlchemy engine for a Teradata database.

        :param drivername: ODBC driver name.
        :param host: Teradata host.
        :param usr: Database username.
        :param pwd: Database password.
        :return: SQLAlchemy engine object.
        """
        from sqlalchemy import create_engine

        link = f"teradata://{usr}:{pwd}@{host}/?driver={drivername}"
        conn = create_engine(link)
        return conn


# MARK: - Redshift Connections
class Redshift:
    """Redshift database connection helpers."""

    @classmethod
    def conn_rs_sa(cls, db: str, host: str, port: str, user: str, pwd: str) -> tuple[Any, Any]:
        """
        Creates a SQLAlchemy engine and connection for a Redshift database.

        :param db: Redshift database name.
        :param host: Redshift host.
        :param port: Redshift port.
        :param user: Database username.
        :param pwd: Database password.
        :return: Tuple of (engine, connection).
        """
        from sqlalchemy import create_engine

        engine = create_engine(f"postgres://{user}:{pwd}@{host}:{port}/{db}", use_batch_mode=True)
        conn = engine.connect()
        return engine, conn

    @classmethod
    def conn_rs_pg(cls, db: str, host: str, port: str, user: str, pwd: str) -> tuple[Any, Any]:
        """
        Creates a psycopg2 connection and cursor for a Redshift database.

        :param db: Redshift database name.
        :param host: Redshift host.
        :param port: Redshift port.
        :param user: Database username.
        :param pwd: Database password.
        :return: Tuple of (cursor, connection).
        """
        import psycopg2

        conn = psycopg2.connect(f"dbname={db} host={host} port={port} user={user} password={pwd}")
        cursor = conn.cursor()
        return cursor, conn


# MARK: - MySQL Connections
class MySQL:
    """MySQL database connection helpers."""

    @classmethod
    def eng_mysql(cls, usr: str, pwd: str, host: Optional[str] = None, db: Optional[str] = None) -> Any:
        """
        Creates a SQLAlchemy engine for a MySQL database.

        :param usr: MySQL database username.
        :param pwd: MySQL database password.
        :param host: MySQL database host.
        :param db: MySQL database name.
        :return: SQLAlchemy engine object.
        """
        from sqlalchemy import create_engine

        conn_str = f"mysql+pymysql://{usr}:{pwd}@{host if host else 'localhost'}{'/' + db if db else ''}"
        engine = create_engine(conn_str, echo=False)
        return engine


# ═══════════════════════════════════════════════════════════
# MARK: - ETL Operations
# ═══════════════════════════════════════════════════════════


# MARK: - Generic ETL
class Generic:
    """Generic ETL utility functions for database operations."""

    @classmethod
    def make_groupby(cls, sql: str, dim_fact_delim: str) -> str:
        """
        Creates a GROUP BY clause from a SQL statement.

        :param sql: SQL statement.
        :param dim_fact_delim: Delimiter that separates dimension columns from fact columns.
        :return: SQL statement with GROUP BY clause appended.
        """
        dims = sql[: sql.find(dim_fact_delim)]
        dims = dims[dims.find("SELECT") + len("SELECT") :]
        dims = dims.split(",")
        dims = [str(idx + 1) for idx, val in enumerate(dims)]
        dims = ",".join(dims)
        group_by = f"\nGROUP BY {dims}"
        return sql + group_by

    @classmethod
    def make_db_schema(cls, df: Any) -> Any:
        """
        Creates a mapping of Pandas data types to SQL data types.

        :param df: A Pandas DataFrame with column types to be converted.
        :return: A Pandas DataFrame of columns with corresponding SQL data types.
        """
        import numpy as np
        import pandas as pd

        cols = []
        dtypes = []

        for col in df.columns:
            cols.append(col)
            col_series = df[col].replace(r"^\s*$", np.nan, regex=True)
            col_series = col_series.dropna()

            try:
                date_len = max(col_series.astype("str").str[:10].str.split("-").apply(lambda x: len(x)))
                if date_len == 3:
                    dtypes.append("datetime64[ns]")
                    continue
            except:
                date_len = 0

            try:
                if col_series.astype("float").apply(float.is_integer).all():
                    is_int = True
                else:
                    is_int = False
            except:
                dtypes.append("object")
                continue

            if is_int and date_len != 3:
                dtype = "Int64"
            elif not is_int and date_len != 3:
                dtype = "float"
            elif date_len == 3:
                dtype = "datetime64[ns]"
            else:
                dtype = "object"

            dtypes.append(dtype)

        schema_df = pd.DataFrame({"col": cols, "dtype_new": dtypes})
        old_schema_df = pd.DataFrame(df.dtypes, columns=["dtype_old"]).reset_index()
        schema_df2 = pd.merge(schema_df, old_schema_df, how="inner", left_on="col", right_on="index")
        schema_df2["dtype_final"] = np.where(
            schema_df2["dtype_new"] != "object",
            schema_df2["dtype_new"],
            schema_df2["dtype_old"],
        )

        return schema_df2

    @classmethod
    def db_apply_schema(cls, df: Any, schema_df: Any) -> Any:
        """
        Applies a schema DataFrame to a Pandas DataFrame.

        :param df: Pandas DataFrame.
        :param schema_df: Schema DataFrame from make_db_schema.
        :return: DataFrame with applied schema.
        """
        import pandas as pd

        for idx, row in schema_df.iterrows():
            col = row["column"]
            col_type = row["type"]
            if col_type == "VARCHAR":
                df[col] = df[col].astype(str)
            elif col_type == "INTEGER":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif col_type == "FLOAT":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif col_type == "TIMESTAMP":
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    @classmethod
    def make_db_cols(cls, df: Any) -> Any:
        """
        Cleans DataFrame column names for database compatibility.

        :param df: Pandas DataFrame.
        :return: DataFrame with cleaned column names.
        """
        columns = [re.sub("#", "num", col) for col in df.columns]
        columns = [re.sub("%", "pct", col) for col in columns]
        columns = [re.sub("[^a-zA-Z0-9]+", " ", col) for col in columns]
        columns = [col.replace(" ", "_") for col in columns]
        columns = [col[:200] for col in columns]
        columns = [col.lower() for col in columns]
        columns = [c.lstrip("_").rstrip("_") for c in columns]
        df.columns = columns
        return df

    @classmethod
    def run_query(cls, engine: Any, sql: str) -> None:
        """
        Executes a SQL statement using a SQLAlchemy engine.

        :param engine: SQLAlchemy engine object.
        :param sql: SQL statement to execute.
        :return: Elapsed time to execute query.
        """
        rptg_tstart = datetime.now()
        engine.execute(sql)
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(f"Runtime: {tdelta}")


# MARK: - Oracle ETL
class OracleETL:
    """Oracle-specific ETL operations."""

    @classmethod
    def make_tbl(cls, df: Any, tbl_name: str) -> str:
        """
        Creates SQL to run a CREATE TABLE statement based on a Pandas DataFrame.

        :param df: Pandas DataFrame.
        :param tbl_name: Oracle table name.
        :return: CREATE TABLE SQL statement.
        """
        import numpy as np
        import pandas as pd

        df = Generic.make_db_cols(df)

        for idx, col in enumerate(df):
            col_desc = col + "-" + str(df[col].map(lambda x: len(str(x))).max())
            try:
                col_max = col + "-" + str(max(df[col]))
            except:
                col_max = col + "-" + "NA"

            if idx == 0:
                col_desc_all = [col_desc]
                col_max_all = [col_max]
            else:
                col_desc_all.append(col_desc)
                col_max_all.append(col_max)

        col_desc_all = pd.DataFrame(col_desc_all)
        col_desc_all.columns = ["char"]
        col_desc_all["column"], col_desc_all["length"] = col_desc_all["char"].str.split("-", 1).str

        col_max_all = pd.DataFrame(col_max_all)
        col_max_all.columns = ["char"]
        col_max_all["column"], col_max_all["max"] = col_max_all["char"].str.split("-", 1).str

        col_desc_types = pd.DataFrame(df.dtypes).reset_index()
        col_desc_types.columns = ["column", "type"]

        col_desc_all = pd.merge(col_desc_all, col_desc_types, how="inner", on="column")
        col_desc_all = pd.merge(col_desc_all, col_max_all[["column", "max"]], how="inner", on="column")

        d = {
            "object": "VARCHAR2",
            "int64": "NUMBER",
            "Int64": "NUMBER",
            "int32": "NUMBER",
            "bool": "VARCHAR2",
            "float64": "FLOAT",
            "datetime64[ns]": "DATE",
        }

        col_desc_all = col_desc_all.astype(str).replace(d)

        col_desc_all["concat"] = np.where(
            col_desc_all["type"] == "VARCHAR2",
            col_desc_all["column"] + " " + col_desc_all["type"].astype(str) + "(" + col_desc_all["length"] + ")",
            col_desc_all["column"] + " " + col_desc_all["type"].astype(str),
        )

        for idx, row in col_desc_all.iterrows():
            if str(row["type"]) == "NUMBER" and row["max"] != "nan" and int(row["max"]) > 2147483647:
                val = row["concat"]
                col_desc_all.loc[idx, "concat"] = val.replace(" NUMBER", f" VARCHAR2({row['length']})")

        col_desc_all = col_desc_all.apply(", ".join).reset_index()
        col_desc_all.columns = ["index", "statement"]
        statement = col_desc_all[col_desc_all["index"] == "concat"]
        sql = statement["statement"].values
        sql = str(sql)
        sql = sql.replace("[", "").replace("]", "")
        sql = "CREATE TABLE " + tbl_name + " ( " + sql + " )"
        sql = sql.replace("'", "")

        return sql

    @classmethod
    def insert_tbl(cls, df: Any, tbl_name: str) -> str:
        """
        Creates SQL to run an INSERT INTO statement for an Oracle table.

        :param df: Pandas DataFrame.
        :param tbl_name: Oracle table name.
        :return: INSERT INTO SQL statement template.
        """
        df = Generic.make_db_cols(df)
        cols = ", ".join(list(df.columns))
        vals = ", ".join([":" + str(i + 1) for i in range(len(df.columns))])
        sql = f"INSERT INTO {tbl_name} ({cols}) VALUES ({vals})"
        return sql

    @classmethod
    def insert_exec(cls, sql: str, conn: Any, df: Any) -> None:
        """
        Executes an INSERT statement with cx_Oracle executemany.

        :param sql: INSERT SQL statement.
        :param conn: cx_Oracle connection object.
        :param df: Pandas DataFrame to insert.
        """

        cursor = conn.cursor()
        rows = [tuple(x) for x in df.values]
        cursor.executemany(sql, rows)
        conn.commit()

    @classmethod
    def make_tbl_complete_force(
        cls,
        df: Any,
        tbl_name: str,
        eng: Any,
        conn: Any,
        attempt_n: int,
        subcols: Any = False,
        chunks: Any = False,
        chunks_delay: Any = False,
    ) -> None:
        """
        Creates and loads an Oracle table from a DataFrame with forced retries.

        :param df: Pandas DataFrame.
        :param tbl_name: Oracle table name.
        :param eng: SQLAlchemy engine.
        :param conn: cx_Oracle connection.
        :param attempt_n: Number of retry attempts.
        :param subcols: Subset of columns to use.
        :param chunks: Chunk size for batch loading.
        :param chunks_delay: Delay between chunks in seconds.
        """

        for attempt in range(attempt_n):
            try:
                cls.make_tbl_complete(
                    df=df,
                    tbl_name=tbl_name,
                    eng=eng,
                    conn=conn,
                    subcols=subcols,
                    chunks=chunks,
                    chunks_delay=chunks_delay,
                )
                print(f"Successfully loaded {tbl_name} on attempt {attempt + 1}")
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {tbl_name}: {e}")
                if attempt < attempt_n - 1:
                    print("Retrying...")

    @classmethod
    def make_tbl_complete(
        cls,
        df: Any,
        tbl_name: str,
        eng: Any,
        conn: Any,
        subcols: Any = False,
        chunks: Any = False,
        chunks_delay: Any = False,
    ) -> None:
        """
        Creates and loads an Oracle table from a DataFrame.

        :param df: Pandas DataFrame.
        :param tbl_name: Oracle table name.
        :param eng: SQLAlchemy engine.
        :param conn: cx_Oracle connection.
        :param subcols: Subset of columns to use.
        :param chunks: Chunk size for batch loading.
        :param chunks_delay: Delay between chunks in seconds.
        """
        import numpy as np

        # drop existing table
        try:
            Generic.run_query(engine=eng, sql=f"DROP TABLE {tbl_name}")
        except:
            pass

        # subset columns if needed
        if subcols:
            df = df[subcols]

        # create the table
        sql = cls.make_tbl(df=df, tbl_name=tbl_name)
        Generic.run_query(engine=eng, sql=sql)

        # insert data
        insert_sql = cls.insert_tbl(df=df, tbl_name=tbl_name)
        df = df.replace({np.nan: None})

        if chunks:
            chunk_size = chunks
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i : i + chunk_size]
                cls.insert_exec(sql=insert_sql, conn=conn, df=chunk)
                print(f"Loaded chunk {i // chunk_size + 1}")
                if chunks_delay:
                    time.sleep(chunks_delay)
        else:
            cls.insert_exec(sql=insert_sql, conn=conn, df=df)

    @classmethod
    def get_oracle_date(cls, date: str) -> str:
        """
        Converts a date string to Oracle date format.

        :param date: Date string.
        :return: Oracle-formatted date string.
        """
        import pandas as pd

        dt = pd.to_datetime(date)
        return dt.strftime("%d-%b-%Y").upper()

    @classmethod
    def get_orcl_date(cls, dat: str) -> str:
        """
        Creates an Oracle TO_DATE expression from a date string.

        :param dat: Date string in YYYY-MM-DD format.
        :return: Oracle TO_DATE SQL expression.
        """
        return f"TO_DATE('{dat}', 'YYYY-MM-DD')"

    @classmethod
    def orcl_tbl_varchar_convert(cls, tbl_name: str, convert_cols: list[str], engine: Any) -> None:
        """
        Converts columns in an Oracle table to VARCHAR2.

        :param tbl_name: Oracle table name.
        :param convert_cols: List of column names to convert.
        :param engine: SQLAlchemy engine.
        """
        from sqlalchemy import text

        with engine.connect() as conn:
            for col in convert_cols:
                sql = text(f"ALTER TABLE {tbl_name} MODIFY {col} VARCHAR2(4000)")
                conn.execute(sql)


# MARK: - Postgres ETL
class PostgresETL:
    """Postgres-specific ETL operations."""

    @classmethod
    def run_query_pg(cls, conn: Any, sql: str) -> None:
        """
        Executes a SQL statement with a Postgres database connection.

        :param conn: Postgres database connection object.
        :param sql: SQL Statement to execute.
        :return: Elapsed time to execute query.
        """
        rptg_tstart = datetime.now()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(f"Runtime: {tdelta}")

    @classmethod
    def insert_val_pg(cls, col_list: list[str], val_list: list[Any], tbl_name: str) -> str:
        """
        Creates SQL to run an INSERT operation of a given Postgres table.

        :param col_list: List of columns to INSERT or UPDATE.
        :param val_list: List of values to INSERT or UPDATE.
        :param tbl_name: Name of Postgres table.
        :return: SQL to run an INSERT statement.
        """
        sql = f"""
        INSERT INTO {tbl_name}
        (
            {str(col_list).replace("[", "").replace("]", "").replace("'", "")}
        ) values (
            {str(val_list).replace("[", "").replace("]", "")}
        )
        """
        return sql

    @classmethod
    def upsert_val_pg(
        cls,
        col_list: list[str],
        val_list: list[Any],
        tbl_name: str,
        constraint_col: str,
    ) -> str:
        """
        Creates SQL to run an UPSERT (INSERT new records or UPDATE existing records) operation of a given Postgres table.

        :param col_list: List of columns to INSERT or UPDATE.
        :param val_list: List of values to INSERT or UPDATE.
        :param tbl_name: Name of Postgres table.
        :param constraint_col: Column/value logic to check against for INSERT or UPDATE.
        :return: SQL to run an UPSERT statement.
        """
        update = ""
        for idx, col in zip(col_list, val_list):
            if type(col) in [str]:
                update = update + idx + f"='{col}',"
            else:
                update = update + idx + f"={col},"

        update = update[: update.rfind(",")]

        sql = f"""
        INSERT INTO {tbl_name}
        ({str(col_list).replace("[", "").replace("]", "").replace("'", "")})
        VALUES
        ({str(val_list).replace("[", "").replace("]", "")})
        ON CONFLICT ({constraint_col})
        DO
        UPDATE SET
        {update}
        """

        return sql

    @classmethod
    def upsert_tbl_pg(
        cls,
        src_tbl: str,
        tgt_tbl: str,
        src_join_cols: list[str],
        src_insert_cols: list[str],
        src_update_cols: Any = False,
        update_compare_cols: Any = False,
    ) -> tuple[str, str]:
        """
        Creates SQL to run an UPSERT (INSERT new records or UPDATE existing records) operation of a given Postgres table.

        :param src_tbl: Postgres source table that contains data to be merged from.
        :param tgt_tbl: Postgres target table to receive UPSERT operation.
        :param src_join_cols: Columns to use to join source and target tables.
        :param src_insert_cols: Columns to be inserted from source table.
        :param src_update_cols: Columns to be updated from source table.
        :param update_compare_cols: Columns to use to compare values across source and target tables.
        :return: A SQL Insert statement and a SQL Update statement.
        """
        src_join_cols_ = str([f"t.{c} = s.{c} AND " for c in src_join_cols]).replace("[", "").replace("]", "").replace("'", "").replace(",", "")

        src_join_cols_ = src_join_cols_[: src_join_cols_.rfind("AND")]

        src_join_cols_f = str([f"t.{c} IS NULL AND " for c in src_join_cols]).replace("[", "").replace("]", "").replace("'", "").replace(",", "")

        src_join_cols_f = src_join_cols_f[: src_join_cols_f.rfind("AND")]

        src_insert_cols_ = str([f"s.{c}" for c in src_insert_cols]).replace("[", "").replace("]", "").replace("'", "")

        if src_update_cols:
            src_update_cols_ = str([f"{c} = s.{c}," for c in src_update_cols]).replace("[", "").replace("]", "").replace("', '", "").replace("'", "")

            src_update_cols_ = src_update_cols_[: src_update_cols_.rfind(",")]

            # update join statement
            src_join_cols2_ = src_join_cols_.replace("t.", f"{tgt_tbl}.")
            if update_compare_cols:
                update_compare_cols_ = (
                    str([f"s.{c} != {tgt_tbl}.{c}," for c in update_compare_cols])
                    .replace("[", "")
                    .replace("]", "")
                    .replace("', '", "")
                    .replace("'", "")
                )

                update_compare_cols_ = update_compare_cols_[: update_compare_cols_.rfind(",")]
                src_join_cols2_ = src_join_cols2_ + " AND " + update_compare_cols_

            sql_update = f"""
                /* Update records*/
                UPDATE {tgt_tbl}
                SET {src_update_cols_}
                FROM {src_tbl} s
                WHERE {src_join_cols2_}
                """.replace("\n", " ")
        else:
            sql_update = ""

        sql_insert = f"""
            /* Insert records*/
            INSERT INTO {tgt_tbl}
            SELECT {src_insert_cols_}
            FROM {src_tbl} s
            LEFT JOIN {tgt_tbl} t
            ON {src_join_cols_}
            WHERE {src_join_cols_f}
            """.replace("\n", " ")

        return sql_update, sql_insert

    @classmethod
    def upsert_df_pg(
        cls,
        df: Any,
        tbl_name: str,
        conn: Any,
        cursor: Any,
        constraint_col: str,
        schema_name: Optional[str] = None,
    ) -> None:
        """
        Upserts a DataFrame into a Postgres table.

        :param df: Pandas DataFrame.
        :param tbl_name: Postgres table name.
        :param conn: Postgres database connection object.
        :param cursor: Postgres database cursor object.
        :param constraint_col: Column to use for conflict resolution.
        :param schema_name: Schema name.
        """
        import numpy as np

        full_tbl = f"{schema_name}.{tbl_name}" if schema_name else tbl_name
        df_load = df.replace({np.nan: None})
        df_columns = list(df_load)
        columns = ",".join(df_columns)
        values = ",".join(["%s" for _ in df_columns])
        update_cols = ",".join([f"{c}=EXCLUDED.{c}" for c in df_columns if c != constraint_col])

        insert_stmt = f"INSERT INTO {full_tbl} ({columns}) VALUES ({values}) ON CONFLICT ({constraint_col}) DO UPDATE SET {update_cols}"

        rptg_tstart = datetime.now()
        import psycopg2.extras

        psycopg2.extras.execute_batch(cursor, insert_stmt, df_load.values)
        conn.commit()
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(f"Runtime: {tdelta}")

    @classmethod
    def insert_df_pg(
        cls,
        df: Any,
        tbl_name: str,
        conn: Any,
        cursor: Any,
        insert_type: str = "fast",
        schema_name: Optional[str] = None,
    ) -> None:
        """
        Inserts a DataFrame into a Postgres table.

        :param df: Pandas DataFrame.
        :param tbl_name: Postgres table name.
        :param conn: Postgres database connection object.
        :param cursor: Postgres database cursor object.
        :param insert_type: Insert method ('fast' uses execute_batch).
        :param schema_name: Schema name.
        """
        import numpy as np
        import psycopg2.extras

        full_tbl = f"{schema_name}.{tbl_name}" if schema_name else tbl_name
        df_load = df.replace({np.nan: None})
        df_load = df_load.round(3)
        df_columns = list(df_load)
        columns = ",".join(df_columns)
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(full_tbl, columns, values)
        rptg_tstart = datetime.now()
        psycopg2.extras.execute_batch(cursor, insert_stmt, df_load.values)
        conn.commit()
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(f"Runtime: {tdelta}")

    @classmethod
    def read_pg(cls, sql: str, conn: Any) -> Any:
        """
        Reads a SQL query result into a Pandas DataFrame.

        :param sql: SQL query string.
        :param conn: Postgres database connection object.
        :return: Pandas DataFrame.
        """
        import pandas as pd

        df = pd.read_sql_query(sql=sql, con=conn)
        return df

    @classmethod
    def read_pg_batches(cls, sql: str, conn: Any, batch_size: int = 10000) -> Any:
        """
        Reads a SQL query result into a Pandas DataFrame in batches.

        :param sql: SQL query string.
        :param conn: Postgres database connection object.
        :param batch_size: Number of rows per batch.
        :return: Pandas DataFrame.
        """
        import pandas as pd

        chunks = pd.read_sql_query(sql=sql, con=conn, chunksize=batch_size)
        df = pd.concat(chunks, ignore_index=True)
        return df

    @classmethod
    def create_tbl_pg(
        cls,
        df: Any,
        tbl_name: str,
        engine: Any,
        schema_name: Any = False,
        if_exists: str = "fail",
        dtype: Any = None,
    ) -> None:
        """
        Creates a Postgres table from a DataFrame using SQLAlchemy.

        :param df: Pandas DataFrame.
        :param tbl_name: Postgres table name.
        :param engine: SQLAlchemy engine.
        :param schema_name: Schema name.
        :param if_exists: What to do if table exists ('fail', 'replace', 'append').
        :param dtype: Column data types dict.
        """
        schema = schema_name if schema_name else None
        df.to_sql(
            tbl_name,
            engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            dtype=dtype,
        )

    @classmethod
    def delete_from_pg(
        cls,
        tbl_name: str,
        conn: Any,
        cursor: Any,
        date_col: Optional[str] = None,
        date_val: Optional[str] = None,
    ) -> None:
        """
        Deletes records from a Postgres table.

        :param tbl_name: Postgres table name.
        :param conn: Postgres database connection object.
        :param cursor: Postgres database cursor object.
        :param date_col: Date column to filter on.
        :param date_val: Date value to filter on.
        """
        if date_col and date_val:
            sql = f"DELETE FROM {tbl_name} WHERE {date_col} = '{date_val}'"
        else:
            sql = f"DELETE FROM {tbl_name}"
        cls.run_query_pg(conn=conn, sql=sql)

    @classmethod
    def drop_tbl_pg(cls, tbl_name: str, conn: Any, cursor: Any) -> None:
        """
        Drops a Postgres table.

        :param tbl_name: Postgres table name.
        :param conn: Postgres database connection object.
        :param cursor: Postgres database cursor object.
        """
        cls.run_query_pg(conn=conn, sql=f"DROP TABLE IF EXISTS {tbl_name}")

    @classmethod
    def refresh_materialized_view_pg(cls, view_name: str, conn: Any, cursor: Any) -> None:
        """
        Refreshes a materialized view in Postgres.

        :param view_name: Materialized view name.
        :param conn: Postgres database connection object.
        :param cursor: Postgres database cursor object.
        """
        cls.run_query_pg(conn=conn, sql=f"REFRESH MATERIALIZED VIEW {view_name}")

    @classmethod
    def truncate_tbl_pg(cls, tbl_name: str, conn: Any, cursor: Any) -> None:
        """
        Truncates a Postgres table.

        :param tbl_name: Postgres table name.
        :param conn: Postgres database connection object.
        :param cursor: Postgres database cursor object.
        """
        cls.run_query_pg(conn=conn, sql=f"TRUNCATE TABLE {tbl_name}")

    @classmethod
    def add_primary_key_pg(cls, tbl_name: str, key_cols: list[str], conn: Any, cursor: Any) -> None:
        """
        Adds a primary key to a Postgres table.

        :param tbl_name: Postgres table name.
        :param key_cols: List of column names for the primary key.
        :param conn: Postgres database connection object.
        :param cursor: Postgres database cursor object.
        """
        cols = ", ".join(key_cols)
        constraint_name = f"{tbl_name}_pkey"
        sql = f"ALTER TABLE {tbl_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({cols})"
        cls.run_query_pg(conn=conn, sql=sql)

    @classmethod
    def add_index_pg(
        cls,
        tbl_name: str,
        key_cols: list[str],
        conn: Any,
        cursor: Any,
        schema_name: Optional[str] = None,
    ) -> None:
        """
        Adds an index to a Postgres table.

        :param tbl_name: Postgres table name.
        :param key_cols: List of column names for the index.
        :param conn: Postgres database connection object.
        :param cursor: Postgres database cursor object.
        :param schema_name: Schema name.
        """
        cols = ", ".join(key_cols)
        full_tbl = f"{schema_name}.{tbl_name}" if schema_name else tbl_name
        idx_name = f"idx_{tbl_name}_{'_'.join(key_cols)}"
        sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {full_tbl} ({cols})"
        cls.run_query_pg(conn=conn, sql=sql)

    @classmethod
    def make_df_tbl_pg(cls, tbl_name: str, df: Any) -> str:
        """
        Creates SQL to run a CREATE TABLE statement based on a Pandas DataFrame.

        :param tbl_name: Postgres table name.
        :param df: Pandas DataFrame.
        :return: CREATE TABLE SQL statement.
        """
        import numpy as np
        import pandas as pd

        # fix columns
        df = Generic.make_db_cols(df)

        # loop thru the columns
        for idx, col in enumerate(df):
            col_desc = col + "-" + str(df[col].map(lambda x: len(str(x))).max())
            try:
                col_max = col + "-" + str(max(df[col]))
            except:
                col_max = col + "-" + "NA"

            if idx == 0:
                col_desc_all = [col_desc]
                col_max_all = [col_max]
            else:
                col_desc_all.append(col_desc)
                col_max_all.append(col_max)

        col_desc_all = pd.DataFrame(col_desc_all)
        col_desc_all.columns = ["char"]
        col_desc_all["column"], col_desc_all["length"] = col_desc_all["char"].str.split("-", 1).str

        col_max_all = pd.DataFrame(col_max_all)
        col_max_all.columns = ["char"]
        col_max_all["column"], col_max_all["max"] = col_max_all["char"].str.split("-", 1).str

        col_desc_types = pd.DataFrame(df.dtypes).reset_index()
        col_desc_types.columns = ["column", "type"]

        col_desc_all = pd.merge(col_desc_all, col_desc_types, how="inner", on="column")
        col_desc_all = pd.merge(col_desc_all, col_max_all[["column", "max"]], how="inner", on="column")

        d = {
            "object": "VARCHAR",
            "int64": "INTEGER",
            "Int64": "INTEGER",
            "int32": "INTEGER",
            "bool": "VARCHAR",
            "float64": "FLOAT",
            "datetime64[ns]": "TIMESTAMP",
            "datetime64[ns, UTC]": "TIMESTAMP",
        }

        col_desc_all = col_desc_all.astype(str).replace(d)

        col_desc_all["concat"] = np.where(
            col_desc_all["type"] == "VARCHAR",
            col_desc_all["column"] + " " + col_desc_all["type"].astype(str) + "(" + col_desc_all["length"] + ")",
            col_desc_all["column"] + " " + col_desc_all["type"].astype(str),
        )

        for idx, row in col_desc_all.iterrows():
            if str(row["type"]) == "INTEGER" and row["max"] != "nan" and int(row["max"]) > 2147483647:
                val = row["concat"]
                col_desc_all.loc[idx, "concat"] = val.replace(" INTEGER", f" VARCHAR({row['length']})")

        col_desc_all = col_desc_all.apply(", ".join).reset_index()
        col_desc_all.columns = ["index", "statement"]
        statement = col_desc_all[col_desc_all["index"] == "concat"]
        sql = statement["statement"].values
        sql = str(sql)
        sql = sql.replace("[", "")
        sql = sql.replace("]", "")
        sql = "CREATE TABLE " + tbl_name + " ( " + sql + " )"
        sql = sql.replace("'", "")

        return sql

    @classmethod
    def insert_pg(
        cls,
        cursor: Any,
        conn: Any,
        df: Any,
        tbl_name: str,
        return_statement: Optional[str] = None,
    ) -> Any:
        """
        Executes an INSERT INTO statement for a given Pandas DataFrame into a Postgres table.

        :param cursor: Postgres database cursor object.
        :param conn: Postgres database connection object.
        :param df: Pandas DataFrame to insert into a Postgres table.
        :param tbl_name: Postgres table name.
        :param return_statement: Optional RETURNING clause.
        :return: Elapsed time to execute query, or returned result if return_statement is given.
        """
        import numpy as np
        import psycopg2.extras

        df_load = df.replace({np.nan: None})
        df_load = df_load.round(3)
        df_columns = list(df_load)
        columns = ",".join(df_columns)
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(tbl_name, columns, values)
        if return_statement:
            insert_stmt = insert_stmt + return_statement
        rptg_tstart = datetime.now()
        psycopg2.extras.execute_batch(cursor, insert_stmt, df_load.values)

        conn.commit()
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60

        if return_statement:
            res = cursor.fetchone()
            return res

        print(f"Runtime: {tdelta}")

    @classmethod
    def make_tbl_complete_pg(cls, df: Any, tbl_name: str, conn: Any, cursor: Any, batch_size: Any = False) -> None:
        """
        Executes a series of SQL statements to CREATE and INSERT into a table from a Pandas DataFrame.

        :param df: Pandas DataFrame to create a table from.
        :param tbl_name: Name of table to be created.
        :param conn: Postgres database connection object.
        :param cursor: Postgres database cursor object.
        :param batch_size: Records to load per batch.
        :return: Elapsed time to execute query.
        """
        # 1 drop the table
        print(f"dropping table: {tbl_name}")
        try:
            cls.run_query_pg(sql=f"drop table {tbl_name}", conn=conn)
        except:
            print(f"table doesn't exist: {tbl_name}")
            pass
        # create the table
        print(f"creating table: {tbl_name}")
        sql = cls.make_df_tbl_pg(df=df, tbl_name=tbl_name)
        print(sql)
        cls.run_query_pg(sql=sql, conn=conn)

        print(f"inserting DF values into table: {tbl_name}")
        rptg_tstart = datetime.now()
        cls.insert_pg(df=df, tbl_name=tbl_name, cursor=cursor, conn=conn, batch_size=batch_size)
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(f"Runtime: {tdelta}")

    @classmethod
    def sequential_load_pg(
        cls,
        override: bool,
        tgt_tbl: str,
        conn: Any,
        dt_start: str,
        dt_end: str,
        saved_day_id_range_placeholder: str,
        dt1_interval: str,
        dt2_interval: str,
        sql_loop_fn: Any,
        sql_loop_fn_type: str,
        filter_day_id_field1: Any = False,
        sql_loop_fn_dt_placeholder1: Any = False,
        filter_day_id_field2: Any = False,
        filter_id_type2: Any = False,
        sql_loop_fn_dt_placeholder2: Any = False,
        filter_day_id_field3: Any = False,
        filter_id_type3: Any = False,
        sql_loop_fn_dt_placeholder3: Any = False,
        loop_src1: Any = False,
        loop_src2: Any = False,
        loop_src3: Any = False,
        log_dir: Any = False,
    ) -> None:
        """
        Sequentially loads data into a Postgres table using date-based SQL looping.

        :param override: Whether to drop and recreate the target table.
        :param tgt_tbl: Target table name.
        :param conn: Postgres database connection object.
        :param dt_start: Start date string.
        :param dt_end: End date string.
        :param saved_day_id_range_placeholder: Placeholder string for date range in SQL.
        :param dt1_interval: Primary date interval (e.g., 'MS' for month start).
        :param dt2_interval: Secondary date interval for weekly fallback.
        :param sql_loop_fn: SQL template string or callable.
        :param sql_loop_fn_type: 'fn' if sql_loop_fn is callable, otherwise string replacement.
        :param filter_day_id_field1: First date filter field.
        :param sql_loop_fn_dt_placeholder1: Placeholder for first date filter.
        :param filter_day_id_field2: Second date filter field.
        :param filter_id_type2: Type of second filter ('range' or '<').
        :param sql_loop_fn_dt_placeholder2: Placeholder for second date filter.
        :param filter_day_id_field3: Third date filter field.
        :param filter_id_type3: Type of third filter ('range' or '<').
        :param sql_loop_fn_dt_placeholder3: Placeholder for third date filter.
        :param loop_src1: First source parameter for callable sql_loop_fn.
        :param loop_src2: Second source parameter for callable sql_loop_fn.
        :param loop_src3: Third source parameter for callable sql_loop_fn.
        :param log_dir: Directory to log SQL statements.
        """
        import pandas as pd

        # define the month start/end dates to loop through
        rptg_dates = pd.date_range(dt_start, dt_end, freq=dt1_interval) - pd.offsets.MonthBegin(1)
        rptg_dates = [str(x)[:10] for x in rptg_dates.to_list()]
        rptg_dates = pd.DataFrame({"start_date": rptg_dates, "end_date": rptg_dates})
        rptg_dates["end_date"] = rptg_dates["end_date"].shift(-1)
        rptg_dates = rptg_dates[pd.to_datetime(rptg_dates["start_date"]) <= datetime.now()].dropna()

        # define the weekly start/end dates to loop thru
        rptg_dates_wk = pd.date_range(dt_start, dt_end, freq=dt2_interval)
        rptg_dates_wk = [str(x)[:10] for x in rptg_dates_wk.to_list()]
        rptg_dates_wk = pd.DataFrame({"start_date": rptg_dates_wk, "end_date": rptg_dates_wk})
        rptg_dates_wk["end_date"] = rptg_dates_wk["end_date"].shift(-1)
        rptg_dates_wk = rptg_dates_wk[pd.to_datetime(rptg_dates_wk["start_date"]) <= datetime.now()].dropna()

        # dropping table if override = True
        if override:
            print(f"""table override True: Dropping table: {tgt_tbl} """)
            try:
                cls.run_query_pg(conn=conn, sql=f"""drop table {tgt_tbl}""")
            except:
                conn.commit()
                pass

        # getting max day id value
        try:
            sql = f"""select max(date(trim(substring(dt_range,regexp_instr(dt_range,'to ')+3,10)))) as day_idnt FROM {tgt_tbl}"""
            saved_dates = pd.read_sql_query(sql=sql, con=conn)
        except:
            conn.commit()
            saved_dates = pd.DataFrame({"day_idnt": ["1999-12-31"]})  # arbitrarily old date

        saved_date_dt = (
            datetime(
                year=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[0]),
                month=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[1]),
                day=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[2]),
            )
            .replace(day=1)
            .strftime("%Y-%m-%d")
        )

        rptg_dates = rptg_dates[pd.to_datetime(rptg_dates["start_date"]) >= pd.to_datetime(saved_date_dt)].reset_index(drop=True)

        print("Starting load from:")
        print(rptg_dates.head(1))

        rptg_freq = "M"

        for idx, row in rptg_dates.iterrows():
            print(f"""{row["start_date"]} to {row["end_date"]}""")

            if idx == 0 and saved_dates["day_idnt"][0] != pd.to_datetime(row["start_date"]):
                print(f"""latest saved data date in table is {str(saved_dates["day_idnt"][0])} ...""")
                # bump up start range:
                new_start = str(pd.to_datetime(str(saved_dates["day_idnt"][0])) + pd.DateOffset(1))[:10]
                print(f"""revising start date to: {new_start} to {row["end_date"]}""")

                # if its a function, pass in params
                if sql_loop_fn_type == "fn":
                    sql = sql_loop_fn(
                        src=loop_src1,
                        src2=loop_src2,
                        src3=loop_src3,
                        start=new_start,
                        end=row["end_date"],
                    )

                # otherwise, we will just replace strings
                else:
                    # date range column for logging
                    sql = sql_loop_fn.replace(
                        saved_day_id_range_placeholder,
                        f" '{new_start} to {row['end_date']}' as dt_range,",
                    )

                    # date filters
                    sql = sql.replace(
                        sql_loop_fn_dt_placeholder1,
                        f" AND date({filter_day_id_field1}) >= '{new_start}' AND date({filter_day_id_field1}) < '{row['end_date']}'",
                    )

                    # check for other date fields
                    if sql_loop_fn_dt_placeholder2:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{new_start}' AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )
                        elif filter_id_type2 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )

                    if sql_loop_fn_dt_placeholder3:
                        if filter_day_id_field3 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{new_start}' AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )
                        elif filter_id_type3 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )

            else:
                if sql_loop_fn_type == "fn":
                    sql = sql_loop_fn(
                        start=row["start_date"],
                        end=row["end_date"],
                        src=loop_src1,
                        src2=loop_src2,
                        src3=loop_src3,
                    )
                else:
                    # date range column for logging
                    sql = sql_loop_fn.replace(
                        saved_day_id_range_placeholder,
                        f" '{row['start_date']} to {row['end_date']}' as dt_range,",
                    )

                    # date range column for logging
                    sql = sql.replace(
                        saved_day_id_range_placeholder,
                        f" '{row['start_date']} to {row['end_date']}' as dt_range,",
                    )

                    sql = sql.replace(
                        sql_loop_fn_dt_placeholder1,
                        f" AND date({filter_day_id_field1}) >= '{row['start_date']}' AND date({filter_day_id_field1}) < '{row['end_date']}'",
                    )

                    # check for other date fields
                    if sql_loop_fn_dt_placeholder2:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{row['start_date']}' AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )

                        elif filter_id_type2 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )

                    if sql_loop_fn_dt_placeholder3:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{row['start_date']}' AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )

                        elif filter_id_type3 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )

            if idx == 0 and override:
                sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
            else:
                sql_prefix = f"INSERT INTO {tgt_tbl} "

            _dump_sql(obj=sql_prefix + sql, filepath=log_dir + f"{tgt_tbl}_{idx}.sql")

            try:
                cls.run_query_pg(conn=conn, sql=sql_prefix + sql)
            except Exception as e:
                print(str(e))
                rptg_freq = "W"
                conn.commit()
                break

        # if the insert failed on a monthly level, cycle down to weekly level
        if rptg_freq == "W":
            print("Insert failed on monthly level...cycling down to weekly")
            # getting max day id value
            try:
                sql = f"""select max(date(trim(substring(dt_range,regexp_instr(dt_range,'to ')+3,10)))) as day_idnt FROM {tgt_tbl}"""
                saved_dates = pd.read_sql_query(sql=sql, con=conn)
            except:
                conn.commit()
                saved_dates = pd.DataFrame({"day_idnt": ["1999-12-31"]})  # arbitrarily old date

            saved_date_dt = (
                datetime(
                    year=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[0]),
                    month=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[1]),
                    day=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[2]),
                )
                .replace(day=1)
                .strftime("%Y-%m-%d")
            )

            rptg_dates_wk = rptg_dates_wk[pd.to_datetime(rptg_dates_wk["start_date"]) >= pd.to_datetime(saved_date_dt)].reset_index(drop=True)

            for idx, row in rptg_dates_wk.iterrows():
                print(f"""{row["start_date"]} to {row["end_date"]}""")

                if idx == 0 and saved_dates["day_idnt"][0] != pd.to_datetime(row["start_date"]):
                    print(f"""latest saved data date in table is {str(saved_dates["day_idnt"][0])} ...""")
                    # bump up start range:
                    new_start = str(pd.to_datetime(str(saved_dates["day_idnt"][0])) + pd.DateOffset(1))[:10]
                    print(f"""revising start date to: {new_start} to {row["end_date"]}""")

                    if sql_loop_fn_type == "fn":
                        sql = sql_loop_fn(
                            src=loop_src1,
                            src2=loop_src2,
                            src3=loop_src3,
                            start=new_start,
                            end=row["end_date"],
                        )
                    else:
                        # date range column for logging
                        sql = sql_loop_fn.replace(
                            saved_day_id_range_placeholder,
                            f" '{new_start} to {row['end_date']}' as dt_range,",
                        )

                        sql = sql.replace(
                            sql_loop_fn_dt_placeholder1,
                            f" AND date({filter_day_id_field1}) >= '{new_start}' AND date({filter_day_id_field1}) < '{row['end_date']}'",
                        )

                        # check for other date fields
                        if sql_loop_fn_dt_placeholder2:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{new_start}' AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )

                        if sql_loop_fn_dt_placeholder3:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{new_start}' AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )

                else:
                    if sql_loop_fn_type == "fn":
                        sql = sql_loop_fn(
                            start=row["start_date"],
                            end=row["end_date"],
                            src=loop_src1,
                            src2=loop_src2,
                            src3=loop_src3,
                        )
                    else:
                        # date range column for logging
                        sql = sql_loop_fn.replace(
                            saved_day_id_range_placeholder,
                            f" '{row['start_date']} to {row['end_date']}' as dt_range,",
                        )

                        sql = sql.replace(
                            sql_loop_fn_dt_placeholder1,
                            f" AND date({filter_day_id_field1}) >= '{row['start_date']}' AND date({filter_day_id_field1}) < '{row['end_date']}'",
                        )

                        # check for other date fields
                        if sql_loop_fn_dt_placeholder2:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{row['start_date']}' AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )

                        if sql_loop_fn_dt_placeholder3:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{row['start_date']}' AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )

                if idx == 0 and override:
                    sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
                else:
                    sql_prefix = f"INSERT INTO {tgt_tbl} "

                _dump_sql(obj=sql_prefix + sql, filepath=log_dir + f"{tgt_tbl}_{idx}.sql")

                cls.run_query_pg(conn=conn, sql=sql_prefix + sql)

    @classmethod
    def sequential_load_pg_wk(
        cls,
        rptg_dates: Any,
        override: bool,
        tgt_tbl: str,
        conn: Any,
        rptg_wk: str,
        rptg_wk_start: str,
        rptg_wk_end: str,
        sql_loop_fn: str,
        # filter dates set 1
        filter_dt_field1: Any = False,
        filter_dt_type1: Any = False,
        filter_dt_placeholder1: Any = False,
        # filter dates set 2
        filter_dt_field2: Any = False,
        filter_dt_type2: Any = False,
        filter_dt_placeholder2: Any = False,
        # filter dates set 3
        filter_dt_field3: Any = False,
        filter_dt_type3: Any = False,
        filter_dt_placeholder3: Any = False,
        log_dir: Any = False,
    ) -> None:
        """
        Sequentially loads data into a Postgres table using weekly date-based SQL looping.

        :param rptg_dates: DataFrame with start_date, end_date, rptg_wk columns.
        :param override: Whether to drop and recreate the target table.
        :param tgt_tbl: Target table name.
        :param conn: Postgres database connection object.
        :param rptg_wk: Placeholder for rptg_wk in SQL.
        :param rptg_wk_start: Placeholder for rptg_wk_start in SQL.
        :param rptg_wk_end: Placeholder for rptg_wk_end in SQL.
        :param sql_loop_fn: SQL template string.
        :param filter_dt_field1: First date filter field.
        :param filter_dt_type1: Type of first filter.
        :param filter_dt_placeholder1: Placeholder for first date filter.
        :param filter_dt_field2: Second date filter field.
        :param filter_dt_type2: Type of second filter.
        :param filter_dt_placeholder2: Placeholder for second date filter.
        :param filter_dt_field3: Third date filter field.
        :param filter_dt_type3: Type of third filter.
        :param filter_dt_placeholder3: Placeholder for third date filter.
        :param log_dir: Directory to log SQL statements.
        """
        # dropping table if override = True
        if override:
            print(f"""table override True: Dropping table: {tgt_tbl} """)
            try:
                cls.run_query_pg(conn=conn, sql=f"""drop table {tgt_tbl}""")
            except:
                conn.commit()
                pass

        for idx, row in rptg_dates.iterrows():
            print(f"""{row["start_date"]} to {row["end_date"]}""")

            # date range column for logging
            sql = sql_loop_fn.replace(rptg_wk, f" '{row['rptg_wk']}' as rptg_wk,")

            sql = sql.replace(rptg_wk_start, f" '{row['start_date']}' as rptg_wk_start,")

            sql = sql.replace(rptg_wk_end, f" '{row['end_date']}' as rptg_wk_end,")

            # date filters
            sql = sql.replace(
                filter_dt_placeholder1,
                f" AND date({filter_dt_field1}) > '{row['start_date']}'  AND date({filter_dt_field1}) <= '{row['end_date']}'",
            )

            # check for other date fields
            if filter_dt_placeholder2:
                if filter_dt_type2 == "range":
                    sql = sql.replace(
                        filter_dt_placeholder2,
                        f" AND date({filter_dt_field2}) > '{row['start_date']}'  AND date({filter_dt_field2}) <= '{row['end_date']}'",
                    )
                elif filter_dt_type2 == "<=":
                    sql = sql.replace(
                        filter_dt_placeholder2,
                        f" AND date({filter_dt_field2}) <= '{row['end_date']}'",
                    )

            if filter_dt_placeholder3:
                if filter_dt_type3 == "range":
                    sql = sql.replace(
                        filter_dt_placeholder3,
                        f" AND date({filter_dt_field3}) > '{row['start_date']}'  AND date({filter_dt_field3}) <= '{row['end_date']}'",
                    )
                elif filter_dt_type3 == "<=":
                    sql = sql.replace(
                        filter_dt_placeholder3,
                        f" AND date({filter_dt_field3}) <= '{row['end_date']}'",
                    )

            if idx == 0 and override:
                sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
            else:
                sql_prefix = f"INSERT INTO {tgt_tbl} "

            _dump_sql(obj=sql_prefix + sql, filepath=log_dir + f"{tgt_tbl}_{idx}.sql")

            try:
                cls.run_query_pg(conn=conn, sql=sql_prefix + sql)
            except Exception as e:
                print(str(e))
                conn.commit()
                break

    @classmethod
    def get_pg_columns(cls, tbl_name: str, conn: Any, schema_name: str = "public") -> Any:
        """
        Gets the column names and types for a Postgres table.

        :param tbl_name: Postgres table name.
        :param conn: Postgres database connection object.
        :param schema_name: Schema name.
        :return: Pandas DataFrame with column information.
        """
        import pandas as pd

        sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
        AND table_name = '{tbl_name}'
        ORDER BY ordinal_position
        """
        return pd.read_sql_query(sql=sql, con=conn)

    @classmethod
    def get_pg_tbls(cls, conn: Any, schema_name: str = "public") -> Any:
        """
        Gets all table names in a Postgres schema.

        :param conn: Postgres database connection object.
        :param schema_name: Schema name.
        :return: Pandas DataFrame with table names.
        """
        import pandas as pd

        sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
        ORDER BY table_name
        """
        return pd.read_sql_query(sql=sql, con=conn)

    @classmethod
    def alter_column_pg(cls, tbl_name: str, col_name: str, col_type: str, conn: Any, cursor: Any) -> None:
        """
        Alters a column type in a Postgres table.

        :param tbl_name: Postgres table name.
        :param col_name: Column name to alter.
        :param col_type: New column type.
        :param conn: Postgres database connection object.
        :param cursor: Postgres database cursor object.
        """
        sql = f"ALTER TABLE {tbl_name} ALTER COLUMN {col_name} TYPE {col_type}"
        cls.run_query_pg(conn=conn, sql=sql)


# MARK: - Redshift ETL
class RedshiftETL:
    """Redshift-specific ETL operations."""

    @classmethod
    def run_query_rs(cls, conn: Any, sql: str) -> None:
        """
        Executes a SQL statement with a Redshift database connection.

        :param conn: Redshift database connection object.
        :param sql: SQL Statement to execute.
        :return: Elapsed time to execute query.
        """
        rptg_tstart = datetime.now()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(f"Runtime: {tdelta}")

    @classmethod
    def insert_val_rs(cls, col_list: list[str], val_list: list[Any], tbl_name: str) -> str:
        """
        Creates SQL to run an INSERT operation of a given Redshift table.

        :param col_list: List of columns to INSERT or UPDATE.
        :param val_list: List of values to INSERT or UPDATE.
        :param tbl_name: Name of Redshift table.
        :return: SQL to run an INSERT statement.
        """
        sql = f"""
        INSERT INTO {tbl_name}
        (
            {str(col_list).replace("[", "").replace("]", "").replace("'", "")}
        ) values (
            {str(val_list).replace("[", "").replace("]", "")}
        )
        """
        return sql

    @classmethod
    def upsert_tbl_rs(
        cls,
        src_tbl: str,
        tgt_tbl: str,
        src_join_cols: list[str],
        src_insert_cols: list[str],
        src_update_cols: Any = False,
        update_compare_cols: Any = False,
    ) -> tuple[str, str]:
        """
        Creates SQL to run an UPSERT (INSERT new records or UPDATE existing records) operation of a given Redshift table.

        :param src_tbl: Redshift source table that contains data to be merged from.
        :param tgt_tbl: Redshift target table to receive UPSERT operation.
        :param src_join_cols: Columns to use to join source and target tables.
        :param src_insert_cols: Columns to be inserted from source table.
        :param src_update_cols: Columns to be updated from source table.
        :param update_compare_cols: Columns to use to compare values across source and target tables.
        :return: A SQL Insert statement and a SQL Update statement.
        """
        src_join_cols_ = str([f"t.{c} = s.{c} AND " for c in src_join_cols]).replace("[", "").replace("]", "").replace("'", "").replace(",", "")

        src_join_cols_ = src_join_cols_[: src_join_cols_.rfind("AND")]

        src_join_cols_f = str([f"t.{c} IS NULL AND " for c in src_join_cols]).replace("[", "").replace("]", "").replace("'", "").replace(",", "")

        src_join_cols_f = src_join_cols_f[: src_join_cols_f.rfind("AND")]

        src_insert_cols_ = str([f"s.{c}" for c in src_insert_cols]).replace("[", "").replace("]", "").replace("'", "")

        if src_update_cols:
            src_update_cols_ = str([f"{c} = s.{c}," for c in src_update_cols]).replace("[", "").replace("]", "").replace("', '", "").replace("'", "")

            src_update_cols_ = src_update_cols_[: src_update_cols_.rfind(",")]

            # update join statement
            src_join_cols2_ = src_join_cols_.replace("t.", f"{tgt_tbl}.")
            if update_compare_cols:
                update_compare_cols_ = (
                    str([f"s.{c} != {tgt_tbl}.{c}," for c in update_compare_cols])
                    .replace("[", "")
                    .replace("]", "")
                    .replace("', '", "")
                    .replace("'", "")
                )

                update_compare_cols_ = update_compare_cols_[: update_compare_cols_.rfind(",")]
                src_join_cols2_ = src_join_cols2_ + " AND " + update_compare_cols_

            sql_update = f"""
                /* Update records*/
                UPDATE {tgt_tbl}
                SET {src_update_cols_}
                FROM {src_tbl} s
                WHERE {src_join_cols2_}
                """.replace("\n", " ")
        else:
            sql_update = ""

        sql_insert = f"""
            /* Insert records*/
            INSERT INTO {tgt_tbl}
            SELECT {src_insert_cols_}
            FROM {src_tbl} s
            LEFT JOIN {tgt_tbl} t
            ON {src_join_cols_}
            WHERE {src_join_cols_f}
            """.replace("\n", " ")

        return sql_update, sql_insert

    @classmethod
    def make_df_tbl_rs(cls, tbl_name: str, df: Any) -> str:
        """
        Creates SQL to run a CREATE TABLE statement based on a Pandas DataFrame.

        :param tbl_name: Redshift table name.
        :param df: Pandas DataFrame.
        :return: CREATE TABLE SQL statement.
        """
        import numpy as np
        import pandas as pd

        # fix columns
        df = Generic.make_db_cols(df)

        # loop thru the columns
        for idx, col in enumerate(df):
            col_desc = col + "-" + str(df[col].map(lambda x: len(str(x))).max())
            try:
                col_max = col + "-" + str(max(df[col]))
            except:
                col_max = col + "-" + "NA"

            if idx == 0:
                col_desc_all = [col_desc]
                col_max_all = [col_max]
            else:
                col_desc_all.append(col_desc)
                col_max_all.append(col_max)

        col_desc_all = pd.DataFrame(col_desc_all)
        col_desc_all.columns = ["char"]
        col_desc_all["column"], col_desc_all["length"] = col_desc_all["char"].str.split("-", 1).str

        col_max_all = pd.DataFrame(col_max_all)
        col_max_all.columns = ["char"]
        col_max_all["column"], col_max_all["max"] = col_max_all["char"].str.split("-", 1).str

        col_desc_types = pd.DataFrame(df.dtypes).reset_index()
        col_desc_types.columns = ["column", "type"]

        col_desc_all = pd.merge(col_desc_all, col_desc_types, how="inner", on="column")
        col_desc_all = pd.merge(col_desc_all, col_max_all[["column", "max"]], how="inner", on="column")

        d = {
            "object": "VARCHAR",
            "int64": "INTEGER",
            "Int64": "INTEGER",
            "int32": "INTEGER",
            "bool": "VARCHAR",
            "float64": "FLOAT",
            "datetime64[ns]": "TIMESTAMP",
            "datetime64[ns, UTC]": "TIMESTAMP",
        }

        col_desc_all = col_desc_all.astype(str).replace(d)

        col_desc_all["concat"] = np.where(
            col_desc_all["type"] == "VARCHAR",
            col_desc_all["column"] + " " + col_desc_all["type"].astype(str) + "(" + col_desc_all["length"] + ")",
            col_desc_all["column"] + " " + col_desc_all["type"].astype(str),
        )

        for idx, row in col_desc_all.iterrows():
            if str(row["type"]) == "INTEGER" and row["max"] != "nan" and int(row["max"]) > 2147483647:
                val = row["concat"]
                col_desc_all.loc[idx, "concat"] = val.replace(" INTEGER", f" VARCHAR({row['length']})")

        col_desc_all = col_desc_all.apply(", ".join).reset_index()
        col_desc_all.columns = ["index", "statement"]
        statement = col_desc_all[col_desc_all["index"] == "concat"]
        sql = statement["statement"].values
        sql = str(sql)
        sql = sql.replace("[", "")
        sql = sql.replace("]", "")
        sql = "CREATE TABLE " + tbl_name + " ( " + sql + " )"
        sql = sql.replace("'", "")

        return sql

    @classmethod
    def insert_df_rs(cls, cursor: Any, conn: Any, df: Any, tbl_name: str) -> None:
        """
        Executes an INSERT INTO statement for a given Pandas DataFrame into a Redshift table.

        :param cursor: Redshift database cursor object.
        :param conn: Redshift database connection object.
        :param df: Pandas DataFrame to insert into a Redshift table.
        :param tbl_name: Redshift table name.
        :return: Elapsed time to execute query.
        """
        import numpy as np
        import psycopg2.extras

        df_load = df.replace({np.nan: None})
        df_load = df_load.round(3)
        df_columns = list(df_load)
        columns = ",".join(df_columns)
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(tbl_name, columns, values)
        rptg_tstart = datetime.now()
        psycopg2.extras.execute_batch(cursor, insert_stmt, df_load.values)
        conn.commit()
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(f"Runtime: {tdelta}")

    @classmethod
    def make_tbl_complete_rs(cls, df: Any, tbl_name: str, conn: Any, cursor: Any, batch_size: Any = False) -> None:
        """
        Executes a series of SQL statements to CREATE and INSERT into a table from a Pandas DataFrame.

        :param df: Pandas DataFrame to create a table from.
        :param tbl_name: Name of table to be created.
        :param conn: Redshift database connection object.
        :param cursor: Redshift database cursor object.
        :param batch_size: Records to load per batch.
        :return: Elapsed time to execute query.
        """
        # 1 drop the table
        print(f"dropping table: {tbl_name}")
        try:
            cls.run_query_rs(sql=f"drop table {tbl_name}", conn=conn)
        except:
            print(f"table doesn't exist: {tbl_name}")
            pass
        # create the table
        print(f"creating table: {tbl_name}")
        sql = cls.make_df_tbl_rs(df=df, tbl_name=tbl_name)
        print(sql)
        cls.run_query_rs(sql=sql, conn=conn)

        print(f"inserting DF values into table: {tbl_name}")
        rptg_tstart = datetime.now()
        cls.insert_df_rs(df=df, tbl_name=tbl_name, cursor=cursor, conn=conn, batch_size=batch_size)
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(f"Runtime: {tdelta}")

    @classmethod
    def sequential_load_rs(
        cls,
        override: bool,
        tgt_tbl: str,
        conn: Any,
        dt_start: str,
        dt_end: str,
        saved_day_id_range_placeholder: str,
        dt1_interval: str,
        dt2_interval: str,
        sql_loop_fn: Any,
        sql_loop_fn_type: str,
        filter_day_id_field1: Any = False,
        sql_loop_fn_dt_placeholder1: Any = False,
        filter_day_id_field2: Any = False,
        filter_id_type2: Any = False,
        sql_loop_fn_dt_placeholder2: Any = False,
        filter_day_id_field3: Any = False,
        filter_id_type3: Any = False,
        sql_loop_fn_dt_placeholder3: Any = False,
        loop_src1: Any = False,
        loop_src2: Any = False,
        loop_src3: Any = False,
        log_dir: Any = False,
    ) -> None:
        """
        Sequentially loads data into a Redshift table using date-based SQL looping.

        :param override: Whether to drop and recreate the target table.
        :param tgt_tbl: Target table name.
        :param conn: Redshift database connection object.
        :param dt_start: Start date string.
        :param dt_end: End date string.
        :param saved_day_id_range_placeholder: Placeholder string for date range in SQL.
        :param dt1_interval: Primary date interval.
        :param dt2_interval: Secondary date interval for weekly fallback.
        :param sql_loop_fn: SQL template string or callable.
        :param sql_loop_fn_type: 'fn' if sql_loop_fn is callable.
        :param filter_day_id_field1: First date filter field.
        :param sql_loop_fn_dt_placeholder1: Placeholder for first date filter.
        :param filter_day_id_field2: Second date filter field.
        :param filter_id_type2: Type of second filter.
        :param sql_loop_fn_dt_placeholder2: Placeholder for second date filter.
        :param filter_day_id_field3: Third date filter field.
        :param filter_id_type3: Type of third filter.
        :param sql_loop_fn_dt_placeholder3: Placeholder for third date filter.
        :param loop_src1: First source parameter.
        :param loop_src2: Second source parameter.
        :param loop_src3: Third source parameter.
        :param log_dir: Directory to log SQL statements.
        """
        import pandas as pd

        # define the month start/end dates to loop through
        rptg_dates = pd.date_range(dt_start, dt_end, freq=dt1_interval) - pd.offsets.MonthBegin(1)
        rptg_dates = [str(x)[:10] for x in rptg_dates.to_list()]
        rptg_dates = pd.DataFrame({"start_date": rptg_dates, "end_date": rptg_dates})
        rptg_dates["end_date"] = rptg_dates["end_date"].shift(-1)
        rptg_dates = rptg_dates[pd.to_datetime(rptg_dates["start_date"]) <= datetime.now()].dropna()

        # define the weekly start/end dates to loop thru
        rptg_dates_wk = pd.date_range(dt_start, dt_end, freq=dt2_interval)
        rptg_dates_wk = [str(x)[:10] for x in rptg_dates_wk.to_list()]
        rptg_dates_wk = pd.DataFrame({"start_date": rptg_dates_wk, "end_date": rptg_dates_wk})
        rptg_dates_wk["end_date"] = rptg_dates_wk["end_date"].shift(-1)
        rptg_dates_wk = rptg_dates_wk[pd.to_datetime(rptg_dates_wk["start_date"]) <= datetime.now()].dropna()

        # dropping table if override = True
        if override:
            print(f"""table override True: Dropping table: {tgt_tbl} """)
            try:
                cls.run_query_rs(conn=conn, sql=f"""drop table {tgt_tbl}""")
            except:
                conn.commit()
                pass

        # getting max day id value
        try:
            sql = f"""select max(date(trim(substring(dt_range,regexp_instr(dt_range,'to ')+3,10)))) as day_idnt FROM {tgt_tbl}"""
            saved_dates = pd.read_sql_query(sql=sql, con=conn)
        except:
            conn.commit()
            saved_dates = pd.DataFrame({"day_idnt": ["1999-12-31"]})  # arbitrarily old date

        saved_date_dt = (
            datetime(
                year=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[0]),
                month=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[1]),
                day=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[2]),
            )
            .replace(day=1)
            .strftime("%Y-%m-%d")
        )

        rptg_dates = rptg_dates[pd.to_datetime(rptg_dates["start_date"]) >= pd.to_datetime(saved_date_dt)].reset_index(drop=True)

        print("Starting load from:")
        print(rptg_dates.head(1))

        rptg_freq = "M"

        for idx, row in rptg_dates.iterrows():
            print(f"""{row["start_date"]} to {row["end_date"]}""")

            if idx == 0 and saved_dates["day_idnt"][0] != pd.to_datetime(row["start_date"]):
                print(f"""latest saved data date in table is {str(saved_dates["day_idnt"][0])} ...""")
                new_start = str(pd.to_datetime(str(saved_dates["day_idnt"][0])) + pd.DateOffset(1))[:10]
                print(f"""revising start date to: {new_start} to {row["end_date"]}""")

                if sql_loop_fn_type == "fn":
                    sql = sql_loop_fn(
                        src=loop_src1,
                        src2=loop_src2,
                        src3=loop_src3,
                        start=new_start,
                        end=row["end_date"],
                    )
                else:
                    sql = sql_loop_fn.replace(
                        saved_day_id_range_placeholder,
                        f" '{new_start} to {row['end_date']}' as dt_range,",
                    )

                    sql = sql.replace(
                        sql_loop_fn_dt_placeholder1,
                        f" AND date({filter_day_id_field1}) >= '{new_start}' AND date({filter_day_id_field1}) < '{row['end_date']}'",
                    )

                    if sql_loop_fn_dt_placeholder2:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{new_start}' AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )
                        elif filter_id_type2 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )

                    if sql_loop_fn_dt_placeholder3:
                        if filter_day_id_field3 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{new_start}' AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )
                        elif filter_id_type3 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )

            else:
                if sql_loop_fn_type == "fn":
                    sql = sql_loop_fn(
                        start=row["start_date"],
                        end=row["end_date"],
                        src=loop_src1,
                        src2=loop_src2,
                        src3=loop_src3,
                    )
                else:
                    sql = sql_loop_fn.replace(
                        saved_day_id_range_placeholder,
                        f" '{row['start_date']} to {row['end_date']}' as dt_range,",
                    )

                    sql = sql.replace(
                        saved_day_id_range_placeholder,
                        f" '{row['start_date']} to {row['end_date']}' as dt_range,",
                    )

                    sql = sql.replace(
                        sql_loop_fn_dt_placeholder1,
                        f" AND date({filter_day_id_field1}) >= '{row['start_date']}' AND date({filter_day_id_field1}) < '{row['end_date']}'",
                    )

                    if sql_loop_fn_dt_placeholder2:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{row['start_date']}' AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )

                        elif filter_id_type2 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )

                    if sql_loop_fn_dt_placeholder3:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{row['start_date']}' AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )

                        elif filter_id_type3 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )

            if idx == 0 and override:
                sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
            else:
                sql_prefix = f"INSERT INTO {tgt_tbl} "

            _dump_sql(obj=sql_prefix + sql, filepath=log_dir + f"{tgt_tbl}_{idx}.sql")

            try:
                cls.run_query_rs(conn=conn, sql=sql_prefix + sql)
            except Exception as e:
                print(str(e))
                rptg_freq = "W"
                conn.commit()
                break

        # if the insert failed on a monthly level, cycle down to weekly level
        if rptg_freq == "W":
            print("Insert failed on monthly level...cycling down to weekly")
            try:
                sql = f"""select max(date(trim(substring(dt_range,regexp_instr(dt_range,'to ')+3,10)))) as day_idnt FROM {tgt_tbl}"""
                saved_dates = pd.read_sql_query(sql=sql, con=conn)
            except:
                conn.commit()
                saved_dates = pd.DataFrame({"day_idnt": ["1999-12-31"]})

            saved_date_dt = (
                datetime(
                    year=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[0]),
                    month=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[1]),
                    day=int(str(saved_dates["day_idnt"].astype(str).values[0]).split("-")[2]),
                )
                .replace(day=1)
                .strftime("%Y-%m-%d")
            )

            rptg_dates_wk = rptg_dates_wk[pd.to_datetime(rptg_dates_wk["start_date"]) >= pd.to_datetime(saved_date_dt)].reset_index(drop=True)

            for idx, row in rptg_dates_wk.iterrows():
                print(f"""{row["start_date"]} to {row["end_date"]}""")

                if idx == 0 and saved_dates["day_idnt"][0] != pd.to_datetime(row["start_date"]):
                    print(f"""latest saved data date in table is {str(saved_dates["day_idnt"][0])} ...""")
                    new_start = str(pd.to_datetime(str(saved_dates["day_idnt"][0])) + pd.DateOffset(1))[:10]
                    print(f"""revising start date to: {new_start} to {row["end_date"]}""")

                    if sql_loop_fn_type == "fn":
                        sql = sql_loop_fn(
                            src=loop_src1,
                            src2=loop_src2,
                            src3=loop_src3,
                            start=new_start,
                            end=row["end_date"],
                        )
                    else:
                        sql = sql_loop_fn.replace(
                            saved_day_id_range_placeholder,
                            f" '{new_start} to {row['end_date']}' as dt_range,",
                        )

                        sql = sql.replace(
                            sql_loop_fn_dt_placeholder1,
                            f" AND date({filter_day_id_field1}) >= '{new_start}' AND date({filter_day_id_field1}) < '{row['end_date']}'",
                        )

                        if sql_loop_fn_dt_placeholder2:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{new_start}' AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )

                        if sql_loop_fn_dt_placeholder3:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{new_start}' AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )

                else:
                    if sql_loop_fn_type == "fn":
                        sql = sql_loop_fn(
                            start=row["start_date"],
                            end=row["end_date"],
                            src=loop_src1,
                            src2=loop_src2,
                            src3=loop_src3,
                        )
                    else:
                        sql = sql_loop_fn.replace(
                            saved_day_id_range_placeholder,
                            f" '{row['start_date']} to {row['end_date']}' as dt_range,",
                        )

                        sql = sql.replace(
                            sql_loop_fn_dt_placeholder1,
                            f" AND date({filter_day_id_field1}) >= '{row['start_date']}' AND date({filter_day_id_field1}) < '{row['end_date']}'",
                        )

                        if sql_loop_fn_dt_placeholder2:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{row['start_date']}' AND date({filter_day_id_field2}) < '{row['end_date']}'",
                            )

                        if sql_loop_fn_dt_placeholder3:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{row['start_date']}' AND date({filter_day_id_field3}) < '{row['end_date']}'",
                            )

                if idx == 0 and override:
                    sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
                else:
                    sql_prefix = f"INSERT INTO {tgt_tbl} "

                _dump_sql(obj=sql_prefix + sql, filepath=log_dir + f"{tgt_tbl}_{idx}.sql")

                cls.run_query_rs(conn=conn, sql=sql_prefix + sql)

    @classmethod
    def sequential_load_rs_wk(
        cls,
        rptg_dates: Any,
        override: bool,
        tgt_tbl: str,
        conn: Any,
        rptg_wk: str,
        rptg_wk_start: str,
        rptg_wk_end: str,
        sql_loop_fn: str,
        # filter dates set 1
        filter_dt_field1: Any = False,
        filter_dt_type1: Any = False,
        filter_dt_placeholder1: Any = False,
        # filter dates set 2
        filter_dt_field2: Any = False,
        filter_dt_type2: Any = False,
        filter_dt_placeholder2: Any = False,
        # filter dates set 3
        filter_dt_field3: Any = False,
        filter_dt_type3: Any = False,
        filter_dt_placeholder3: Any = False,
        log_dir: Any = False,
    ) -> None:
        """
        Sequentially loads data into a Redshift table using weekly date-based SQL looping.

        :param rptg_dates: DataFrame with start_date, end_date, rptg_wk columns.
        :param override: Whether to drop and recreate the target table.
        :param tgt_tbl: Target table name.
        :param conn: Redshift database connection object.
        :param rptg_wk: Placeholder for rptg_wk in SQL.
        :param rptg_wk_start: Placeholder for rptg_wk_start in SQL.
        :param rptg_wk_end: Placeholder for rptg_wk_end in SQL.
        :param sql_loop_fn: SQL template string.
        :param filter_dt_field1: First date filter field.
        :param filter_dt_type1: Type of first filter.
        :param filter_dt_placeholder1: Placeholder for first date filter.
        :param filter_dt_field2: Second date filter field.
        :param filter_dt_type2: Type of second filter.
        :param filter_dt_placeholder2: Placeholder for second date filter.
        :param filter_dt_field3: Third date filter field.
        :param filter_dt_type3: Type of third filter.
        :param filter_dt_placeholder3: Placeholder for third date filter.
        :param log_dir: Directory to log SQL statements.
        """
        # dropping table if override = True
        if override:
            print(f"""table override True: Dropping table: {tgt_tbl} """)
            try:
                cls.run_query_rs(conn=conn, sql=f"""drop table {tgt_tbl}""")
            except:
                conn.commit()
                pass

        for idx, row in rptg_dates.iterrows():
            print(f"""{row["start_date"]} to {row["end_date"]}""")

            # date range column for logging
            sql = sql_loop_fn.replace(rptg_wk, f" '{row['rptg_wk']}' as rptg_wk,")

            sql = sql.replace(rptg_wk_start, f" '{row['start_date']}' as rptg_wk_start,")

            sql = sql.replace(rptg_wk_end, f" '{row['end_date']}' as rptg_wk_end,")

            # date filters
            sql = sql.replace(
                filter_dt_placeholder1,
                f" AND date({filter_dt_field1}) > '{row['start_date']}'  AND date({filter_dt_field1}) <= '{row['end_date']}'",
            )

            # check for other date fields
            if filter_dt_placeholder2:
                if filter_dt_type2 == "range":
                    sql = sql.replace(
                        filter_dt_placeholder2,
                        f" AND date({filter_dt_field2}) > '{row['start_date']}'  AND date({filter_dt_field2}) <= '{row['end_date']}'",
                    )
                elif filter_dt_type2 == "<=":
                    sql = sql.replace(
                        filter_dt_placeholder2,
                        f" AND date({filter_dt_field2}) <= '{row['end_date']}'",
                    )

            if filter_dt_placeholder3:
                if filter_dt_type3 == "range":
                    sql = sql.replace(
                        filter_dt_placeholder3,
                        f" AND date({filter_dt_field3}) > '{row['start_date']}'  AND date({filter_dt_field3}) <= '{row['end_date']}'",
                    )
                elif filter_dt_type3 == "<=":
                    sql = sql.replace(
                        filter_dt_placeholder3,
                        f" AND date({filter_dt_field3}) <= '{row['end_date']}'",
                    )

            if idx == 0 and override:
                sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
            else:
                sql_prefix = f"INSERT INTO {tgt_tbl} "

            _dump_sql(obj=sql_prefix + sql, filepath=log_dir + f"{tgt_tbl}_{idx}.sql")

            try:
                cls.run_query_rs(conn=conn, sql=sql_prefix + sql)
            except Exception as e:
                print(str(e))
                conn.commit()
                break


# MARK: - Teradata ETL
class TeradataETL:
    """Teradata-specific ETL operations."""

    @classmethod
    def insert_td(
        cls,
        tbl: str,
        df: Any,
        conn: Any,
        batch_size: Any = False,
        date_cols: Any = False,
    ) -> None:
        """
        Executes an INSERT INTO statement for a given Pandas DataFrame.

        :param tbl: Teradata table name.
        :param df: Pandas DataFrame.
        :param conn: Teradata connection object.
        :param batch_size: Records to load per batch.
        :param date_cols: A list of date columns to convert to Pandas datetime.
        :return: Printed SQL statements for each step.
        """
        import numpy as np
        import pandas as pd

        print(f"batch size: {batch_size}")

        if type(df) != type(pd.DataFrame()):
            print("Detected something other than a DataFrame\n     Please use a pandas DataFrame")
            raise TypeError("Unsupported object type!")

        if date_cols:
            print("    ...Attempting to convert elligible columns to date")
            for idx, column in enumerate(date_cols):
                df[column] = pd.to_datetime(df[column], errors="ignore")

            date_columns = list(df.select_dtypes(include=[np.datetime64]).columns)
            print(f"         {len(date_columns)} date column(s) found")
            for column in date_columns:
                df[column] = df[column].dt.strftime("%Y-%m-%d")

        sql_vars = ("?, " * (len(df.columns) - 1)) + "?"

        sql = f"insert into {tbl}  values({sql_vars})"
        data = df

        print("    ...Beginning bulk insert operation")
        if not batch_size:
            batch_size = 10000

        try:
            print(f"{len(range(0, int(np.floor(df.shape[0] / batch_size) + 1)))} batches found")
            for i in range(0, int(np.floor(df.shape[0] / batch_size) + 1)):
                data_sample = [tuple(x) for x in data.iloc[batch_size * i : batch_size * (i + 1), :].values]
                conn.executemany(sql, data_sample, batch=True)
                print(sql)
                print(f"        ...Completed batch {i} of {len(range(0, int(np.floor(df.shape[0] / batch_size) + 1)))}")
        except Exception as e:
            print(data.head())
            raise e
        print("    ...Successfully loaded Data into Teradata")
        return None

    @classmethod
    def run_query_td(cls, conn: Any, sql: str) -> None:
        """
        Executes a SQL statement with a Teradata database connection.

        :param conn: Teradata database connection object.
        :param sql: SQL statement to execute.
        :return: Elapsed time to execute query.
        """
        rptg_tstart = datetime.now()
        conn.execute(sql)
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(f"Runtime: {tdelta}")

    @classmethod
    def make_tbl_td(cls, df: Any, tbl_name: str) -> str:
        """
        Creates SQL to run a CREATE TABLE statement based on a Pandas DataFrame.

        :param df: Pandas DataFrame.
        :param tbl_name: Teradata table name.
        :return: CREATE TABLE SQL statement.
        """
        import numpy as np
        import pandas as pd

        # fix columns
        df = Generic.make_db_cols(df)

        # loop thru the columns
        for idx, col in enumerate(df):
            col_desc = col + "-" + str(df[col].map(lambda x: len(str(x))).max())
            try:
                col_max = col + "-" + str(max(df[col]))
            except:
                col_max = col + "-" + "NA"

            if idx == 0:
                col_desc_all = [col_desc]
                col_max_all = [col_max]
            else:
                col_desc_all.append(col_desc)
                col_max_all.append(col_max)

        col_desc_all = pd.DataFrame(col_desc_all)
        col_desc_all.columns = ["char"]
        col_desc_all["column"], col_desc_all["length"] = col_desc_all["char"].str.split("-", 1).str

        col_max_all = pd.DataFrame(col_max_all)
        col_max_all.columns = ["char"]
        col_max_all["column"], col_max_all["max"] = col_max_all["char"].str.split("-", 1).str

        col_desc_types = pd.DataFrame(df.dtypes).reset_index()
        col_desc_types.columns = ["column", "type"]

        col_desc_all = pd.merge(col_desc_all, col_desc_types, how="inner", on="column")
        col_desc_all = pd.merge(col_desc_all, col_max_all[["column", "max"]], how="inner", on="column")

        d = {
            "object": "VARCHAR",
            "int64": "INTEGER",
            "Int64": "INTEGER",
            "int32": "INTEGER",
            "bool": "VARCHAR",
            "float64": "FLOAT",
            "datetime64[ns]": "DATE",
        }

        col_desc_all = col_desc_all.replace(d)

        col_desc_all["concat"] = np.where(
            col_desc_all["type"] == "VARCHAR",
            col_desc_all["column"] + " " + col_desc_all["type"].astype(str) + "(" + col_desc_all["length"] + ")",
            col_desc_all["column"] + " " + col_desc_all["type"].astype(str),
        )

        for idx, row in col_desc_all.iterrows():
            if str(row["type"]) == "INTEGER" and row["max"] != "nan" and int(row["max"]) > 2147483647:
                val = row["concat"]
                col_desc_all.loc[idx, "concat"] = val.replace(" INTEGER", f" VARCHAR({row['length']})")

        col_desc_all = col_desc_all.apply(", ".join).reset_index()
        col_desc_all.columns = ["index", "statement"]
        statement = col_desc_all[col_desc_all["index"] == "concat"]
        sql = statement["statement"].values
        sql = str(sql)
        sql = sql.replace("[", "")
        sql = sql.replace("]", "")
        sql = "CREATE TABLE " + tbl_name + " ( " + sql + " )"
        sql = sql.replace("'", "")

        return sql

    @classmethod
    def make_tbl_complete_td(cls, df: Any, tbl_name: str, conn: Any, batch_size: Any = False) -> None:
        """
        Executes a series of SQL statements to CREATE and INSERT into a table from a Pandas DataFrame.

        :param df: Pandas DataFrame to create a table from.
        :param tbl_name: Name of table to be created.
        :param conn: Teradata database connection object.
        :param batch_size: Records to load per batch.
        :return: Elapsed time to execute query.
        """
        # 1 drop the table
        print(f"dropping table: {tbl_name}")
        try:
            cls.run_query_td(sql=f"drop table {tbl_name}", conn=conn)
        except:
            print(f"table doesn't exist: {tbl_name}")
            pass
        # create the table
        print(f"creating table: {tbl_name}")
        sql = cls.make_tbl_td(df=df, tbl_name=tbl_name)
        print(sql)
        cls.run_query_td(sql=sql, conn=conn)

        print(f"inserting DF values into table: {tbl_name}")
        rptg_tstart = datetime.now()
        cls.insert_td(df=df, tbl=tbl_name, conn=conn, batch_size=batch_size)
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(f"Runtime: {tdelta}")

    @classmethod
    def load_td(cls, df: Any, tbl_name: str, conn: Any) -> None:
        """
        Loads a Pandas DataFrame into a Teradata table.

        :param df: Pandas DataFrame.
        :param tbl_name: Teradata table name.
        :param conn: Teradata connection object.
        """
        cls.insert_td(tbl=tbl_name, df=df, conn=conn)

    @classmethod
    def drop_td(cls, tbl_name: str, conn: Any) -> None:
        """
        Drops a Teradata table.

        :param tbl_name: Teradata table name.
        :param conn: Teradata connection object.
        """
        cls.run_query_td(conn=conn, sql=f"DROP TABLE {tbl_name}")

    @classmethod
    def read_td(cls, sql: str, conn: Any) -> Any:
        """
        Reads a SQL query result into a Pandas DataFrame from Teradata.

        :param sql: SQL query string.
        :param conn: Teradata connection object.
        :return: Pandas DataFrame.
        """
        import pandas as pd

        df = pd.read_sql(sql=sql, con=conn)
        return df
