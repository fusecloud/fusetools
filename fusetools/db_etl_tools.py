"""
Database connections and engines.

|pic1| |pic2| |pic3| |pic4|
    .. |pic1| image:: ../images_source/db_etl_tools/oracle1.png
        :width: 20%
    .. |pic2| image:: ../images_source/db_etl_tools/postgres1.png
        :width: 20%
    .. |pic3| image:: ../images_source/db_etl_tools/teradata.png
        :width: 20%
    .. |pic4| image:: ../images_source/db_etl_tools/redshift1.png
        :width: 20%

"""

from datetime import datetime
import os
import re
import time
import cx_Oracle
import numpy as np
import pandas as pd
import psycopg2
from colorama import Fore
from sqlalchemy import text
import psycopg2.extras
from fusetools.text_tools import Export


class Generic:
    """
    Generic functions for SQL queries and ETL.
    """

    @classmethod
    def make_groupby(cls, sql, dim_fact_delim):
        """
        Creates a dynmaically generated GROUP BY clause for a given SQL statement.

        :param sql: SQL statement provided.
        :param dim_fact_delim: Delimiter between selected columns.
        :return: A complete SQL statement with dynamically generated GROUP BY clause.
        """
        dim_segs_ = []
        for idxx, d in enumerate(sql.replace("\n", "").split("SELECT")[1].split(dim_fact_delim)[0].split(", ")):
            if d.strip() != '':
                dim_segs_.append(d.split(" as ")[1].strip())

        sql_all = sql + " GROUP BY " + ', '.join(dim_segs_)
        sql_all = sql_all.replace("\n", " ").replace('"', "")
        return sql_all

    @classmethod
    def make_db_schema(cls, df):
        """
        Creates a mapping of Pandas data types to SQL data types.

        :param df: A Pandas DataFrame with column types to be converted.
        :return: A Pandas DataFrame of columns with corresponding SQL data types.
        """
        cols = []
        dtypes = []

        for col in df.columns:

            cols.append(col)
            col_series = df[col].replace(r'^\s*$', np.nan, regex=True)
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
                    int = True
                else:
                    int = False
            except:
                dtypes.append("object")
                continue

            if int and date_len != 3:
                dtype = "Int64"
            elif not int and date_len != 3:
                dtype = "float"
            elif date_len == 3:
                dtype = "datetime64[ns]"
            else:
                dtype = "object"

            dtypes.append(dtype)

        schema_df = pd.DataFrame({"col": cols, "dtype_new": dtypes})
        old_schema_df = pd.DataFrame(df.dtypes, columns=["dtype_old"]).reset_index()
        schema_df2 = pd.merge(schema_df, old_schema_df, how="inner", left_on="col", right_on="index")
        schema_df2['dtype_final'] = np.where(
            schema_df2['dtype_new'] != "object",
            schema_df2['dtype_new'],
            schema_df2['dtype_old']
        )

        return schema_df2

    @classmethod
    def db_apply_schema(cls, df, schema_df):
        """
        Converts Pandas DataFrame columns based on schema DataFrame provided.

        :param df: A Pandas DataFrame with column types to be converted.
        :param schema_df: A Pandas DataFrame of columns with corresponding SQL data types.
        :return: Pandas DataFrame with columns converted to SQL schema.
        """
        df_ret = df
        df_ret = df_ret.replace(r'^\s*$', np.nan, regex=True)
        df_ret = df_ret.replace('', np.nan, regex=True)
        df_ret = df_ret.replace({np.nan: None})

        for idx, row in schema_df.iterrows():
            if row['dtype_final'] == "Int64":
                df_ret[row['col']] = df_ret[row['col']].replace({np.nan: None})
                df_ret[row['col']] = df_ret[row['col']].astype(float).astype("Int64")

            elif row['dtype_final'] == "datetime64[ns]":
                df_ret[row['col']] = pd.to_datetime(df_ret[row['col']], errors="coerce")
            else:
                df_ret[row['col']] = df_ret[row['col']].replace({np.nan: None})
                df_ret[row['col']] = df_ret[row['col']].astype(row['dtype_final'])

        return df_ret

    @classmethod
    def make_db_cols(cls, df):
        """
        Returns a Pandas DataFrame column names that are converted for database standards.

        :param df: A Pandas DataFrame with columns to be transformed
        :return: Pandas DataFrame column names that are converted for database standards.
        """

        columns = [re.sub('#', 'num', col) for col in df.columns]
        columns = [re.sub('%', 'pct', col) for col in columns]
        columns = [re.sub('[^a-zA-Z0-9]+', ' ', col) for col in columns]
        columns = [col.replace(" ", "_") for col in columns]
        columns = [col[:200] for col in columns]
        columns = [col.lower() for col in columns]
        columns = [c.lstrip("_").rstrip("_") for c in columns]

        df.columns = columns

        return df

    @classmethod
    def run_query(cls, engine, sql):
        """
        Executes a SQL query.

        :param engine: A database engine object.
        :param sql: A SQL statement to be executed.
        :return: Time for execution of SQL query.
        """
        rptg_tstart = datetime.now()
        engine.execute(sql)
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(Fore.RED + f"Runtime: {tdelta}")


class Oracle:
    """
    Generic functions for Oracle SQL queries and ETL.

    .. image:: ../images_source/db_etl_tools/oracle1.png
    """

    @classmethod
    def make_tbl(cls, df, tbl_name):
        """
        Provides a CREATE TABLE SQL statement for a given Pandas DataFrame.

        :param df: A Pandas DataFrame to be added as an Oracle table.
        :param tbl_name: Oracle table name to be created.
        :return: CREATE TABLE SQL statement.
        """

        for idx, col in enumerate(df):
            col_desc = col + "-" + str(df[col].map(lambda x: len(str(x))).max())
            if idx == 0:
                col_desc_all = [col_desc]
            else:
                col_desc_all.append(col_desc)
        col_desc_all = pd.DataFrame(col_desc_all)
        col_desc_all.columns = ["char"]
        col_desc_all['column'], col_desc_all['length'] = col_desc_all['char'].str.split('-', 1).str
        col_desc_types = pd.DataFrame(df.dtypes).reset_index()
        col_desc_types.columns = ["column", "type"]
        col_desc_all = pd.merge(
            col_desc_all,
            col_desc_types,
            how="inner",
            on="column")

        d = {'object': 'VARCHAR',
             'int64': 'NUMBER',
             'float64': 'VARCHAR',
             'datetime64[ns]': 'VARCHAR'}

        col_desc_all = col_desc_all.replace(d)

        col_desc_all['concat'] = np.where(col_desc_all['type'] != "NUMBER",
                                          col_desc_all['column'] + " " + col_desc_all['type'] + "(" + col_desc_all[
                                              'length'] + ")",
                                          col_desc_all['column'] + " " + col_desc_all['type'])

        col_desc_all = col_desc_all.apply(', '.join).reset_index()
        col_desc_all.columns = ["index", "statement"]
        statement = col_desc_all[col_desc_all['index'] == 'concat']
        sql = statement['statement'].values
        sql = str(sql)
        sql = sql.replace("[", "")
        sql = sql.replace("]", "")
        sql = "CREATE TABLE " + tbl_name + " ( " + sql + " )"
        sql = sql.replace("'", "")

        return sql

    @classmethod
    def insert_tbl(cls, df, tbl_name):
        """
        Executes an INSERT INTO statement for a given Pandas DataFrame.

        :param df: A Pandas DataFrame with values to be inserted.
        :param tbl_name: An Oracle table for Pandas DataFrame to be inserted into.
        :return: SQL for INSERT INTO statement.
        """
        sql = 'INSERT INTO ' + tbl_name + '(' + ', '.join(df.columns) + ') VALUES (' + ''.join(
            [':' + str(v) + ', ' for v in list(range(1, len(df.columns)))]) + ':' + str(len(df.columns)) + ')'
        return sql

    @classmethod
    def insert_exec(cls, sql, conn, df):
        """
        Executes a provided SQL statement.

        :param sql: A provided SQL query.
        :param conn: A database connection.
        :param df: A Pandas DataFrame.
        :return: Nothing.
        """
        cursor = cx_Oracle.Cursor(conn)
        cursor.prepare(sql)
        cursor.executemany(None, df.values.tolist())
        conn.commit()
        cursor.close()
        # conn.close()

    @classmethod
    def make_tbl_complete_force(cls, df, tbl_name, eng, conn, attempt_n,
                                subcols=False, chunks=False, chunks_delay=False):
        """
        Executes a series of SQL statements to CREATE and INSERT into a table from a Pandas DataFrame.

        :param df: Pandas DataFrame to create a table from.
        :param tbl_name: Name of table to be created.
        :param eng: Oracle database engine object.
        :param conn: Oracle database connection object.
        :param attempt_n: Number of times to attempt to run INSERT statement.
        :param subcols: A list of columns of the Pandas DataFrame to apply operations on.
        :param chunks: Number of chunks to split Pandas DataFrame into.
        :param chunks_delay: Delay between chunk's INSERT statement.
        :return: Print statements outline sequential SQL statements executed.
        """
        if len(df) > 0:
            if subcols:
                df = df[subcols]
            df.fillna(' ', inplace=True)
            df = df.astype(str)
            # make create table sql
            sql = cls.make_tbl(df, tbl_name)
            print(sql)
            # drop table
            try:
                eng.execute("drop table " + str(tbl_name))
            except Exception as e:
                print(str(e))
                pass
            # create table
            eng.execute(sql)

            # split large df into chunks
            if chunks:
                df_split = np.array_split(df, chunks)
                for sub in df_split:
                    # make insert table sql
                    sql = cls.insert_tbl(sub, tbl_name)
                    print(sql)
                    # execute insert statement
                    # add try counter
                    attempts = attempt_n
                    while attempts > 0:
                        try:
                            cls.insert_exec(sql, conn, sub)
                        except:
                            attempts -= 1
                            print(Fore.RED + f"Failed upload attempt...{attempts} remaining.")
                            time.sleep(1)

                    if chunks_delay:
                        time.sleep(chunks_delay)
                    else:
                        time.sleep(2)

            else:
                # make insert table sql
                sql = cls.insert_tbl(df, tbl_name)
                print(sql)
                # execute insert statement
                cls.insert_exec(sql, conn, df)

    @classmethod
    def make_tbl_complete(cls, df, tbl_name, eng, conn, subcols=False, chunks=False, chunks_delay=False):
        """
        Executes a series of SQL statements to CREATE and INSERT into a table from a Pandas DataFrame.

        :param df: Pandas DataFrame to create a table from.
        :param tbl_name: Name of table to be created.
        :param eng: Oracle database engine object.
        :param conn: Oracle database connection object.
        :param subcols: A list of columns of the Pandas DataFrame to apply operations on.
        :param chunks: Number of chunks to split Pandas DataFrame into.
        :param chunks_delay: Delay between chunk's INSERT statement.
        :return: Print statements outline sequential SQL statements executed.
        """
        if len(df) > 0:
            if subcols:
                df = df[subcols]
            df.fillna(' ', inplace=True)
            df = df.astype(str)
            # make create table sql
            sql = cls.make_tbl(df, tbl_name)
            print(sql)
            # drop table
            try:
                eng.execute("drop table " + str(tbl_name))
            except:
                pass
            # create table
            eng.execute(sql)

            # split large df into chunks
            if chunks:
                df_split = np.array_split(df, chunks)
                for sub in df_split:
                    # make insert table sql
                    sql = cls.insert_tbl(sub, tbl_name)
                    print(sql)
                    # execute insert statement
                    cls.insert_exec(sql, conn, sub)

                    if chunks_delay:
                        time.sleep(chunks_delay)
                    else:
                        time.sleep(2)

            else:
                # make insert table sql
                sql = cls.insert_tbl(df, tbl_name)
                print(sql)
                # execute insert statement
                cls.insert_exec(sql, conn, df)

    @classmethod
    def get_oracle_date(cls, date):
        """
        Converts a date to an Oracle date of format "DD-MMM-YYY"

        :param date: A provided date.
        :return: An Oracle database date.
        """

        # given a datetime YYYY-MM-DD
        if "-" in date:
            year, month, day = str(pd.to_datetime(date)).split("-")
            year = year[2:]
            day = day.replace(" 00:00:00", "")
            month_name = {
                '01': 'JAN',
                '02': 'FEB',
                '03': 'MAR',
                '04': 'APR',
                '05': 'MAY',
                '06': 'JUN',
                '07': 'JUL',
                '08': 'AUG',
                '09': 'SEP',
                '10': 'OCT',
                '11': 'NOV',
                '12': 'DEC'}

            month = month_name.get(month)

            date = day + "-" + month + "-" + year

        # given an excel date
        elif "/" in date:

            date = str(pd.to_datetime(date)).replace(" 00:00:00", "")

            year, month, day = str(pd.to_datetime(date)).split("-")
            year = year[2:]
            day = day.replace(" 00:00:00", "")
            month_name = {
                '01': 'JAN',
                '02': 'FEB',
                '03': 'MAR',
                '04': 'APR',
                '05': 'MAY',
                '06': 'JUN',
                '07': 'JUL',
                '08': 'AUG',
                '09': 'SEP',
                '10': 'OCT',
                '11': 'NOV',
                '12': 'DEC'}

            month = month_name.get(month)

            date = day + "-" + month + "-" + year

        return date

    @classmethod
    def get_orcl_date(cls, dat):
        """
        Converts a date to an Oracle date of format "DD-MMM-YYY".

        :param dat: A provided date column of a Pandas Series.
        :return: An Oracle database date.
        """

        dat['mon'] = dat.dt.month
        dat['day'] = dat.dt.day
        # .astype(str).str.pad(width=2, fillchar="0", side="left")
        dat['year'] = dat.dt.year

        mon_abbrevs = {
            1: 'JAN',
            2: 'FEB',
            3: 'MAR',
            4: 'APR',
            5: 'MAY',
            6: 'JUN',
            7: 'JUL',
            8: 'AUG',
            9: 'SEP',
            10: 'OCT',
            11: 'NOV',
            12: 'DEC'}

        dat['mon_abbrevs'] = \
            dat['mon'].map(mon_abbrevs)

        dat['day'] = dat['day'].str[:-2]
        dat['year'] = dat['year'].astype(str).str[:4]
        dat['year'] = dat['year'].astype(str).str[-2:]

        dat['date_comb'] = \
            dat['day'].astype(str) + "-" + dat['mon_abbrevs'].astype(str) + "-" + dat['year'].astype(str)

        return dat['date_comb']

    @classmethod
    def orcl_tbl_varchar_convert(cls, tbl_name, convert_cols, engine):
        """
        Converts a set of columns to VARCHAR(300) for a given Oracle table.

        :param tbl_name: Oracle table name.
        :param convert_cols: List of columns to convert.
        :param engine: Oracle database engine.
        :return: Printed ALTER table statements for each column.
        """

        # loop through
        for col in convert_cols:
            sql = f'''
                alter table {tbl_name}
                modify {col} varchar(300)

            '''

            print(sql)
            engine.execute(text(sql).execution_options(autocommit=True))
            time.sleep(1)


class Postgres:
    """
    Generic functions for Postgres SQL queries and ETL.

    .. image:: ../images_source/db_etl_tools/postgres1.png
    """

    @classmethod
    def run_query_pg(cls, conn, sql):
        """
        Executes a SQL statement with a Postgres database connection.

        :param conn: Postgres database connection object,
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
        print(Fore.RED + f"Runtime: {tdelta}")

    @classmethod
    def insert_val_pg(cls, col_list, val_list, tbl_name):
        """
        Creates SQL to run an INSERT operation of a given Postgres table.

        :param col_list: List of columns to INSERT or UPDATE.
        :param val_list: List of values to INSERT or UPDATE.
        :param tbl_name: Name of Postgres table.
        :return: SQL to run an INSERT statement.
        """
        sql = f'''
        INSERT INTO {tbl_name} 
        (
            {str(col_list).replace("[", "").replace("]", "").replace("'", "")}
        ) values (
            {str(val_list).replace("[", "").replace("]", "")}
        )
        '''
        return sql

    @classmethod
    def upsert_val_pg(cls, col_list, val_list, tbl_name, constraint_col):
        """
        Creates SQL to run an UPSERT (INSERT new records or UPDATE existing records) operation of a given Postgres table.

        :param col_list: List of columns to INSERT or UPDATE.
        :param val_list: List of values to INSERT or UPDATE.
        :param constraint_col: Column/value logic to check against for INSERT or UPDATE.
        :param tbl_name: Name of Postgres table.
        :return: SQL to run an UPSERT statement.
        """

        update = ""
        for idx, col in zip(col_list, val_list):
            if type(col) in [str]:
                update = update + idx + f"='{col}',"
            else:
                update = update + idx + f"={col},"

        update = update[:update.rfind(",")]

        sql = f'''
        INSERT INTO {tbl_name} 
        ({str(col_list).replace("[", "").replace("]", "").replace("'", "")}) 
        VALUES 
        ({str(val_list).replace("[", "").replace("]", "")})
        ON CONFLICT ({constraint_col}) 
        DO 
        UPDATE SET 
        {update}
        '''

        return sql

    @classmethod
    def upsert_tbl_pg(cls, src_tbl, tgt_tbl, src_join_cols, src_insert_cols,
                      src_update_cols=False, update_compare_cols=False):
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

        src_join_cols_ = (
            str([f"t.{c} = s.{c} AND "
                 for c in src_join_cols])
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
                .replace(",", "")
        )

        src_join_cols_ = src_join_cols_[:src_join_cols_.rfind("AND")]

        src_join_cols_f = (
            str([f"t.{c} IS NULL AND "
                 for c in src_join_cols])
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
                .replace(",", "")
        )

        src_join_cols_f = src_join_cols_f[:src_join_cols_f.rfind("AND")]

        src_insert_cols_ = (
            str([f"s.{c}"
                 for c in src_insert_cols])
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
        )

        if src_update_cols:

            src_update_cols_ = (
                str([f"{c} = s.{c},"
                     for c in src_update_cols])
                    .replace("[", "")
                    .replace("]", "")
                    .replace("', '", "")
                    .replace("'", "")
            )

            src_update_cols_ = src_update_cols_[:src_update_cols_.rfind(",")]

            # update join statement
            src_join_cols2_ = src_join_cols_.replace("t.", f"{tgt_tbl}.")
            if update_compare_cols:
                update_compare_cols_ = (
                    str([f"s.{c} != {tgt_tbl}.{c},"
                         for c in update_compare_cols])
                        .replace("[", "")
                        .replace("]", "")
                        .replace("', '", "")
                        .replace("'", "")
                )

                update_compare_cols_ = update_compare_cols_[:update_compare_cols_.rfind(",")]
                src_join_cols2_ = src_join_cols2_ + " AND " + update_compare_cols_
                # src_join_cols2_ = src_join_cols2_.replace("t.", f"{tgt_tbl}.")

            # https://dwgeek.com/amazon-redshift-merge-statement-alternative-and-example.html/
            sql_update = f'''
                /* Update records*/ 
                UPDATE {tgt_tbl} 
                SET {src_update_cols_}
                FROM {src_tbl} s 
                WHERE {src_join_cols2_}
                '''.replace("\n", " ")
        else:
            sql_update = ""

        sql_insert = f'''
            /* Insert records*/
            INSERT INTO {tgt_tbl}
            SELECT {src_insert_cols_}
            FROM {src_tbl} s 
            LEFT JOIN {tgt_tbl} t 
            ON {src_join_cols_}
            WHERE {src_join_cols_f}
            '''.replace("\n", " ")

        return sql_update, sql_insert

    @classmethod
    def make_df_tbl_pg(cls, tbl_name, df):
        """
        Creates SQL to run a CREATE TABLE statement based on a Pandas DataFrame.

        :param tbl_name: Postgres table name.
        :param df: Pandas DataFrame.
        :return: CREATE TABLE SQL statement.
        """
        # fix columns
        df = Generic.make_db_cols(df)

        # loop thru the columns
        for idx, col in enumerate(df):
            # find the max length of each field
            col_desc = col + "-" + str(df[col].map(lambda x: len(str(x))).max())
            # find the max value of each fields
            try:
                col_max = col + "-" + str(max(df[col]))
            except:
                col_max = col + "-" + 'NA'

            if idx == 0:
                col_desc_all = [col_desc]
                col_max_all = [col_max]
            else:
                col_desc_all.append(col_desc)
                col_max_all.append(col_max)

        # make df of column lengths
        col_desc_all = pd.DataFrame(col_desc_all)
        col_desc_all.columns = ["char"]
        col_desc_all['column'], col_desc_all['length'] = \
            col_desc_all['char'].str.split('-', 1).str

        # make df of column max
        col_max_all = pd.DataFrame(col_max_all)
        col_max_all.columns = ["char"]
        col_max_all['column'], col_max_all['max'] = \
            col_max_all['char'].str.split('-', 1).str

        # make df of column dtypes
        col_desc_types = pd.DataFrame(df.dtypes).reset_index()
        col_desc_types.columns = ["column", "type"]

        # join dfs
        col_desc_all = pd.merge(
            col_desc_all,
            col_desc_types,
            how="inner",
            on="column")

        col_desc_all = pd.merge(
            col_desc_all,
            col_max_all[["column", "max"]],
            how="inner",
            on="column")

        # define data type mapping (pandas --> teradata)
        d = {'object': 'VARCHAR',
             'int64': 'INTEGER',
             'Int64': 'INTEGER',
             'int32': 'INTEGER',
             'bool': 'VARCHAR',
             'float64': 'FLOAT',
             'datetime64[ns]': 'TIMESTAMP',
             "datetime64[ns, UTC]": "TIMESTAMP"}

        col_desc_all = col_desc_all.astype(str).replace(d)

        # list the columns where you want to specify the lengths
        col_desc_all['concat'] = np.where(
            # if varchar, use the length of the longest char
            col_desc_all['type'] == "VARCHAR",
            col_desc_all['column'] + " " + \
            col_desc_all['type'].astype(str) + \
            "(" + col_desc_all['length'] + ")",
            col_desc_all['column'] + " " + \
            col_desc_all['type'].astype(str))

        # convert integers with a max val over certain amount to varchar
        for idx, row in col_desc_all.iterrows():
            if str(row['type']) == 'INTEGER' and row['max'] != "nan" and int(row['max']) > 2147483647:
                val = row['concat']

                col_desc_all.loc[idx, 'concat'] = \
                    val.replace(
                        " INTEGER",
                        f" VARCHAR({row['length']})")

        col_desc_all = col_desc_all.apply(', '.join).reset_index()
        col_desc_all.columns = ["index", "statement"]
        statement = col_desc_all[col_desc_all['index'] == 'concat']
        sql = statement['statement'].values
        sql = str(sql)
        sql = sql.replace("[", "")
        sql = sql.replace("]", "")
        sql = "CREATE TABLE " + tbl_name + " ( " + sql + " )"
        sql = sql.replace("'", "")

        return sql

    @classmethod
    def insert_df_pg(cls, cursor, conn, df, tbl_name, return_statement=None):
        """
        Executes an INSERT INTO statement for a given Pandas DataFrame into a Postgres table..

        :param cursor: Postgres database cursor object.
        :param conn: Postgres database connection object.
        :param df: Pandas DataFrame to insert into a Postgres table.
        :param tbl_name: Postgres table name.
        :return: Elapsed time to execute query.
        """
        df_load = df.replace({np.nan: None})
        df_load = df_load.round(3)
        df_columns = list(df_load)
        # create (col1,col2,...)
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

        print(Fore.RED + f"Runtime: {tdelta}")

    @classmethod
    def make_tbl_complete_pg(cls, df, tbl_name, conn, cursor, batch_size=False):
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
        sql = cls.make_tbl_pg(df=df, tbl_name=tbl_name)
        print(sql)
        cls.run_query_pg(sql=sql, conn=conn)

        print(f"inserting DF values into table: {tbl_name}")
        rptg_tstart = datetime.now()
        cls.insert_pg(df=df, tbl=tbl_name, cursor=cursor, conn=conn, batch_size=batch_size)
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(Fore.RED + f"Runtime: {tdelta}")

    @classmethod
    def sequential_load_pg(cls,
                           override,
                           tgt_tbl,
                           conn,
                           dt_start,
                           dt_end,
                           saved_day_id_range_placeholder,
                           dt1_interval,
                           dt2_interval,
                           sql_loop_fn,
                           sql_loop_fn_type,
                           filter_day_id_field1=False,
                           sql_loop_fn_dt_placeholder1=False,
                           filter_day_id_field2=False,
                           filter_id_type2=False,
                           sql_loop_fn_dt_placeholder2=False,
                           filter_day_id_field3=False,
                           filter_id_type3=False,
                           sql_loop_fn_dt_placeholder3=False,
                           loop_src1=False,
                           loop_src2=False,
                           loop_src3=False,
                           log_dir=False):
        """

        :param override:
        :param tgt_tbl:
        :param conn:
        :param dt_start:
        :param dt_end:
        :param saved_day_id_range_placeholder:
        :param dt1_interval:
        :param dt2_interval:
        :param sql_loop_fn:
        :param sql_loop_fn_type:
        :param filter_day_id_field1:
        :param sql_loop_fn_dt_placeholder1:
        :param filter_day_id_field2:
        :param filter_id_type2:
        :param sql_loop_fn_dt_placeholder2:
        :param filter_day_id_field3:
        :param filter_id_type3:
        :param sql_loop_fn_dt_placeholder3:
        :param loop_src1:
        :param loop_src2:
        :param loop_src3:
        :param log_dir:
        :return:
        """

        # define the month startend dates to loop through
        rptg_dates = pd.date_range(dt_start, dt_end, freq=dt1_interval) - pd.offsets.MonthBegin(1)
        rptg_dates = [str(x)[:10] for x in rptg_dates.to_list()]
        rptg_dates = pd.DataFrame({
            "start_date": rptg_dates,
            "end_date": rptg_dates
        })
        rptg_dates['end_date'] = rptg_dates['end_date'].shift(-1)
        rptg_dates = rptg_dates[pd.to_datetime(rptg_dates['start_date']) <= datetime.now()].dropna()

        # define the weekly start/end dates to loop thru
        rptg_dates_wk = pd.date_range(dt_start, dt_end, freq=dt2_interval)
        rptg_dates_wk = [str(x)[:10] for x in rptg_dates_wk.to_list()]
        rptg_dates_wk = pd.DataFrame({
            "start_date": rptg_dates_wk,
            "end_date": rptg_dates_wk
        })
        rptg_dates_wk['end_date'] = rptg_dates_wk['end_date'].shift(-1)
        rptg_dates_wk = rptg_dates_wk[pd.to_datetime(rptg_dates_wk['start_date']) <= datetime.now()].dropna()

        # dropping table if override = True
        if override:
            print(f'''table override True: Dropping table: {tgt_tbl} ''')
            try:
                cls.run_query_pg(conn=conn, sql=f'''drop table {tgt_tbl}''')
            except:
                conn.commit()
                pass

        # getting max day id value
        try:
            sql = f'''select max(date(trim(substring(dt_range,regexp_instr(dt_range,'to ')+3,10)))) as day_idnt FROM {tgt_tbl}'''
            saved_dates = pd.read_sql_query(sql=sql, con=conn)
        except:
            conn.commit()
            saved_dates = pd.DataFrame({"day_idnt": ["1999-12-31"]})  # arbitrarily old date

        saved_date_dt = \
            datetime(
                year=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[0]),
                month=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[1]),
                day=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[2])
            ).replace(day=1).strftime("%Y-%m-%d")

        rptg_dates = rptg_dates[
            pd.to_datetime(rptg_dates['start_date']) >= \
            pd.to_datetime(saved_date_dt)].reset_index(drop=True)

        print("Starting load from:")
        print(rptg_dates.head(1))

        rptg_freq = "M"

        for idx, row in rptg_dates.iterrows():
            print(f'''{row['start_date']} to {row['end_date']}''')
            # if idx == 0:
            #     break

            if idx == 0 and saved_dates['day_idnt'][0] != pd.to_datetime(row['start_date']):

                print(Fore.RED + f'''latest saved data date in table is {str(saved_dates['day_idnt'][0])} ...''')
                # bump up start range:
                new_start = str(pd.to_datetime(str(saved_dates['day_idnt'][0])) + pd.DateOffset(1))[:10]
                print(Fore.RED + f'''revising start date to: {new_start} to {row['end_date']}''')

                # if its a function, pass in params
                if sql_loop_fn_type == "fn":
                    sql = sql_loop_fn(src=loop_src1,
                                      src2=loop_src2,
                                      src3=loop_src3,
                                      start=new_start,
                                      end=row['end_date'])

                # otherwise, we will just replace strings
                else:
                    # date range column for logging
                    sql = sql_loop_fn.replace(
                        saved_day_id_range_placeholder,
                        f" '{new_start} to {row['end_date']}' as dt_range,"
                    )

                    # date filters
                    sql = sql.replace(
                        sql_loop_fn_dt_placeholder1,
                        f" AND date({filter_day_id_field1}) >= '{new_start}' AND date({filter_day_id_field1}) < '{row['end_date']}'"
                    )

                    # check for other date fields
                    if sql_loop_fn_dt_placeholder2:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{new_start}' AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )
                        elif filter_id_type2 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )

                    if sql_loop_fn_dt_placeholder3:
                        if filter_day_id_field3 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{new_start}' AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )
                        elif filter_id_type3 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )

            else:
                if sql_loop_fn_type == "fn":
                    sql = sql_loop_fn(
                        start=row['start_date'],
                        end=row['end_date'],
                        src=loop_src1,
                        src2=loop_src2,
                        src3=loop_src3
                    )
                else:

                    # date range column for logging
                    sql = sql_loop_fn.replace(
                        saved_day_id_range_placeholder,
                        f" '{row['start_date']} to {row['end_date']}' as dt_range,"
                    )

                    # date range column for logging
                    sql = sql.replace(
                        saved_day_id_range_placeholder,
                        f" '{row['start_date']} to {row['end_date']}' as dt_range,"
                    )

                    sql = sql.replace(
                        sql_loop_fn_dt_placeholder1,
                        f" AND date({filter_day_id_field1}) >= '{row['start_date']}' AND date({filter_day_id_field1}) < '{row['end_date']}'"
                    )

                    # check for other date fields
                    if sql_loop_fn_dt_placeholder2:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{row['start_date']}' AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )

                        elif filter_id_type2 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )

                    if sql_loop_fn_dt_placeholder3:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{row['start_date']}' AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )

                        elif filter_id_type3 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )

            if idx == 0 and override:
                sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
            else:
                sql_prefix = f"INSERT INTO {tgt_tbl} "

            Export.dump_sql(obj=sql_prefix + sql,
                            dir=log_dir + f"{tgt_tbl}_{idx}.sql")

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
                sql = f'''select max(date(trim(substring(dt_range,regexp_instr(dt_range,'to ')+3,10)))) as day_idnt FROM {tgt_tbl}'''
                saved_dates = pd.read_sql_query(sql=sql, con=conn)
            except:
                conn.commit()
                saved_dates = pd.DataFrame({"day_idnt": ["1999-12-31"]})  # arbitrarily old date

            saved_date_dt = \
                datetime(
                    year=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[0]),
                    month=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[1]),
                    day=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[2])
                ).replace(day=1).strftime("%Y-%m-%d")

            rptg_dates_wk = rptg_dates_wk[
                pd.to_datetime(rptg_dates_wk['start_date']) >= \
                pd.to_datetime(saved_date_dt)].reset_index(drop=True)

            for idx, row in rptg_dates_wk.iterrows():
                print(f'''{row['start_date']} to {row['end_date']}''')

                if idx == 0 and saved_dates['day_idnt'][0] != pd.to_datetime(row['start_date']):

                    print(Fore.RED + f'''latest saved data date in table is {str(saved_dates['day_idnt'][0])} ...''')
                    # bump up start range:
                    new_start = str(pd.to_datetime(str(saved_dates['day_idnt'][0])) + pd.DateOffset(1))[:10]
                    print(Fore.RED + f'''revising start date to: {new_start} to {row['end_date']}''')

                    if sql_loop_fn_type == "fn":
                        sql = sql_loop_fn(src=loop_src1,
                                          src2=loop_src2,
                                          src3=loop_src3,
                                          start=new_start,
                                          end=row['end_date'])
                    else:

                        # date range column for logging
                        sql = sql_loop_fn.replace(
                            saved_day_id_range_placeholder,
                            f" '{new_start} to {row['end_date']}' as dt_range,"
                        )

                        sql = sql.replace(
                            sql_loop_fn_dt_placeholder1,
                            f" AND date({filter_day_id_field1}) >= '{new_start}' AND date({filter_day_id_field1}) < '{row['end_date']}'"
                        )

                        # check for other date fields
                        if sql_loop_fn_dt_placeholder2:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{new_start}' AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )

                        if sql_loop_fn_dt_placeholder3:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{new_start}' AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )

                else:

                    if sql_loop_fn_type == "fn":
                        sql = sql_loop_fn(
                            start=row['start_date'],
                            end=row['end_date'],
                            src=loop_src1,
                            src2=loop_src2,
                            src3=loop_src3
                        )
                    else:

                        # date range column for logging
                        sql = sql_loop_fn.replace(
                            saved_day_id_range_placeholder,
                            f" '{row['start_date']} to {row['end_date']}' as dt_range,"
                        )

                        sql = sql.replace(
                            sql_loop_fn_dt_placeholder1,
                            f" AND date({filter_day_id_field1}) >= '{row['start_date']}' AND date({filter_day_id_field1}) < '{row['end_date']}'"
                        )

                        # check for other date fields
                        if sql_loop_fn_dt_placeholder2:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{row['start_date']}' AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )

                        if sql_loop_fn_dt_placeholder3:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{row['start_date']}' AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )

                if idx == 0 and override:
                    sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
                else:
                    sql_prefix = f"INSERT INTO {tgt_tbl} "

                Export.dump_sql(obj=sql_prefix + sql,
                                dir=log_dir + f"{tgt_tbl}_{idx}.sql")

                cls.run_query_pg(conn=conn, sql=sql_prefix + sql)

    @classmethod
    def sequential_load_pg_wk(cls,
                              rptg_dates,
                              override,
                              tgt_tbl,
                              conn,
                              rptg_wk,
                              rptg_wk_start,
                              rptg_wk_end,
                              sql_loop_fn,
                              # filter dates set 1
                              filter_dt_field1=False,
                              filter_dt_type1=False,
                              filter_dt_placeholder1=False,
                              # filter dates set 2
                              filter_dt_field2=False,
                              filter_dt_type2=False,
                              filter_dt_placeholder2=False,
                              # filter dates set 3
                              filter_dt_field3=False,
                              filter_dt_type3=False,
                              filter_dt_placeholder3=False,
                              log_dir=False
                              ):
        """

        :param rptg_dates:
        :param override:
        :param tgt_tbl:
        :param conn:
        :param rptg_wk:
        :param rptg_wk_start:
        :param rptg_wk_end:
        :param sql_loop_fn:
        :param filter_dt_field1:
        :param filter_dt_type1:
        :param filter_dt_placeholder1:
        :param filter_dt_field2:
        :param filter_dt_type2:
        :param filter_dt_placeholder2:
        :param filter_dt_field3:
        :param filter_dt_type3:
        :param filter_dt_placeholder3:
        :param log_dir:
        :return:
        """

        # dropping table if override = True
        if override:
            print(f'''table override True: Dropping table: {tgt_tbl} ''')
            try:
                cls.run_query_pg(conn=conn, sql=f'''drop table {tgt_tbl}''')
            except:
                conn.commit()
                pass

        for idx, row in rptg_dates.iterrows():
            print(f'''{row['start_date']} to {row['end_date']}''')

            # date range column for logging
            sql = sql_loop_fn.replace(
                rptg_wk,
                f" '{row['rptg_wk']}' as rptg_wk,"
            )

            sql = sql.replace(
                rptg_wk_start,
                f" '{row['start_date']}' as rptg_wk_start,"
            )

            sql = sql.replace(
                rptg_wk_end,
                f" '{row['end_date']}' as rptg_wk_end,"
            )

            # date filters
            sql = sql.replace(
                filter_dt_placeholder1,
                f" AND date({filter_dt_field1}) > '{row['start_date']}' "
                f" AND date({filter_dt_field1}) <= '{row['end_date']}'"
            )

            # check for other date fields
            if filter_dt_placeholder2:
                if filter_dt_type2 == "range":
                    sql = sql.replace(
                        filter_dt_placeholder2,
                        f" AND date({filter_dt_field2}) > '{row['start_date']}' "
                        f" AND date({filter_dt_field2}) <= '{row['end_date']}'"
                    )
                elif filter_dt_type2 == "<=":
                    sql = sql.replace(
                        filter_dt_placeholder2,
                        f" AND date({filter_dt_field2}) <= '{row['end_date']}'"
                    )

            if filter_dt_placeholder3:
                if filter_dt_type3 == "range":
                    sql = sql.replace(
                        filter_dt_placeholder3,
                        f" AND date({filter_dt_field3}) > '{row['start_date']}' "
                        f" AND date({filter_dt_field3}) <= '{row['end_date']}'"
                    )
                elif filter_dt_type3 == "<=":
                    sql = sql.replace(
                        filter_dt_placeholder3,
                        f" AND date({filter_dt_field3}) <= '{row['end_date']}'"
                    )

            if idx == 0 and override:
                sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
            else:
                sql_prefix = f"INSERT INTO {tgt_tbl} "

            Export.dump_sql(obj=sql_prefix + sql,
                            dir=log_dir + f"{tgt_tbl}_{idx}.sql")

            try:
                cls.run_query_pg(conn=conn, sql=sql_prefix + sql)
            except Exception as e:
                print(str(e))
                conn.commit()
                break


class Redshift:
    """
    Generic functions for Redshift SQL queries and ETL.

    .. image:: ../images_source/db_etl_tools/redshift1.png
    """

    @classmethod
    def run_query_rs(cls, conn, sql):
        """
        Executes a SQL statement with a Redshift database connection.

        :param conn: Redshift database connection object,
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
        print(Fore.RED + f"Runtime: {tdelta}")

    @classmethod
    def insert_val_rs(cls, col_list, val_list, tbl_name):
        """
        Creates SQL to run an INSERT operation of a given Redshift table.

        :param col_list: List of columns to INSERT or UPDATE.
        :param val_list: List of values to INSERT or UPDATE.
        :param tbl_name: Name of Postgres table.
        :return: SQL to run an INSERT statement.
        """
        sql = f'''
        INSERT INTO {tbl_name} 
        (
            {str(col_list).replace("[", "").replace("]", "").replace("'", "")}
        ) values (
            {str(val_list).replace("[", "").replace("]", "")}
        )
        '''
        return sql

    @classmethod
    def upsert_tbl_rs(cls, src_tbl, tgt_tbl, src_join_cols, src_insert_cols,
                      src_update_cols=False, update_compare_cols=False):
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

        src_join_cols_ = (
            str([f"t.{c} = s.{c} AND "
                 for c in src_join_cols])
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
                .replace(",", "")
        )

        src_join_cols_ = src_join_cols_[:src_join_cols_.rfind("AND")]

        src_join_cols_f = (
            str([f"t.{c} IS NULL AND "
                 for c in src_join_cols])
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
                .replace(",", "")
        )

        src_join_cols_f = src_join_cols_f[:src_join_cols_f.rfind("AND")]

        src_insert_cols_ = (
            str([f"s.{c}"
                 for c in src_insert_cols])
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
        )

        if src_update_cols:

            src_update_cols_ = (
                str([f"{c} = s.{c},"
                     for c in src_update_cols])
                    .replace("[", "")
                    .replace("]", "")
                    .replace("', '", "")
                    .replace("'", "")
            )

            src_update_cols_ = src_update_cols_[:src_update_cols_.rfind(",")]

            # update join statement
            src_join_cols2_ = src_join_cols_.replace("t.", f"{tgt_tbl}.")
            if update_compare_cols:
                update_compare_cols_ = (
                    str([f"s.{c} != {tgt_tbl}.{c},"
                         for c in update_compare_cols])
                        .replace("[", "")
                        .replace("]", "")
                        .replace("', '", "")
                        .replace("'", "")
                )

                update_compare_cols_ = update_compare_cols_[:update_compare_cols_.rfind(",")]
                src_join_cols2_ = src_join_cols2_ + " AND " + update_compare_cols_
                # src_join_cols2_ = src_join_cols2_.replace("t.", f"{tgt_tbl}.")

            # https://dwgeek.com/amazon-redshift-merge-statement-alternative-and-example.html/
            sql_update = f'''
                /* Update records*/ 
                UPDATE {tgt_tbl} 
                SET {src_update_cols_}
                FROM {src_tbl} s 
                WHERE {src_join_cols2_}
                '''.replace("\n", " ")
        else:
            sql_update = ""

        sql_insert = f'''
            /* Insert records*/
            INSERT INTO {tgt_tbl}
            SELECT {src_insert_cols_}
            FROM {src_tbl} s 
            LEFT JOIN {tgt_tbl} t 
            ON {src_join_cols_}
            WHERE {src_join_cols_f}
            '''.replace("\n", " ")

        return sql_update, sql_insert

    @classmethod
    def make_df_tbl_rs(cls, tbl_name, df):
        """
        Creates SQL to run a CREATE TABLE statement based on a Pandas DataFrame.

        :param tbl_name: Redshift table name.
        :param df: Pandas DataFrame.
        :return: CREATE TABLE SQL statement.
        """
        # fix columns
        df = Generic.make_db_cols(df)

        # loop thru the columns
        for idx, col in enumerate(df):
            # find the max length of each field
            col_desc = col + "-" + str(df[col].map(lambda x: len(str(x))).max())
            # find the max value of each fields
            try:
                col_max = col + "-" + str(max(df[col]))
            except:
                col_max = col + "-" + 'NA'

            if idx == 0:
                col_desc_all = [col_desc]
                col_max_all = [col_max]
            else:
                col_desc_all.append(col_desc)
                col_max_all.append(col_max)

        # make df of column lengths
        col_desc_all = pd.DataFrame(col_desc_all)
        col_desc_all.columns = ["char"]
        col_desc_all['column'], col_desc_all['length'] = \
            col_desc_all['char'].str.split('-', 1).str

        # make df of column max
        col_max_all = pd.DataFrame(col_max_all)
        col_max_all.columns = ["char"]
        col_max_all['column'], col_max_all['max'] = \
            col_max_all['char'].str.split('-', 1).str

        # make df of column dtypes
        col_desc_types = pd.DataFrame(df.dtypes).reset_index()
        col_desc_types.columns = ["column", "type"]

        # join dfs
        col_desc_all = pd.merge(
            col_desc_all,
            col_desc_types,
            how="inner",
            on="column")

        col_desc_all = pd.merge(
            col_desc_all,
            col_max_all[["column", "max"]],
            how="inner",
            on="column")

        # define data type mapping (pandas --> teradata)
        d = {'object': 'VARCHAR',
             'int64': 'INTEGER',
             'Int64': 'INTEGER',
             'int32': 'INTEGER',
             'bool': 'VARCHAR',
             'float64': 'FLOAT',
             'datetime64[ns]': 'TIMESTAMP',
             "datetime64[ns, UTC]": "TIMESTAMP"}

        col_desc_all = col_desc_all.astype(str).replace(d)

        # list the columns where you want to specify the lengths
        col_desc_all['concat'] = np.where(
            # if varchar, use the length of the longest char
            col_desc_all['type'] == "VARCHAR",
            col_desc_all['column'] + " " + \
            col_desc_all['type'].astype(str) + \
            "(" + col_desc_all['length'] + ")",
            col_desc_all['column'] + " " + \
            col_desc_all['type'].astype(str))

        # convert integers with a max val over certain amount to varchar
        for idx, row in col_desc_all.iterrows():
            if str(row['type']) == 'INTEGER' and row['max'] != "nan" and int(row['max']) > 2147483647:
                val = row['concat']

                col_desc_all.loc[idx, 'concat'] = \
                    val.replace(
                        " INTEGER",
                        f" VARCHAR({row['length']})")

        col_desc_all = col_desc_all.apply(', '.join).reset_index()
        col_desc_all.columns = ["index", "statement"]
        statement = col_desc_all[col_desc_all['index'] == 'concat']
        sql = statement['statement'].values
        sql = str(sql)
        sql = sql.replace("[", "")
        sql = sql.replace("]", "")
        sql = "CREATE TABLE " + tbl_name + " ( " + sql + " )"
        sql = sql.replace("'", "")

        return sql

    @classmethod
    def insert_df_rs(cls, cursor, conn, df, tbl_name):
        """
        Executes an INSERT INTO statement for a given Pandas DataFrame into a Redshift table..

        :param cursor: Redshift database cursor object.
        :param conn: Redshift database connection object.
        :param df: Pandas DataFrame to insert into a Redshift table.
        :param tbl_name: Redshift table name.
        :return: Elapsed time to execute query.
        """
        df_load = df.replace({np.nan: None})
        df_load = df_load.round(3)
        df_columns = list(df_load)
        # create (col1,col2,...)
        columns = ",".join(df_columns)
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(tbl_name, columns, values)
        rptg_tstart = datetime.now()
        psycopg2.extras.execute_batch(cursor, insert_stmt, df_load.values)
        conn.commit()
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(Fore.RED + f"Runtime: {tdelta}")

    @classmethod
    def make_tbl_complete_rs(cls, df, tbl_name, conn, cursor, batch_size=False):
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
        sql = cls.make_tbl_rs(df=df, tbl_name=tbl_name)
        print(sql)
        cls.run_query_rs(sql=sql, conn=conn)

        print(f"inserting DF values into table: {tbl_name}")
        rptg_tstart = datetime.now()
        cls.insert_rs(df=df, tbl=tbl_name, cursor=cursor, conn=conn, batch_size=batch_size)
        rptg_tend = datetime.now()
        tdelta = rptg_tend - rptg_tstart
        tdelta = tdelta.total_seconds() / 60
        print(Fore.RED + f"Runtime: {tdelta}")

    @classmethod
    def sequential_load_rs(cls,
                           override,
                           tgt_tbl,
                           conn,
                           dt_start,
                           dt_end,
                           saved_day_id_range_placeholder,
                           dt1_interval,
                           dt2_interval,
                           sql_loop_fn,
                           sql_loop_fn_type,
                           filter_day_id_field1=False,
                           sql_loop_fn_dt_placeholder1=False,
                           filter_day_id_field2=False,
                           filter_id_type2=False,
                           sql_loop_fn_dt_placeholder2=False,
                           filter_day_id_field3=False,
                           filter_id_type3=False,
                           sql_loop_fn_dt_placeholder3=False,
                           loop_src1=False,
                           loop_src2=False,
                           loop_src3=False,
                           log_dir=False):
        """

        :param override:
        :param tgt_tbl:
        :param conn:
        :param dt_start:
        :param dt_end:
        :param saved_day_id_range_placeholder:
        :param dt1_interval:
        :param dt2_interval:
        :param sql_loop_fn:
        :param sql_loop_fn_type:
        :param filter_day_id_field1:
        :param sql_loop_fn_dt_placeholder1:
        :param filter_day_id_field2:
        :param filter_id_type2:
        :param sql_loop_fn_dt_placeholder2:
        :param filter_day_id_field3:
        :param filter_id_type3:
        :param sql_loop_fn_dt_placeholder3:
        :param loop_src1:
        :param loop_src2:
        :param loop_src3:
        :param log_dir:
        :return:
        """

        # define the month startend dates to loop through
        rptg_dates = pd.date_range(dt_start, dt_end, freq=dt1_interval) - pd.offsets.MonthBegin(1)
        rptg_dates = [str(x)[:10] for x in rptg_dates.to_list()]
        rptg_dates = pd.DataFrame({
            "start_date": rptg_dates,
            "end_date": rptg_dates
        })
        rptg_dates['end_date'] = rptg_dates['end_date'].shift(-1)
        rptg_dates = rptg_dates[pd.to_datetime(rptg_dates['start_date']) <= datetime.now()].dropna()

        # define the weekly start/end dates to loop thru
        rptg_dates_wk = pd.date_range(dt_start, dt_end, freq=dt2_interval)
        rptg_dates_wk = [str(x)[:10] for x in rptg_dates_wk.to_list()]
        rptg_dates_wk = pd.DataFrame({
            "start_date": rptg_dates_wk,
            "end_date": rptg_dates_wk
        })
        rptg_dates_wk['end_date'] = rptg_dates_wk['end_date'].shift(-1)
        rptg_dates_wk = rptg_dates_wk[pd.to_datetime(rptg_dates_wk['start_date']) <= datetime.now()].dropna()

        # dropping table if override = True
        if override:
            print(f'''table override True: Dropping table: {tgt_tbl} ''')
            try:
                cls.run_query_rs(conn=conn, sql=f'''drop table {tgt_tbl}''')
            except:
                conn.commit()
                pass

        # getting max day id value
        try:
            sql = f'''select max(date(trim(substring(dt_range,regexp_instr(dt_range,'to ')+3,10)))) as day_idnt FROM {tgt_tbl}'''
            saved_dates = pd.read_sql_query(sql=sql, con=conn)
        except:
            conn.commit()
            saved_dates = pd.DataFrame({"day_idnt": ["1999-12-31"]})  # arbitrarily old date

        saved_date_dt = \
            datetime(
                year=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[0]),
                month=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[1]),
                day=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[2])
            ).replace(day=1).strftime("%Y-%m-%d")

        rptg_dates = rptg_dates[
            pd.to_datetime(rptg_dates['start_date']) >= \
            pd.to_datetime(saved_date_dt)].reset_index(drop=True)

        print("Starting load from:")
        print(rptg_dates.head(1))

        rptg_freq = "M"

        for idx, row in rptg_dates.iterrows():
            print(f'''{row['start_date']} to {row['end_date']}''')
            # if idx == 0:
            #     break

            if idx == 0 and saved_dates['day_idnt'][0] != pd.to_datetime(row['start_date']):

                print(Fore.RED + f'''latest saved data date in table is {str(saved_dates['day_idnt'][0])} ...''')
                # bump up start range:
                new_start = str(pd.to_datetime(str(saved_dates['day_idnt'][0])) + pd.DateOffset(1))[:10]
                print(Fore.RED + f'''revising start date to: {new_start} to {row['end_date']}''')

                # if its a function, pass in params
                if sql_loop_fn_type == "fn":
                    sql = sql_loop_fn(src=loop_src1,
                                      src2=loop_src2,
                                      src3=loop_src3,
                                      start=new_start,
                                      end=row['end_date'])

                # otherwise, we will just replace strings
                else:
                    # date range column for logging
                    sql = sql_loop_fn.replace(
                        saved_day_id_range_placeholder,
                        f" '{new_start} to {row['end_date']}' as dt_range,"
                    )

                    # date filters
                    sql = sql.replace(
                        sql_loop_fn_dt_placeholder1,
                        f" AND date({filter_day_id_field1}) >= '{new_start}' AND date({filter_day_id_field1}) < '{row['end_date']}'"
                    )

                    # check for other date fields
                    if sql_loop_fn_dt_placeholder2:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{new_start}' AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )
                        elif filter_id_type2 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )

                    if sql_loop_fn_dt_placeholder3:
                        if filter_day_id_field3 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{new_start}' AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )
                        elif filter_id_type3 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )

            else:
                if sql_loop_fn_type == "fn":
                    sql = sql_loop_fn(
                        start=row['start_date'],
                        end=row['end_date'],
                        src=loop_src1,
                        src2=loop_src2,
                        src3=loop_src3
                    )
                else:

                    # date range column for logging
                    sql = sql_loop_fn.replace(
                        saved_day_id_range_placeholder,
                        f" '{row['start_date']} to {row['end_date']}' as dt_range,"
                    )

                    # date range column for logging
                    sql = sql.replace(
                        saved_day_id_range_placeholder,
                        f" '{row['start_date']} to {row['end_date']}' as dt_range,"
                    )

                    sql = sql.replace(
                        sql_loop_fn_dt_placeholder1,
                        f" AND date({filter_day_id_field1}) >= '{row['start_date']}' AND date({filter_day_id_field1}) < '{row['end_date']}'"
                    )

                    # check for other date fields
                    if sql_loop_fn_dt_placeholder2:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{row['start_date']}' AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )

                        elif filter_id_type2 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )

                    if sql_loop_fn_dt_placeholder3:
                        if filter_id_type2 == "range":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{row['start_date']}' AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )

                        elif filter_id_type3 == "<":
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )

            if idx == 0 and override:
                sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
            else:
                sql_prefix = f"INSERT INTO {tgt_tbl} "

            Export.dump_sql(obj=sql_prefix + sql,
                            dir=log_dir + f"{tgt_tbl}_{idx}.sql")

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
            # getting max day id value
            try:
                sql = f'''select max(date(trim(substring(dt_range,regexp_instr(dt_range,'to ')+3,10)))) as day_idnt FROM {tgt_tbl}'''
                saved_dates = pd.read_sql_query(sql=sql, con=conn)
            except:
                conn.commit()
                saved_dates = pd.DataFrame({"day_idnt": ["1999-12-31"]})  # arbitrarily old date

            saved_date_dt = \
                datetime(
                    year=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[0]),
                    month=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[1]),
                    day=int(str(saved_dates['day_idnt'].astype(str).values[0]).split("-")[2])
                ).replace(day=1).strftime("%Y-%m-%d")

            rptg_dates_wk = rptg_dates_wk[
                pd.to_datetime(rptg_dates_wk['start_date']) >= \
                pd.to_datetime(saved_date_dt)].reset_index(drop=True)

            for idx, row in rptg_dates_wk.iterrows():
                print(f'''{row['start_date']} to {row['end_date']}''')

                if idx == 0 and saved_dates['day_idnt'][0] != pd.to_datetime(row['start_date']):

                    print(Fore.RED + f'''latest saved data date in table is {str(saved_dates['day_idnt'][0])} ...''')
                    # bump up start range:
                    new_start = str(pd.to_datetime(str(saved_dates['day_idnt'][0])) + pd.DateOffset(1))[:10]
                    print(Fore.RED + f'''revising start date to: {new_start} to {row['end_date']}''')

                    if sql_loop_fn_type == "fn":
                        sql = sql_loop_fn(src=loop_src1,
                                          src2=loop_src2,
                                          src3=loop_src3,
                                          start=new_start,
                                          end=row['end_date'])
                    else:

                        # date range column for logging
                        sql = sql_loop_fn.replace(
                            saved_day_id_range_placeholder,
                            f" '{new_start} to {row['end_date']}' as dt_range,"
                        )

                        sql = sql.replace(
                            sql_loop_fn_dt_placeholder1,
                            f" AND date({filter_day_id_field1}) >= '{new_start}' AND date({filter_day_id_field1}) < '{row['end_date']}'"
                        )

                        # check for other date fields
                        if sql_loop_fn_dt_placeholder2:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{new_start}' AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )

                        if sql_loop_fn_dt_placeholder3:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{new_start}' AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )

                else:

                    if sql_loop_fn_type == "fn":
                        sql = sql_loop_fn(
                            start=row['start_date'],
                            end=row['end_date'],
                            src=loop_src1,
                            src2=loop_src2,
                            src3=loop_src3
                        )
                    else:

                        # date range column for logging
                        sql = sql_loop_fn.replace(
                            saved_day_id_range_placeholder,
                            f" '{row['start_date']} to {row['end_date']}' as dt_range,"
                        )

                        sql = sql.replace(
                            sql_loop_fn_dt_placeholder1,
                            f" AND date({filter_day_id_field1}) >= '{row['start_date']}' AND date({filter_day_id_field1}) < '{row['end_date']}'"
                        )

                        # check for other date fields
                        if sql_loop_fn_dt_placeholder2:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder2,
                                f" AND date({filter_day_id_field2}) >= '{row['start_date']}' AND date({filter_day_id_field2}) < '{row['end_date']}'"
                            )

                        if sql_loop_fn_dt_placeholder3:
                            sql = sql.replace(
                                sql_loop_fn_dt_placeholder3,
                                f" AND date({filter_day_id_field3}) >= '{row['start_date']}' AND date({filter_day_id_field3}) < '{row['end_date']}'"
                            )

                if idx == 0 and override:
                    sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
                else:
                    sql_prefix = f"INSERT INTO {tgt_tbl} "

                Export.dump_sql(obj=sql_prefix + sql,
                                dir=log_dir + f"{tgt_tbl}_{idx}.sql")

                cls.run_query_rs(conn=conn, sql=sql_prefix + sql)

    @classmethod
    def sequential_load_rs_wk(cls,
                              rptg_dates,
                              override,
                              tgt_tbl,
                              conn,
                              rptg_wk,
                              rptg_wk_start,
                              rptg_wk_end,
                              sql_loop_fn,
                              # filter dates set 1
                              filter_dt_field1=False,
                              filter_dt_type1=False,
                              filter_dt_placeholder1=False,
                              # filter dates set 2
                              filter_dt_field2=False,
                              filter_dt_type2=False,
                              filter_dt_placeholder2=False,
                              # filter dates set 3
                              filter_dt_field3=False,
                              filter_dt_type3=False,
                              filter_dt_placeholder3=False,
                              log_dir=False
                              ):
        """

        :param rptg_dates:
        :param override:
        :param tgt_tbl:
        :param conn:
        :param rptg_wk:
        :param rptg_wk_start:
        :param rptg_wk_end:
        :param sql_loop_fn:
        :param filter_dt_field1:
        :param filter_dt_type1:
        :param filter_dt_placeholder1:
        :param filter_dt_field2:
        :param filter_dt_type2:
        :param filter_dt_placeholder2:
        :param filter_dt_field3:
        :param filter_dt_type3:
        :param filter_dt_placeholder3:
        :param log_dir:
        :return:
        """

        # dropping table if override = True
        if override:
            print(f'''table override True: Dropping table: {tgt_tbl} ''')
            try:
                cls.run_query_rs(conn=conn, sql=f'''drop table {tgt_tbl}''')
            except:
                conn.commit()
                pass

        for idx, row in rptg_dates.iterrows():
            print(f'''{row['start_date']} to {row['end_date']}''')

            # date range column for logging
            sql = sql_loop_fn.replace(
                rptg_wk,
                f" '{row['rptg_wk']}' as rptg_wk,"
            )

            sql = sql.replace(
                rptg_wk_start,
                f" '{row['start_date']}' as rptg_wk_start,"
            )

            sql = sql.replace(
                rptg_wk_end,
                f" '{row['end_date']}' as rptg_wk_end,"
            )

            # date filters
            sql = sql.replace(
                filter_dt_placeholder1,
                f" AND date({filter_dt_field1}) > '{row['start_date']}' "
                f" AND date({filter_dt_field1}) <= '{row['end_date']}'"
            )

            # check for other date fields
            if filter_dt_placeholder2:
                if filter_dt_type2 == "range":
                    sql = sql.replace(
                        filter_dt_placeholder2,
                        f" AND date({filter_dt_field2}) > '{row['start_date']}' "
                        f" AND date({filter_dt_field2}) <= '{row['end_date']}'"
                    )
                elif filter_dt_type2 == "<=":
                    sql = sql.replace(
                        filter_dt_placeholder2,
                        f" AND date({filter_dt_field2}) <= '{row['end_date']}'"
                    )

            if filter_dt_placeholder3:
                if filter_dt_type3 == "range":
                    sql = sql.replace(
                        filter_dt_placeholder3,
                        f" AND date({filter_dt_field3}) > '{row['start_date']}' "
                        f" AND date({filter_dt_field3}) <= '{row['end_date']}'"
                    )
                elif filter_dt_type3 == "<=":
                    sql = sql.replace(
                        filter_dt_placeholder3,
                        f" AND date({filter_dt_field3}) <= '{row['end_date']}'"
                    )

            if idx == 0 and override:
                sql_prefix = f"CREATE TABLE {tgt_tbl} AS "
            else:
                sql_prefix = f"INSERT INTO {tgt_tbl} "

            Export.dump_sql(obj=sql_prefix + sql,
                            dir=log_dir + f"{tgt_tbl}_{idx}.sql")

            try:
                cls.run_query_rs(conn=conn, sql=sql_prefix + sql)
            except Exception as e:
                print(str(e))
                conn.commit()
                break


class Teradata:
    """
    Generic functions for Teradata SQL queries and ETL.

    .. image:: ../images_source/db_etl_tools/teradata.png
    """

    @classmethod
    def insert_td(cls, tbl, df, conn, batch_size=False, date_cols=False):
        """
        Executes an INSERT INTO statement for a given Pandas DataFrame.

        :param tbl: Teradata table name.
        :param df: Pandas DataFrame.
        :param conn: Teradata connection object.
        :param batch_size: Records to load per batch.
        :param date_cols: A list of date columns to convert to Pandas datetime.
        :return: Printed SQL statements for each step.
        """

        print(f"batch size: {batch_size}")

        if type(df) != type(pd.DataFrame()):
            print("Detected something other than a DataFrame\n     Please use a pandas DataFrame")
            raise TypeError('Unsupported object type!')

        if date_cols:
            # Convert columns to a date object for loading
            #     TD is picky, and wants 'YYYY-MM-DD' dates
            print('    ...Attempting to convert elligible columns to date')
            for idx, column in enumerate(date_cols):
                df[column] = pd.to_datetime(df[column], errors='ignore')

            date_columns = list(df.select_dtypes(include=[np.datetime64]).columns)
            print(f"         {len(date_columns)} date column(s) found")
            for column in date_columns:
                df[column] = df[column].dt.strftime('%Y-%m-%d')

        sql_vars = ('?, ' * (len(df.columns) - 1)) + '?'

        sql = f"insert into {tbl}  values({sql_vars})"
        data = df

        print("    ...Beginning bulk insert operation")
        if not batch_size:
            batch_size = 10000

        try:
            print(f"{len(range(0, int(np.floor(df.shape[0] / batch_size) + 1)))} batches found")
            for i in range(0, int(np.floor(df.shape[0] / batch_size) + 1)):
                data_sample = [tuple(x) for x in data.iloc[batch_size * i:batch_size * (i + 1), :].values]
                conn.executemany(sql, data_sample, batch=True)
                print(sql)
                print(f"        ...Completed batch {i} of {len(range(0, int(np.floor(df.shape[0] / batch_size) + 1)))}")
        except Exception as e:
            print(data.head())
            raise e
        print('    ...Successfully loaded Data into Teradata')
        return None

    @classmethod
    def run_query_td(cls, conn, sql):
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
        print(Fore.RED + f"Runtime: {tdelta}")

    @classmethod
    def make_tbl_td(cls, df, tbl_name):
        """
        Creates SQL to run a CREATE TABLE statement based on a Pandas DataFrame.

        :param df: Pandas DataFrame.
        :param tbl_name: Teradata table name.
        :return: CREATE TABLE SQL statement.
        """
        # fix columns
        df = cls.make_db_cols(df)

        # loop thru the columns
        for idx, col in enumerate(df):
            # find the max length of each field
            col_desc = col + "-" + str(df[col].map(lambda x: len(str(x))).max())
            # find the max value of each fields
            try:
                col_max = col + "-" + str(max(df[col]))
            except:
                col_max = col + "-" + 'NA'

            if idx == 0:
                col_desc_all = [col_desc]
                col_max_all = [col_max]
            else:
                col_desc_all.append(col_desc)
                col_max_all.append(col_max)

        # make df of column lengths
        col_desc_all = pd.DataFrame(col_desc_all)
        col_desc_all.columns = ["char"]
        col_desc_all['column'], col_desc_all['length'] = \
            col_desc_all['char'].str.split('-', 1).str

        # make df of column max
        col_max_all = pd.DataFrame(col_max_all)
        col_max_all.columns = ["char"]
        col_max_all['column'], col_max_all['max'] = \
            col_max_all['char'].str.split('-', 1).str

        # make df of column dtypes
        col_desc_types = pd.DataFrame(df.dtypes).reset_index()
        col_desc_types.columns = ["column", "type"]

        # join dfs
        col_desc_all = pd.merge(
            col_desc_all,
            col_desc_types,
            how="inner",
            on="column")

        col_desc_all = pd.merge(
            col_desc_all,
            col_max_all[["column", "max"]],
            how="inner",
            on="column")

        # define data type mapping (pandas --> teradata)
        d = {'object': 'VARCHAR',
             'int64': 'INTEGER',
             "Int64": "INTEGER",
             'int32': 'INTEGER',
             'bool': 'VARCHAR',
             'float64': 'FLOAT',
             'datetime64[ns]': 'DATE'}

        col_desc_all = col_desc_all.replace(d)

        # list the columns where you want to specify the lengths
        col_desc_all['concat'] = np.where(
            # if varchar, use the length of the longest char
            col_desc_all['type'] == "VARCHAR",
            col_desc_all['column'] + " " + \
            col_desc_all['type'].astype(str) + \
            "(" + col_desc_all['length'] + ")",
            col_desc_all['column'] + " " + \
            col_desc_all['type'].astype(str))

        # convert integers with a max val over certain amount to varchar
        for idx, row in col_desc_all.iterrows():
            if str(row['type']) == 'INTEGER' and row['max'] != "nan" and int(row['max']) > 2147483647:
                val = row['concat']

                col_desc_all.loc[idx, 'concat'] = \
                    val.replace(
                        " INTEGER",
                        f" VARCHAR({row['length']})")

        col_desc_all = col_desc_all.apply(', '.join).reset_index()
        col_desc_all.columns = ["index", "statement"]
        statement = col_desc_all[col_desc_all['index'] == 'concat']
        sql = statement['statement'].values
        sql = str(sql)
        sql = sql.replace("[", "")
        sql = sql.replace("]", "")
        sql = "CREATE TABLE " + tbl_name + " ( " + sql + " )"
        sql = sql.replace("'", "")

        return sql

    @classmethod
    def make_tbl_complete_td(cls, df, tbl_name, conn, batch_size=False):
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
        print(Fore.RED + f"Runtime: {tdelta}")
