"""
Database connections and engines.

|pic1| |pic2| |pic3| |pic4| |pic5|
    .. |pic1| image:: ../images_source/db_conn_tools/mysql.png
        :width: 20%
    .. |pic2| image:: ../images_source/db_conn_tools/oracle1.png
        :width: 20%
    .. |pic3| image:: ../images_source/db_conn_tools/postgres1.png
        :width: 20%
    .. |pic4| image:: ../images_source/db_conn_tools/redshift1.png
        :width: 20%
    .. |pic5| image:: ../images_source/db_conn_tools/teradata.png
        :width: 20%

"""

import teradata
import cx_Oracle
import psycopg2
from sqlalchemy import create_engine


class Oracle:
    """
    Oracle database connections.

    .. image:: ../images_source/db_conn_tools/oracle1.png
    """

    # Engines
    @classmethod
    def eng_oracle(cls, usr, pwd, dbname):
        """
        Create an Oracle database engine object using SqlAlchemy.

        :param usr: Oracle username.
        :param pwd: Oracle password.
        :return: Oracle database engine object.
        """
        eng = create_engine(
            'oracle://' + usr + ':' + pwd + f'@{dbname}'
            , encoding='utf-8', echo=False)

        return eng

    @classmethod
    def eng_oracle_addr(cls, usr, pwd, host, port, dbname):
        """
        Create an Oracle database engine object using JDBC. Requires cx_oracle package.

        :param usr: Oracle username.
        :param pwd: Oracle password.
        :return: Oracle database engine object.
        """
        eng = create_engine(
            f'''
            oracle+cx_oracle://{usr}:{pwd}@(DESCRIPTION =(SOURCE_ROUTE = YES)(ADDRESS_LIST =
            (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))
            (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))
            (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))
            (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))
            (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))
            (CONNECT_DATA =(SERVICE_NAME = {dbname})))      
            '''
        )

        return eng

    # Connections
    @classmethod
    def con_oracle(cls, usr, pwd, dbname):
        """
        Create an Oracle connection object.

        :param usr: Oracle username.
        :param pwd: Oracle password.
        :return: Oracle database connection object.
        """
        conn = cx_Oracle.connect(
            usr + "/" + pwd + f"@{dbname}",
            encoding="utf-8")

        return conn


class Postgres:
    """
    Postgres database connections.

    .. image:: ../images_source/db_conn_tools/postgres1.png
    """

    @classmethod
    def eng_postgres(cls, usr, pwd, port):
        """
        Create a Postgres database engine object.

        :param usr: A Postgres username.
        :param pwd: A Postgres password.
        :param port: Port for Postgres database.
        :return: A Postgres database connection.
        """
        engine = create_engine(
            f'postgresql://{usr}:' + \
            f"{pwd}" + \
            f'@localhost:{port}/{usr}')

        return engine

    @classmethod
    def con_postgres(cls, host, db, usr, pwd):
        """
        Create a Postgres database connection object.

        :param host: Hostname for Postgres database.
        :param db: Name of Postgres database.
        :param usr: Postgres username.
        :param pwd: Postgres password.
        :return: A Postgres database connection object.
        """
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=usr,
            password=pwd)

        cursor = conn.cursor()

        return cursor, conn


class TeraData:
    """
    Teradata database connections.

    .. image:: ../images_source/db_conn_tools/teradata.png

    """

    @classmethod
    def conn_td(cls, host):
        """
        Create a Teradata database connection object via Teradata Python package.

        :param host: Teradata hostname.
        :return: Returns a Teradata database connection object.
        """
        udaExec = teradata.UdaExec(appName="test", version="1.0", logConsole=False)
        conn = udaExec.connect(method="odbc", dsn=host, autocommit=True, transactionMode="Teradata")

        return conn

    @classmethod
    def conn_pyodbc(cls, drivername, host, usr, pwd):
        """
        Create a Teradata database connection object via ODBC.

        :param drivername: Teradata driver name for ODBC.
        :param host: Teradata hostname.
        :param usr: Teradata username.
        :param pwd: Teradata password.
        :return: Teradata database connection object.
        """
        link = f'DRIVER={drivername};DBCNAME={host};UID={usr};PWD={pwd}'
        conn = pyodbc.connect(link, autocommit=True)

        return conn

    @classmethod
    def conn_sa(cls, drivername, host, usr, pwd):
        """
        Create a Teradata database connection object via SqlAlchemy.

        :param drivername: Teradata database driver name for ODBC.
        :param host: Teradata database hostname.
        :param usr: Teradata username.
        :param pwd: Teradata password.
        :return: Teradata database connection object.
        """
        link = f'teradata://{usr}:{pwd}@{host}/?driver={drivername}'

        conn = create_engine(link)

        return conn


class Redshift:
    """
    Redshift database connections.

    .. image:: ../images_source/db_conn_tools/redshift1.png
    """

    @classmethod
    def conn_rs_sa(cls, db, host, port, user, pwd):
        """
        Create a Redshift database connection object via SqlAlchemy.

        :param db: Redshift database name.
        :param host: Redshift database host address.
        :param port: Redshift database post.
        :param user: Redshift username.
        :param pwd: Redshift password.
        :return: Redshift database connection object.
        """
        engine = create_engine(f"postgres://{user}:{pwd}@{host}:{port}/{db}", use_batch_mode=True)
        conn = engine.connect()

        return engine, conn

    @classmethod
    def conn_rs_pg(cls, db, host, port, user, pwd):
        """
        Create a Redshift database connection object via psycopg2 package.

        :param db: Redshift database name.
        :param host: Redshift database host address.
        :param port: Redshift database post.
        :param user: Redshift username.
        :param pwd: Redshift password.
        :return: Redshift database connection object.
        """
        conn = psycopg2.connect(f"dbname={db} host={host} port={port} user={user} password={pwd}")
        cursor = conn.cursor()

        return cursor, conn


class MySQL:
    """
    MySQL database connections.

    .. image:: ../images_source/db_conn_tools/mysql.png
    """

    @classmethod
    def eng_mysql(cls, usr, pwd):
        """
        Create a MySQL database engine object via SqlAlchemy.

        :param usr: MySQL username.
        :param pwd: MySQL password.
        :return: MySQL database engine object.
        """
        engine = create_engine(
            f'mysql+pymysql://{usr}:' + \
            f"{pwd}" + \
            f'@localhost', echo=False)

        return engine
