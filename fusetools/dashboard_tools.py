"""
Dashboards and data visualization applications.

|pic1|
    .. |pic1| image:: ../images_source/dashboard_tools/tableau1.png
        :width: 50%

"""

import os
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
    TableDefinition, escape_string_literal, TableName
import tableauserverclient as tsc


class Tableau:
    """
    Functions for interacting with the Tableau Server API.

    .. image:: ../images_source/dashboard_tools/tableau1.png
    """

    @classmethod
    def auth_tableau_server(cls, server_address, username, pwd):
        """
        Authenticates a connection to a Tableau Server domain.

        :param server_address: Tableau server domain address.
        :param username: Tableau server username.
        :param pwd: Tableau server password.
        :return: Tableau authentication object & Tableau server object.
        """

        tableau_auth = tsc.TableauAuth(username, pwd)
        server = tsc.Server(server_address=server_address)
        server.version = '2.0'
        server.auth.sign_in(auth_req=tableau_auth)
        return tableau_auth, server

    @classmethod
    def get_tableau_server_obj_id(cls, server, tableau_auth, obj_name):
        """
        Returns the name and object id for a requested object name.

        :param server: Tableau server domain address.
        :param tableau_auth: Authenticated Tableau server object.
        :param obj_name: Name to search for.
        :return: Name and object id for a requested object name.
        """

        # LIST DATA SOURCES
        with server.auth.sign_in(tableau_auth):
            endpoint = {
                'workbook': server.workbooks,
                'datasource': server.datasources,
                'view': server.views,
                'job': server.jobs,
                'project': server.projects,
                'webhooks': server.webhooks,
            }.get("project")

            # get the marketing analytics project folder
            for idx, resource in enumerate(tsc.Pager(endpoint.get)):
                # print(idx, resource.id, resource.name)
                if str(resource.name) == obj_name:
                    break

            return (resource.name, resource.id)

    @classmethod
    def make_tableau_datasource_schema(cls, df_schema):
        """
        Converts a Pandas DataFrame of column types & builds a Tableau schema list.

        :param df_schema: Pandas DataFrame of column types.
        :return: List of Tableau sqlType objects.
        """
        tableau_schema_cols = []
        for idx, row in df_schema.iterrows():
            if row['dtype_final'] in ["object", "dtype('O')"]:
                tableau_col_type = SqlType.text()
            elif row['dtype_final'] in ["datetime64[ns]"]:
                tableau_col_type = SqlType.date()
            elif row['dtype_final'] in ["float"]:
                tableau_col_type = SqlType.double()
            elif row['dtype_final'] in ["Int64"]:
                tableau_col_type = SqlType.int()
            else:
                tableau_col_type = SqlType.text()

            tableau_schema_cols.append(
                TableDefinition.Column(row['col'],
                                       tableau_col_type,
                                       NULLABLE)
            )

        return tableau_schema_cols

    @classmethod
    def make_tableau_hyperfile(cls, df, save_dir, hyperfile_name, tableau_schema_cols):
        """
        Creates a Tableau hyperfile (data source) to load to Tableau Server.

        :param df: Pandas DataFrame to be loaded to Tableau server.
        :param save_dir: Local filepath to save Pandas DataFrame as CSV.
        :param hyperfile_name: Filename for hyperfile.
        :param tableau_schema_cols: List of columns for the hyperfile with designated Tableau data types.
        :return: Tableau hyperfile.
        """
        os.chdir(save_dir)
        # CREATE A TDE DATA SOURCE (LOCALLY)
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            # Creates a new Hyper file or replaces the file if it already exists.
            with Connection(endpoint=hyper.endpoint,
                            database=hyperfile_name,
                            create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                # create the schema
                connection.catalog.create_schema('Extract')
                # create the table definition
                # it's require to name the table as Extract, if you want to upload .hyper file with Tableau API automatically
                guardrails_table = TableDefinition(
                    table_name=TableName('Extract', 'Extract'),
                    columns=tableau_schema_cols
                )
                # create the table in the connection catalog
                connection.catalog.create_table(guardrails_table)

                # # create a path that locates CSV file
                df.to_csv(f"{save_dir}.csv", index=False, encoding='utf-8', na_rep='NULL')

                # load all rows into table from the CSV file
                # `execute_command` executes a SQL statement and returns the impacted row count

                print(f"COPY {guardrails_table.table_name} "
                      f"from {escape_string_literal(save_dir)} "
                      f"with (format csv, NULL 'NULL', delimiter ',', header)")

                count_in_customer_table = \
                    connection.execute_command(
                        command=f"COPY {guardrails_table.table_name} "
                                f"from {escape_string_literal(save_dir + '.csv')} "
                                f"with (format csv, NULL 'NULL', delimiter ',', header)")

                print(f"The number of rows in table {guardrails_table.table_name} "
                      f"is {count_in_customer_table}.")

    @classmethod
    def push_tableau_hyperfile(cls, hyperfile_path, server, tableau_auth, project_folder_id):
        """
        Pushes a Tableau hyperfile to Tableau Server to create a data source.

        :param hyperfile_path: Path to Tableau hyperfile.
        :param server: Tableau server object.
        :param tableau_auth: Tableau authentication object.
        :param project_folder_id: ID of Tableau server project folder.
        :return: Data source object for loaded hyperfile.
        """

        tde_file_path = rf'{hyperfile_path}'

        with server.auth.sign_in(auth_req=tableau_auth):
            # Use the project id to create new datsource_item
            new_datasource = tsc.DatasourceItem(project_folder_id)
            # code to get project location and create DatasourceItem
            try:
                new_datasource = server.datasources.publish(datasource_item=new_datasource,
                                                            file_path=tde_file_path,
                                                            mode="Overwrite",
                                                            )
                print("TDE loaded")
            except tsc.server.endpoint.exceptions.ServerResponseError:
                print("overwrite method failed, trying diff method...")
                overwrite_true = tsc.Server.PublishMode.Overwrite
                new_datasource = server.datasources.publish(datasource_item=new_datasource,
                                                            file_path=tde_file_path,
                                                            mode=overwrite_true,
                                                            )
                print("TDE loaded")
                return new_datasource

    @classmethod
    def get_all_sever_items(cls, server, tableau_auth):
        """
        Retrieves all projects on an organization's Tableau server account.

        :param server: Tableau server object.
        :param tableau_auth: Tableau authentication object.
        :return: Projects on an organization's Tableau server account.
        """
        with server.auth.sign_in(tableau_auth):
            # get all projects on site
            all_project_items, pagination_item = server.projects.get()

            return all_project_items, pagination_item
