"""
Marketing, Advertising, Campaign & Analytics Tools.

|pic1|
    .. |pic1| image:: ../images_source/marketing_tools/googleanalytics1.png
        :width: 40%

"""

# https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/
import json
import os

import numpy as np
import requests
from apiclient.discovery import build
from more_itertools import flatten
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
import time


class GoogleAnalytics:
    """
    Functions for interacting with Google Analytics.

    .. image:: ../images_source/marketing_tools/googleanalytics1.png

    """

    @staticmethod
    def authorize_credentials(filename):
        """

        Initializes an Analytics Reporting API V4 service object.

        :param filename: Filepath to authentication JSON token.
        :return: An authorized Analytics Reporting API V4 service object.
        """
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            filename=filename,
            scopes=['https://www.googleapis.com/auth/analytics.readonly'])

        # Build the service object.
        creds = build('analyticsreporting', 'v4', credentials=credentials)

        return creds

    @classmethod
    def ga_pull_metrics(cls, creds, view_id,
                        start_date, end_date,
                        metrics,
                        dimensions=False,
                        filter_dimensions=False,
                        filter_operators=False,
                        filter_expressions=False,
                        filter_excludes=False):
        """
        Generates a Google Analytics report.

        :param creds: Authentication object.
        :param view_id: Id for organization's Google Analytics account.
        :param start_date: Reporting start date.
        :param end_date: Reporting end date.
        :param metrics: List of KPIs to compute. Docs: https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/.
        :param dimensions: List of Dimensions to aggregate results by. Docs: https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/.
        :param filter_dimensions: List of Dimensions to filter results by.
        :param filter_operators: List of arithmetical operators to constraint dimensions by.
        :param filter_expressions: List of expressions to constraint dimensions by.
        :param filter_excludes: Whether or not use dimension filter to exclude results.
        :return: JSON response with report results.
        """

        # define query object with start, end and view_id
        body = {
            'reportRequests': [
                {
                    'viewId': view_id,
                    'dateRanges': [{'startDate': start_date,
                                    'endDate': end_date}]
                }]}

        # add metrics in list
        metrics_val = [{'expression': m} for m in metrics]
        body.get("reportRequests")[0]['metrics'] = \
            metrics_val

        # add dimensions in list
        if dimensions:
            dimensions_val = [{'name': n} for n in dimensions]
            body.get("reportRequests")[0]['dimensions'] = \
                dimensions_val

        # add filters in list
        if filter_dimensions and filter_operators and filter_expressions:

            # create a pandas dataframe of the filter conditions
            try:
                filters_df = pd.DataFrame({
                    "filter_dimensions": filter_dimensions,
                    "filter_operators": filter_operators,
                    "filter_expressions": filter_expressions,
                    "filter_excludes": filter_excludes
                })

                filters_df['filter_excludes'] = np.where(
                    filters_df['filter_excludes'] == False,
                    "false",
                    "true"
                )

            except Exception as e:
                print(str(e))

            # loop through pandas df of filter conditions
            # construct a list of the filter conditions to pass to query object
            filters_val = []
            for idx, row in filters_df.iterrows():
                filters_val_sub = \
                    {
                        "filters": [{"dimensionName": row['filter_dimensions'],
                                     "operator": row['filter_operators'],
                                     "expressions": row['filter_expressions'],
                                     "not": row['filter_excludes']
                                     }]
                    }
                filters_val.append(filters_val_sub)

            body.get("reportRequests")[0]['dimensionFilterClauses'] = \
                filters_val

        # execute query object
        r = creds.reports().batchGet(body=body).execute()

        if dimensions:

            try:

                # loop thru result object
                for idx, elem in enumerate(r.get("reports")[0].get("data").get("rows")):

                    # extract dimensions for dim df
                    for idxd, dim in enumerate(dimensions):
                        if idxd == 0:
                            dim_df = pd.DataFrame({
                                f'dim_{idxd}': [str(elem.get("dimensions")[idxd])]
                            })
                        else:
                            dim_df[f'dim_{idxd}'] = \
                                [str(elem.get("dimensions")[idxd])]

                    # extract metrics for metric df
                    for idxm, dim in enumerate(metrics):
                        if idxm == 0:
                            metric_df = pd.DataFrame({
                                f'metric_{idxm}': [str(elem.get("metrics")[0].get("values")[idxm])]
                            })
                        else:
                            metric_df[f'metric_{idxm}'] = \
                                [str(elem.get("metrics")[0].get("values")[idxm])]

                    # rename cols
                    dim_df.columns = dimensions
                    metric_df.columns = metrics

                    # concatenate
                    final_df = pd.concat([
                        dim_df,
                        metric_df
                    ], axis=1)

                    if idx == 0:
                        final_df_all = final_df
                    else:
                        final_df_all = pd.concat([
                            final_df_all,
                            final_df
                        ])
            except:
                final_df_all = pd.DataFrame()
        else:
            final_df_all = pd.DataFrame()

        return final_df_all, r


class AppAnnie:
    """

    """

    @classmethod
    def pull_metrics(self, api_keys, start_date, end_date):
        base_url = "https://api.appannie.com/v1.3/accounts"

        # get all combinations for request counter
        # lists
        api_keys_list_f = []
        acct_list_all_f = []
        acct_name_list_all_f = []
        mkt_list_all_f = []

        # loop thru api keys
        for idx, api_key in enumerate(api_keys):
            # if idx == 0:
            #     break
            # get accounts
            acct_list_all = []
            mkt_list_all = []
            acct_list_name_all = []
            product_list_all2 = []
            headers = {"Authorization": "Bearer " + api_key}
            r = requests.get(url=base_url, headers=headers)
            r_json = json.loads(r.content)

            acct_list = r_json.get("accounts")

            acct_list_all.append([x.get("account_id") for x in acct_list])
            acct_list_all = list(flatten(acct_list_all))

            acct_list_name_all.append([x.get("account_name") for x in acct_list])
            acct_list_name_all = list(flatten(acct_list_name_all))

            mkt_list_all.append([x.get("market") for x in acct_list])
            mkt_list_all = list(flatten(mkt_list_all))

            acct_list_all_f.append(acct_list_all)
            acct_name_list_all_f.append(acct_list_name_all)
            mkt_list_all_f.append(mkt_list_all)

            acct_list_all_f2 = list(flatten(acct_list_all_f))
            acct_name_list_all_f2 = list(flatten(acct_name_list_all_f))
            mkt_list_all_f2 = list(flatten(mkt_list_all_f))

            df_api_acct_mkt = \
                pd.DataFrame({
                    "acct_id": acct_list_all_f2,
                    "acct_name": acct_name_list_all_f2,
                    "mkt": mkt_list_all_f2
                })

            df_api_acct_mkt['api_key'] = [api_key] * len(acct_name_list_all_f2)

            if idx == 0:
                df_api_acct_mkt_all = df_api_acct_mkt
            else:
                df_api_acct_mkt_all = pd.concat([
                    df_api_acct_mkt_all,
                    df_api_acct_mkt
                ])

        df_api_acct_mkt_all.reset_index(drop=True, inplace=True)

        # products
        df_api_acct_mkt_all['product_ids'] = ""
        for idx, row in df_api_acct_mkt_all.iterrows():
            url = f"{base_url}/{row['acct_id']}/products"
            headers = {"Authorization": "Bearer " + row["api_key"]}
            print(headers)
            r = requests.get(url=url, headers=headers)
            r_json = json.loads(r.content)
            print(r_json)
            product_list = r_json.get("products")
            if product_list:
                product_list = [x.get("product_id") for x in product_list]
                df_api_acct_mkt_all["product_ids"].iloc[idx] = product_list
            else:
                continue

        df_api_acct_mkt_all = df_api_acct_mkt_all[df_api_acct_mkt_all['product_ids'] != ""].reset_index(drop=True)
        df_api_acct_mkt_exp = df_api_acct_mkt_all.explode('product_ids').reset_index(drop=True)
        df_api_acct_mkt_exp.dropna(how="any", inplace=True)

        accts_ = []
        products_ = []
        platforms_ = []
        dates2_ = []
        units2_ = []

        for idx, row in df_api_acct_mkt_exp.iterrows():
            time.sleep(2)
            print(f"progress...{idx / df_api_acct_mkt_exp.shape[0]}")

            headers = {"Authorization": "Bearer " + row['api_key']}
            break_down = "date"
            url1 = f'''{base_url}/{row['acct_id']}/products/{row['product_ids']}'''
            url2 = f'''{url1}/sales?break_down={break_down}&start_date={start_date}&end_date={end_date}'''

            try:
                r = requests.get(url=url2, headers=headers)
            except Exception as e:
                print(str(e))
                time.sleep(5)

            if "error" in str(r.content):
                continue

            r_json = json.loads(r.content)
            dates_ = []
            units_ = []

            for idxx, date in enumerate(r_json.get("sales_list")):
                dates_.append(date.get("date"))
                units_.append(date.get("units").get("product").get("downloads"))

            accts_.append([row['acct_id']] * len(dates_))
            products_.append([row['product_ids']] * len(dates_))
            platforms_.append([r_json.get("market")] * len(dates_))
            dates2_.append(dates_)
            units2_.append(units_)

        accts_f = list(flatten(accts_))
        products_f = list(flatten(products_))
        dates2_f = list(flatten(dates2_))
        units2_f = list(flatten(units2_))
        platforms_f = list(flatten(platforms_))

        df_result = pd.DataFrame({
            "acct": accts_f,
            "product": products_f,
            "platform": platforms_f,
            "dates": dates2_f,
            "units": units2_f
        })

        df_result['run_date'] = datetime.now().strftime("%Y-%m-%d")

        print((df_result
            .groupby(['acct', 'product', 'platform'])
            .agg(
            max_d=("dates", "max"),
            unit_n=("units", "sum")
        )).reset_index(drop=False))

        return df_result


