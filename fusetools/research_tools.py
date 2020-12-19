from urllib.request import urlopen

import requests
from geopy.distance import vincenty
from geopy.geocoders import Nominatim
import json
import pandas as pd
import time

from pandas.tseries.offsets import MonthEnd


class Economics:
    """
    Functions for dealing with Economic data sources.

    """

    @classmethod
    def bls_query(cls, series_id, start_year, end_year, api_key):
        """
        Retrieves Consumer Price Index figures from the Bureau of Labor Statistics API.

        :param series_id: Unique ID for a geography and an economic measure.
        :param start_year: Starting year for query.
        :param end_year: Ending year for query.
        :param api_key: API key for BLS.
        :return: JSON response for API call.
        """
        headers = {'Content-type': 'application/json'}

        data = json.dumps({
            "seriesid": [series_id],
            "startyear": start_year, "endyear": end_year
            ,
            "registrationkey": api_key
        })

        p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
        json_data = json.loads(p.text)

        return json_data

    @classmethod
    def bls_query_lookup(cls, lookup_df, lookup_area_type, start_year, end_year, api_keys):
        """
        Executes a series of queries against the BLS database using a DataFrame of lookups.

        :param lookup_df: Pandas DataFrame of BLS lookup codes.
        :param lookup_area_type: Type of area to lookup data for.
        :param start_year: Starting year for query.
        :param end_year: Ending year for query.
        :param api_keys: API key for BLS.
        :return: Pandas DataFrame of responses for all queried lookup codes.
        """

        headers = {'Content-type': 'application/json'}
        lookup_df['results_log'] = ""
        lookup_df['results_time'] = ""
        lookup_df['results_val'] = ""
        lookup_df['lookup_area_type'] = lookup_area_type
        key_num = 0

        for idx, chunk in lookup_df.iterrows():
            use_key = api_keys[key_num]
            print(f'''progress...{idx / len(lookup_df)}''')
            data = json.dumps({
                "seriesid": [chunk['q_code']],
                "startyear": start_year, "endyear": end_year
                ,
                "registrationkey": use_key
            })

            p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
            json_data = json.loads(p.text)
            print(json_data.get("message"))
            time.sleep(1.5)

            # if the response failed, try again
            if not json_data:
                print(f"No response on {chunk['q_code']}")
                time.sleep(3.5)
                p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
                json_data = json.loads(p.text)

            # if the api exceeded the daily limit (500 calls), switch to a new key
            elif json_data.get("message") and "daily threshold" in json_data.get("message")[0]:
                # try switching keys if we pass the daily limit
                print(f'''Switching API key''')
                key_num += 1
                use_key = api_keys[key_num]
                data = json.dumps({
                    "seriesid": [chunk['q_code']]
                    ,
                    "startyear": start_year, "endyear": end_year
                    ,
                    "registrationkey": use_key
                })
                p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
                json_data = json.loads(p.text)
            try:
                lookup_df.loc[idx, "results_log"] = json_data.get("status")
                years_ = [x.get("year") for x in json_data.get("Results").get("series")[0].get("data")]
                periods_ = [x.get("period") for x in json_data.get("Results").get("series")[0].get("data")]
                period_names_ = [x.get("periodName") for x in json_data.get("Results").get("series")[0].get("data")]
                values_ = [x.get("value") for x in json_data.get("Results").get("series")[0].get("data")]

                df_ret = pd.DataFrame({
                    "q_code": chunk['q_code'],
                    "code": chunk['code'],
                    "year": years_,
                    "period": periods_,
                    "period_name": period_names_,
                    "value": values_
                })

                df_ret['status'] = "success"

            except:

                df_ret = pd.DataFrame({
                    "q_code": chunk['q_code'],
                    "code": chunk['code'],
                    "year": [],
                    "period": [],
                    "period_name": [],
                    "value": []
                })

                df_ret['status'] = "fail"

            if idx == 0:
                df_ret_all = df_ret.copy()
            else:
                df_ret_all = pd.concat([df_ret_all, df_ret])

            print(f'''data size: {df_ret_all.shape[0]}''')

        # calculate start of the month
        df_ret_all['month_start'] = pd.to_datetime(
            df_ret_all['year'] + \
            "-" + \
            df_ret_all['period'].str[-2:] + "-01"
        )
        # calculate end of the month
        df_ret_all['month_end'] = df_ret_all['month_start'] + MonthEnd(1)

        return df_ret_all

    @classmethod
    def bea_gdp(cls, api_key, tbl_name="SQGDP1"):
        """
        Performs a query against the BEA API.

        :param api_key: BEA API.
        :param tbl_name: Name of data table to query from BEA database.
        :return: Pandas DataFrame of responses.
        """
        r = requests.get(
            f'''
        https://apps.bea.gov/api/data/?&
        UserID={api_key}&
        method=GetData&
        datasetname=Regional&TableName={tbl_name}&
        LineCode=3&
        GeoFIPS=STATE&
        ResultFormat=JSON'''.replace("\n", "").strip()
        )
        data_values = [x.get("DataValue") for x in json.loads(r.content).get("BEAAPI").get("Results").get("Data")]
        time_periods = [x.get("TimePeriod") for x in json.loads(r.content).get("BEAAPI").get("Results").get("Data")]
        geo_names = [x.get("GeoName") for x in json.loads(r.content).get("BEAAPI").get("Results").get("Data")]
        cl_units = [x.get("CL_UNIT") for x in json.loads(r.content).get("BEAAPI").get("Results").get("Data")]
        unit_mults = [x.get("UNIT_MULT") for x in json.loads(r.content).get("BEAAPI").get("Results").get("Data")]

        df = \
            pd.DataFrame(
                {
                    "data_values": data_values,
                    "time_periods": time_periods,
                    "geo_names": geo_names,
                    "cl_units": cl_units,
                    "unit_mults": unit_mults
                })

        return df


class Geography:
    """
    Functions for dealing with Geographical tasks.

    """

    @classmethod
    def get_city_lat_lon(cls, city):
        """
        Calculates the latitude and longitude for a city.

        :param city: Name of city.
        :return: Location (latitude and longitude).
        """
        geolocator = Nominatim()
        loc = geolocator.geocode(city)
        return loc

    @classmethod
    def calculate_distance(cls, lat_from, lon_from, lat_to, lon_to):
        """
        Calculates the distance between two latitude/longitude coordinate pairs.

        :param lat_from: Latitude of the point being compared from.
        :param lon_from: Longitude of the point being compared from.
        :param lat_to: Latitude of the point being compared to.
        :param lon_to: Longitude of the point being compared to.
        :return: Calculated distance.
        """
        coords_from = (lat_from, lon_from)
        coords_to = (lat_to, lon_to)
        try:
            dist = vincenty(coords_from, coords_to).miles
        except Exception as e:
            print(str(e))

        return dist

    @classmethod
    def walk_bike_transit_score(cls, addr, lat, lon, api_key):
        """

        :param addr:
        :param lat:
        :param lon:
        :param api_key:
        :return:
        """
        addr_base = "http://api.walkscore.com/score?format=json&address="
        addr_base = addr_base + addr + \
                    "&lat=" + lat + "&lon=" + lon + \
                    "&transit=1" + "&bike=1" + "&wsapikey=" + api_key
        try:
            jsonurl = urlopen(addr_base)
            text = json.loads(jsonurl.read())
            scores_df = pd.DataFrame.from_dict(text)
            bike_score = scores_df.loc["score", "bike"]
            transit_score = scores_df.loc["score", "transit"]
            walk_score = scores_df.loc["score", "walkscore"]

            del scores_df
        except Exception as e:
            print(str(e))
            pass

        return bike_score, transit_score, walk_score
