"""
DataFrame & SQL Tools for Analytics.

|pic1|
    .. |pic1| image:: ../images_source/pandas1.png
        :width: 50%
"""

import pandas as pd
from fusetools.date_tools import get_rptg_yr, get_rptg_week


class SQL:
    """
    Functions for running analytical SQL operations

    """

    @classmethod
    def sql_rs_yoy_cum_comp(cls, tbl_name, time_col, year_col, fact_cols,
                            agg_func=False, dim_cols=False,
                            date_join_tbl=False, date_join_col=False,
                            date_join_time_start_col=False,
                            date_join_time_end_col=False):
        """
        Performs a cumulative aggregation over the year as well as provided dimensional columns
        OR if agg_func param is not provided,

        :param tbl_name: Table with data to perform calculation on
        :param time_col: Column with sub-year time granularity to compare across years
        :param year_col: Column with year time granularity
        :param fact_cols: List of KPI columns to calculate
        :param agg_func: Type of aggregation to perform, if not provided will just do a snapshot of current week vs current week
        :param dim_cols: List of dimensional columns to compare aggregation over (optional)
        :param date_join_tbl: Table with date columns to join to (optional)
        :param date_join_col: Column from date table to join on (optional)
        :param date_join_time_start_col: Column from date table with time granularity start date
        :param date_join_time_end_col: Column from date table with time granularity end date
        :return: Analytical SQL query
        """

        if date_join_tbl:
            date_join_placeholder_curr = f'''
           left join {date_join_tbl} dt_curr
           on dt_curr.{date_join_col} = cast(rs1.{year_col}||lpad(rs1.{time_col},2,0) as integer)
           '''

            date_join_placeholder_prior = f'''
           left join {date_join_tbl} dt_prior
           on dt_prior.{date_join_col} = cast(rs2.{year_col}||lpad(rs2.{time_col},2,0) as integer)
           '''

            date_join_cols_curr = [f"date(dt_curr.{date_join_time_start_col}) + 1 as start_date_curr",
                                   f"date(dt_curr.{date_join_time_end_col}) as end_date_curr", ""]

            date_join_cols_prior = [f"date(dt_prior.{date_join_time_start_col}) + 1 as start_date_prior",
                                    f"date(dt_prior.{date_join_time_end_col}) as end_date_prior", ""]
        else:
            date_join_placeholder_curr = ""
            date_join_cols_curr = ""
            date_join_placeholder_prior = ""
            date_join_cols_prior = ""

        if dim_cols:
            dim_cols1 = str(dim_cols).replace("[", "", ).replace("]", "").replace("'", "")
            dim_cols_fact = "," + dim_cols1
        else:
            dim_cols1 = ""
            dim_cols_fact = ""

        if agg_func:
            fact_cols1 = [
                f"{agg_func}({f}) over (partition by {year_col} {dim_cols_fact} order by {time_col} rows unbounded preceding) as {f}"
                for f in fact_cols]
            fact_cols2 = [
                f"{agg_func}({f}) as {f}" for f in fact_cols
            ]
        else:
            fact_cols1 = fact_cols

        sql1 = f'''
       with rs as (
           select
           'Cumulative' as calc_type,
           {dim_cols1 + ","}
           {time_col},
           {year_col},
           {str(fact_cols1).replace("[", "").replace("]", "").replace("'", "")}
           from {tbl_name}

           UNION

           select
           'Snapshot' as calc_type,
           {dim_cols1 + ","}
           {time_col},
           {year_col},
           {str(fact_cols2).replace("[", "").replace("]", "").replace("'", "")}
           from {tbl_name}
           group by {time_col},{year_col},{dim_cols1}
       )'''
        if dim_cols:
            dim_col_placement = [f"rs1.{d}" for d in dim_cols]
            dim_col_joins = [" and rs1." + d + "=" + "rs2." + d for d in dim_cols]
            dim_col_joins = str(dim_col_joins).replace("[", "").replace("]", "").replace("'", "").replace(",", "")
        else:
            dim_col_placement = ""
            dim_col_joins = ""

        fact_col_placement_curr = [f"rs1.{f} as {f}_curr" for f in fact_cols]
        fact_col_placement_prior = [f"rs2.{f} as {f}_prior" for f in fact_cols]

        sql2 = f'''
       select
       current_date as run_date,
       rs1.calc_type,
       {str(dim_col_placement).replace("[", "").replace("]", "").replace("'", "")},
       rs1.{year_col} as year_col_curr,
       rs1.{time_col} as time_col_curr,
       cast(rs1.{year_col}||lpad(rs1.{time_col},2,0) as integer) as yeartime_curr,
       {str(date_join_cols_curr).replace("[", "").replace("]", "").replace("'", "")}
       {str(fact_col_placement_curr).replace("[", "").replace("]", "").replace("'", "").replace("'", "")},
       rs2.{year_col} as year_col_prior,
       rs2.{time_col} as time_col_prior,
       cast(rs2.{year_col}||lpad(rs2.{time_col},2,0) as integer) as yeartime_prior,
       {str(date_join_cols_prior).replace("[", "").replace("]", "").replace("'", "")}
       {str(fact_col_placement_prior).replace("[", "").replace("]", "").replace("'", "").replace("'", "")}
       from rs rs1
       left join rs rs2
       on rs1.calc_type = rs2.calc_type
       and rs1.week = rs2.week
       and rs1.year = rs2.year + 1
       {dim_col_joins}
       {date_join_placeholder_curr}
       {date_join_placeholder_prior}
       order by calc_type, yeartime_curr desc
       '''

        sql3 = sql1 + sql2

        return sql3.replace("\n", " ")

    @classmethod
    def sql_rs_wow_comp(cls, tbl_name, time_col, fact_cols,
                        dim_cols=False,
                        date_join_tbl=False, date_join_col=False,
                        date_join_time_start_col=False,
                        date_join_time_end_col=False
                        ):
        """
        Performs a week over week comparison.

        :param tbl_name: Table with data to perform calculation on
        :param time_col: Column with sub-year time granularity to compare across years
        :param fact_cols: List of KPI columns to calculate
        :param dim_cols: List of dimensional columns to compare aggregation over (optional)
        :param date_join_tbl: Table with date columns to join to (optional)
        :param date_join_col: Column from date table to join on (optional)
        :param date_join_time_start_col: Column from date table with time granularity start date
        :param date_join_time_end_col: Column from date table with time granularity end date
        :return: Analytical SQL query
        """

        if date_join_tbl:
            date_join_placeholder_curr = f'''
           left join {date_join_tbl} dt_curr
           on dt_curr.{date_join_col} = dr1.{time_col}
           '''

            date_join_placeholder_prior = f'''
           left join {date_join_tbl} dt_prior
           on dt_prior.{date_join_col} = dr2.{time_col}
           '''

            date_join_cols_curr = [f"date(dt_curr.{date_join_time_start_col}) + 1 as start_date_curr",
                                   f"date(dt_curr.{date_join_time_end_col}) as end_date_curr", ""]

            date_join_cols_prior = [f"date(dt_prior.{date_join_time_start_col}) + 1 as start_date_prior",
                                    f"date(dt_prior.{date_join_time_end_col}) as end_date_prior", ""]
        else:
            date_join_placeholder_curr = ""
            date_join_cols_curr = ""
            date_join_placeholder_prior = ""
            date_join_cols_prior = ""

        if dim_cols:
            sql1 = f'''
           with dat_ranks as (
             select x.*,
             rank() over ( partition by {str(dim_cols).replace("[", "", ).replace("]", "").replace("'", "")} order by {time_col} desc) as time_col_rnk
             from {tbl_name} x
           )'''
            dim_col_placement = [f"dr1.{d}" for d in dim_cols]
            dim_col_joins = [" and dr1." + d + "=" + "dr2." + d for d in dim_cols]
            dim_col_joins = str(dim_col_joins).replace("[", "").replace("]", "").replace("'", "").replace(",", "")
        else:
            sql1 = f'''
               with dat_ranks as (
                 select x.*,
                 rank() over (order by {time_col} desc) as time_col_rnk
                 from {tbl_name} x
               )'''
            dim_col_placement = ""
            dim_col_joins = ""

        fact_col_placement_curr = [f"dr1.{f} as {f}_curr" for f in fact_cols]
        fact_col_placement_prior = [f"dr2.{f} as {f}_prior" for f in fact_cols]

        sql2 = f'''
       select
       current_date as run_date,
       {str(dim_col_placement).replace("[", "").replace("]", "").replace("'", "")},
       dr1.{time_col} as time_col_curr,
       dr1.time_col_rnk as time_col_rnk_curr,
       {str(date_join_cols_curr).replace("[", "").replace("]", "").replace("'", "")}
       {str(fact_col_placement_curr).replace("[", "").replace("]", "").replace("'", "")},
       dr2.{time_col} as time_col_prior,
       dr2.time_col_rnk as time_col_rnk_prior,
       {str(date_join_cols_prior).replace("[", "").replace("]", "").replace("'", "")}
       {str(fact_col_placement_prior).replace("[", "").replace("]", "").replace("'", "")}
       from dat_ranks dr1
       left join dat_ranks dr2
       on dr1.time_col_rnk = dr2.time_col_rnk - 1
       {dim_col_joins}
       {date_join_placeholder_curr}
       {date_join_placeholder_prior}
       order by dr1.time_col_rnk
       '''

        sql3 = sql1 + sql2
        return sql3.replace("\n", " ")


class Pandas:
    """
    Functions for running analytical Pandas operations

    """

    @classmethod
    def period_start_dt(cls, df):
        """
        Returns the first day of a year or month for a Pandas Series.

        :param df: Pandas DataFrame.
        :return: First day of year or month for Pandas Series.
        """
        df['year_start'] = pd.to_datetime(df['year'].astype(str) + "-01-" + "01")
        return df

    @classmethod
    def yoy_comp(cls, df, val_dict, dim=False, hist=False):
        """
        Computes a YoY cumulative YTD comparison across for a given week.

        :param df: Pandas DataFrame.
        :param val_dict: Column and aggregation type specification.
        :param dim: Dimension to group comparison by (Option).
        :param hist: Flag of whether to keep all historical date combinations.
        :return: Pandas DataFrame with YoY cumulative YTD comparison.
        """
        rptg_yr = get_rptg_yr()
        rptg_wk = get_rptg_week()[4:6]

        # current period
        df_sub_curr = df[
            (df['year'] == int(rptg_yr))
            &
            (df['week'] <= int(rptg_wk))
            ]

        ## make cumulative columns
        for k, v in val_dict.items():
            for idx, m in enumerate(v):
                if m == "sum":
                    if dim:
                        df_sub_curr[f'{k}_cumsum'] = df_sub_curr.groupby(dim)[k].cumsum()
                    else:
                        df_sub_curr[f'{k}_cumsum'] = df_sub_curr[k].cumsum()
                elif m == "ratio":
                    df_sub_curr[f'{k.replace("/", "_to_")}_ratio'] = df_sub_curr[k.split("/")[0]] / \
                                                                     df_sub_curr[k.split("/")[1]]

        # prior period
        df_sub_prior = df[
            (df['year'] == int(rptg_yr) - 1)
            &
            (df['week'] <= int(rptg_wk))
            ]

        ## make cumulative columns
        for k, v in val_dict.items():
            for idx, m in enumerate(v):
                if m == "sum":
                    if dim:
                        df_sub_prior[f'{k}_cumsum'] = df_sub_prior.groupby(dim)[k].cumsum()
                    else:
                        df_sub_prior[f'{k}_cumsum'] = df_sub_prior[k].cumsum()

                elif m == "ratio":
                    df_sub_prior[f'{k.replace("/", "_to_")}_ratio'] = df_sub_prior[k.split("/")[0]] / \
                                                                      df_sub_prior[k.split("/")[1]]

        if dim:
            if isinstance(dim, str):
                dim_ = [dim, "week"]
            else:
                dim.append("week")
                dim_ = dim

            if hist:
                df_sub_all = pd.merge(
                    # keep all weeks (historical)
                    df_sub_curr,
                    df_sub_prior,
                    how="left",
                    left_on=dim_,
                    right_on=dim_,
                    suffixes=["_curr", "_prior"]
                )
            else:
                df_sub_all = pd.merge(
                    # filter for current week only
                    df_sub_curr[df_sub_curr['week'] == max(df_sub_curr['week'])],
                    df_sub_prior,
                    how="left",
                    left_on=dim_,
                    right_on=dim_,
                    suffixes=["_curr", "_prior"]
                )
        else:
            if hist:
                df_sub_all = pd.merge(
                    # keep all weeks (historical)
                    df_sub_curr,
                    df_sub_prior,
                    how="left",
                    left_on=["week"],
                    right_on=["week"],
                    suffixes=["_curr", "_prior"]
                )
            else:
                df_sub_all = pd.merge(
                    # filter for current week only
                    df_sub_curr[df_sub_curr['week'] == max(df_sub_curr['week'])],
                    df_sub_prior,
                    how="left",
                    left_on=["week"],
                    right_on=["week"],
                    suffixes=["_curr", "_prior"]
                )

        # percentage changes
        for k, v in val_dict.items():
            if "/" in k:
                df_sub_all[f'{k.replace("/", "_to_")}_ratio_chg'] = \
                    (df_sub_all[f'{k.replace("/", "_to_")}_ratio_curr'] - \
                     df_sub_all[f'{k.replace("/", "_to_")}_ratio_prior']) / df_sub_all[
                        f'{k.replace("/", "_to_")}_ratio_curr']
            else:
                df_sub_all[k + "_cumsum_chg"] = \
                    (df_sub_all[k + "_cumsum_curr"] - df_sub_all[k + "_cumsum_prior"]) / df_sub_all[k + "_cumsum_prior"]

        return df_sub_all

    @classmethod
    def period_comp(cls, df, period_field, val_fields, dim=False, val_field_suffix=False, hist=False):
        """
        Creates a snapshot comparison between two periods.

        :param df: Pandas DataFrame.
        :param period_field: Column with period to compare across
        :param val_fields: List of columns with numeric values to compare
        :param dim: Column with dimension to group across (optional)
        :param val_field_suffix: Suffix for value field to add to final dataset (optional)
        :param hist: Include history flag, returns all periods if True, otherwise just the most recent two periods
        :return: Comparison Pandas DataFrame
        """

        if not val_field_suffix:
            val_field_suffix = ""
        # df[period_field] = pd.to_datetime(df[period_field])
        if dim:
            df['per_rank'] = df.groupby(dim)[period_field].rank(ascending=False)
            if hist:
                df_comp = pd.merge(
                    df,
                    df.assign(per_rank=df['per_rank'] - 1),
                    left_on=[dim, "per_rank"],
                    right_on=[dim, "per_rank"],
                    suffixes=["_1", "_2"]
                )

            else:
                df_comp = pd.merge(
                    df.query("per_rank == 1"),
                    df.query("per_rank == 2"),
                    left_on=dim,
                    right_on=dim,
                    suffixes=["_1", "_2"]
                )
        else:
            df['per_rank'] = df[period_field].rank(ascending=False)
            df['key'] = 1
            if hist:
                df_comp = pd.merge(
                    df,
                    df.assign(per_rank=df['per_rank'] - 1),
                    left_on="per_rank",
                    right_on="per_rank",
                    suffixes=["_1", "_2"]
                )
            else:
                df_comp = pd.merge(
                    df.query("per_rank == 1"),
                    df.query("per_rank == 2"),
                    left_on="key",
                    right_on="key",
                    suffixes=["_1", "_2"]
                )
        for idxx, val_field in enumerate(val_fields):
            df_comp[f'per_comp_{val_field}{val_field_suffix}'] = (df_comp[f'{val_field}_1'] - df_comp[
                f'{val_field}_2']) / \
                                                                 df_comp[
                                                                     f'{val_field}_2']

        return df_comp

    @classmethod
    def ptd_measure(cls, df, period, val_fields, kpi, dim=False):
        """
        Creates a 'Period to date' aggregation.

        :param df: Pandas DataFrame
        :param period: Type of period (year, month)
        :param val_fields: Columns to aggregate.
        :param kpi: Type of aggregation to perform.
        :param dim: Dimension to group comparison by (Optional).
        :return: Pandas DataFrame with PTD measure.
        """
        if period == "year":
            df['max_year'] = max(df['year'])
            df_sub = df[df['year'] == df['max_year']]
        elif period == "month":
            df['max_year'] = max(df['year'])
            df_sub = df[
                (df['year'] == df['max_year'])
            ]
            df_sub['max_month'] = max(df_sub['month'])
            df_sub = df_sub[
                (df_sub['month'] == df_sub['max_month'])
            ]

        ret = {}
        for idxx, val_field in enumerate(val_fields):
            print(val_field)

            if dim:
                ret.update({
                    val_field:
                        (df_sub
                         .groupby(dim)
                         .agg({val_field: kpi})  # [0]
                         )
                })
            else:
                ret.update({
                    val_field:
                        df_sub.agg({val_field: kpi})[0]
                })

    @classmethod
    def find_na_holder(cls, df, col, col_new):
        """
        Returns the combination of 2 de-duped columns in a Pandas DataFrame.

        :param df: Pandas DataFrame
        :param col: Original column.
        :param col_new: New column.
        :return: Combination of 2 de-duped columns in a Pandas DataFrame.
        """
        return [col, (
            df[pd.isnull(df[col])][col_new]
                .drop_duplicates()
                .reset_index())[col_new][0]]

    @classmethod
    def append_window_agg(cls, df, dim, metric, metric_agg, comp_col=False):
        """
        Joins a window function's aggregation to a Pandas DataFrame.

        :param df: Pandas DataFrame.
        :param dim: Column for which to partition of data by.
        :param metric: Column to aggregate.
        :param metric_agg: Type of calculation to perform.
        :param comp_col: Flag of whether or not to create a comparison column.
        :return: Pandas DataFrame with a window function's aggregation.
        """

        agg = (df
               .groupby(dim)
               .agg({metric: metric_agg})
               .reset_index()
               .rename(columns={metric: f'''{dim}_{metric}'''})
               )
        df2 = pd.merge(
            df,
            agg,
            how="inner",
            left_on=dim,
            right_on=dim
        )
        if comp_col:
            df2[f'''{dim}_{metric}_pct'''] = df2[metric] / df2[f'''{dim}_{metric}''']
        return df2
