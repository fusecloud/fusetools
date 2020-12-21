"""
Functions for interacting with Machine Learning Tools.

|pic1| |pic2|
    .. |pic1| image:: ../images_source/stat_tools/scipy.png
        :width: 30%
    .. |pic2| image:: ../images_source/stat_tools/statsmodels.png
        :width: 30%

"""
import six
from fusetools.text_tools import Blob
import numpy as np
import pandas as pd
# price elasticity
import statsmodels.api as sm
# optimization
# survival
from lifelines.statistics import logrank_test
from scipy.stats import beta, chi2_contingency
## poisson test
from scipy.stats import binom_test
## z-score converter
from scipy.stats import norm
## t test
from scipy.stats import ttest_ind
## sample size
## chisquared
from statsmodels.stats.proportion import proportions_chisquare
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import gridspec
import matplotlib.ticker as mtick
import os


class Desc:
    """
    Functions for helping with Descriptive statistical tasks.

    """

    @classmethod
    def make_tbl(cls, width, height, df, title, font_size):
        """
        Creates a plot of a data table.

        :param width: Width of table.
        :param height: Height of table.
        :param df: Pandas DataFrame.
        :param title: Title of plot.
        :param font_size: Font size.
        :return: Plot of a data table.
        """
        import matplotlib.pyplot as plt
        import six
        import seaborn as sns
        current_palette_7 = sns.color_palette("Set1", 2)
        sns.set_palette(current_palette_7)
        fig = plt.figure(figsize=(width, height))
        ax1 = plt.subplot()
        ax1.axis('off')
        font_size = font_size
        header_color = '#40466e'
        row_colors = ['#f1f1f2', 'w']
        edge_color = 'w'
        bbox = [0, 0, 1, 1]
        header_columns = 0

        mpl_table = ax1.table(cellText=df.values,
                              bbox=bbox, colLabels=df.columns)

        mpl_table.auto_set_font_size(False)
        mpl_table.set_fontsize(font_size)

        for k, cell in six.iteritems(mpl_table._cells):
            cell.set_edgecolor(edge_color)
            if k[0] == 0 or k[1] < header_columns:
                cell.set_text_props(weight='bold', color='w', wrap=True)
                cell.set_facecolor(header_color)
            else:
                cell.set_facecolor(row_colors[k[0] % len(row_colors)])

        ax1.set_title(title);

    @classmethod
    def group_stats(cls, df, dim_cols, agg_dict):
        """
        Creates a Pandas DataFrame with specified aggregations over specified dimensions.

        :param df: Pandas DataFrame.
        :param dim_cols: List of columns to group by.
        :param agg_dict: Dictionary of columns and calculations to perform.
        :return: Pandas DataFrame of calculated results.
        """
        df1 = (df
               .groupby(dim_cols)
               .agg(agg_dict)
               .reset_index(inplace=True)
               )

        return df1


class Test:
    """
    Functions for implementing Statistical tests.

    """

    @classmethod
    def ttest(cls, df, grp_col, grp_1_flag, grp_2_flag, target_kpi):
        """
        Performs a t-test between two groups split by a flag.

        :param df: Pandas DataFrame containing data.
        :param grp_col: Column used to group the data.
        :param grp_1_flag: Value used to distinguish group 1.
        :param grp_2_flag: Value used to distinguish group 2.
        :param target_kpi: Column for the target metric to compare test across groups.
        :return: T-Test p-value.
        """
        a = df[df[grp_col] == grp_1_flag]
        a = a[[target_kpi]]
        b = df[df[grp_col] == grp_2_flag]
        b = b[[target_kpi]]
        t, p = ttest_ind(a, b, equal_var=False)
        return p

    @classmethod
    def ttest_result(
            cls,
            sample1_dat_ttest,
            sample2_dat_ttest):
        """
        Performs a T-Test between two groups of data.

        :param sample1_dat_ttest: Sample 1 dataset.
        :param sample2_dat_ttest: Sample 2 dataset.
        :return: T-Test p-value.
        """
        p1, p2 = Blob.text_parse(sample1_dat_ttest, sample2_dat_ttest)
        t, p = ttest_ind(p1, p2, equal_var=False)

        return p

    @classmethod
    def cramers_corrected_stat(cls, cat_col1, cat_col2):
        """
        Calculates correlation between 2 categorical variables using Cramer's method.

        :param cat_col1: Categorical column 1.
        :param cat_col2: Categorical column 2.
        :return: Correlation between 2 categorical variables using Cramer's method.
        """

        # https://stackoverflow.com/questions/20892799/using-pandas-calculate-cram%C3%A9rs-coefficient-matrix
        confusion_matrix = pd.crosstab(cat_col1, cat_col2)
        chi2, chi2_pval = \
            chi2_contingency(confusion_matrix)[0], chi2_contingency(confusion_matrix)[1]
        n = confusion_matrix.sum().sum()
        phi2 = chi2 / n
        r, k = confusion_matrix.shape
        phi2corr = max(0, phi2 - ((k - 1) * (r - 1)) / (n - 1))
        rcorr = r - ((r - 1) ** 2) / (n - 1)
        kcorr = k - ((k - 1) ** 2) / (n - 1)

        return np.sqrt(phi2corr / min((kcorr - 1), (rcorr - 1))), chi2_pval

    @classmethod
    def sample_size1(cls,
                     baseline_input,
                     effect_size_input,
                     significance_level_input,
                     statistical_power_input):
        """
        Calculates sample size needed for desired measuring effect size.

        :param baseline_input: Baseline rate to measure effect against against.
        :param effect_size_input: Desired effect size to measure.
        :param significance_level_input: Desired level of statistical significance.
        :param statistical_power_input: Desired level of statistical power.
        :return: Calculated sample size.
        """

        z = norm.isf([float(significance_level_input) / 2])  # two-sided t test
        zp = -1 * norm.isf([float(statistical_power_input)])
        d = (float(baseline_input) - float(effect_size_input))
        s = 2 * ((float(baseline_input) + float(effect_size_input)) / 2) * \
            (1 - ((float(baseline_input) + float(effect_size_input)) / 2))
        n = s * ((zp + z) ** 2) / (d ** 2)
        n = int(round(n[0]))

        return n

    @classmethod
    def chi_squared_result(cls,
                           sample1_successes,
                           sample1_trials,
                           sample2_successes,
                           sample2_trials):
        """
        Calculates correlation between 2 proportions using a Chi-Squared test..

        :param sample1_successes: Sample 1's successes.
        :param sample1_trials: Sample 1's trials.
        :param sample2_successes: Sample 2's successes.
        :param sample2_trials: Sample 2's successes.
        :return: Chi-Squared p-value.
        """

        successes = np.array([int(sample1_successes), int(sample2_successes)])
        trials = np.array([int(sample1_trials), int(sample2_trials)])
        result = proportions_chisquare(successes, trials)
        p = result[1]
        return p

    @classmethod
    def survival_result(cls,
                        data_type,
                        sample1_dat_survival=False,
                        sample2_dat_survival=False,
                        survival_confidence_level=False,
                        sample1_dat_survival_mean=False,
                        sample1_dat_survival_size=False,
                        sample2_dat_survival_mean=False,
                        sample2_dat_survival_size=False,
                        ):
        """
        Performs a survival test which tells if statistical difference in times until an outcome between two samples.

        :param data_type: Classification of whether data is in array/list data format or a scalar format (sample or other).
        :param sample1_dat_survival: Sample 1 data if array/list.
        :param sample2_dat_survival: Sample 1 data if array/list.
        :param survival_confidence_level: Confidence interval to assess measure test.
        :param sample1_dat_survival_mean: Sample 1 mean if scalar value.
        :param sample1_dat_survival_size: Sample 1 size if scalar value.
        :param sample2_dat_survival_mean: Sample 2 mean if scalar value.
        :param sample2_dat_survival_size: Sample 2 size if scalar value.
        :return: P-value for statistical significance in difference in times until an outcomes between two samples.
        """

        if data_type == "sample":

            p1, p2 = Blob.text_parse(sample1_dat_survival, sample2_dat_survival)
            x = logrank_test(p1, p2, alpha=float(survival_confidence_level))
            p = float(x.p_value)

        else:

            if float(sample1_dat_survival_mean) > float(sample2_dat_survival_mean):

                f1 = float(sample1_dat_survival_mean) / float(sample2_dat_survival_mean)
                df1 = 2 * float(sample1_dat_survival_size)
                df2 = 2 * float(sample2_dat_survival_size)

            else:
                f1 = float(sample2_dat_survival_mean) / float(sample1_dat_survival_mean)
                df1 = 2 * float(sample2_dat_survival_size)
                df2 = 2 * float(sample1_dat_survival_size)

            p = 2 * (1.0 - beta.cdf((df1 * f1) / (df1 * f1 + df2), df1 / 2, df2 / 2))

        return p

    @classmethod
    def poisson(cls,
                sample1_events,
                sample1_days,
                sample2_events,
                sample2_days):
        """
        Performs a Poisson test which tests statistical difference between groups comparing counts over a period of time.

        :param sample1_events: Count of sample 1 events.
        :param sample1_days: Count of sample 1 days.
        :param sample2_events: Count of sample 2 events.
        :param sample2_days: Count of sample 2 days.
        :return: P-value for a Poisson statistical test.
        """

        p = binom_test(np.array([float(sample1_events) / float(sample1_days),
                                 float(sample2_events) / float(sample2_days)]),
                       float(sample1_events) + float(sample1_events))

        return p

    @classmethod
    def pe(cls,
           type,
           original_quantity=False,
           new_quantity=False,
           original_price=False,
           new_price=False,
           pe_prices=False,
           pe_quantities=False):
        """
        Calculates the Price Elasticity of Demand.

        :param type: Classification of whether data is in array/list data format or a scalar format (sample or other).
        :param original_quantity: Starting quantity demanded if data is scalar values.
        :param new_quantity: Ending quantity demanded if data is scalar values.
        :param original_price: Starting price if data is scalar values.
        :param new_price: Ending price if data is scalar values.
        :param pe_prices: Array/list of prices paid for quantities demanded.
        :param pe_quantities: Array/lust of quantities demanded.
        :return: Price elasticity of demand (float).
        """

        if type == "sample":
            p1, p2 = Blob.text_parse(pe_prices, pe_quantities)

            est = sm.OLS(np.log(p2), sm.add_constant(np.log(p1))).fit()
            pe = est.params[1]

        else:

            pe = ((float(new_quantity) - float(original_quantity)) / (float(new_quantity) + float(original_quantity))) / \
                 ((float(new_price) - float(original_price)) / (float(new_price) + float(original_price)))

        return pe

    @classmethod
    def correlation(cls,
                    sample1_dat,
                    sample2_dat):
        """
        Performs a Pearson test of correlation between two data samples.

        :param sample1_dat: Sample 1 data array/list.
        :param sample2_dat: Sample 2 data array/list.
        :return: Pearson correlation result.
        """
        p1, p2 = Blob.text_parse(sample1_dat, sample2_dat)

        pd.DataFrame()
        df = pd.DataFrame(
            {'sample1': p2,
             'sample2': p1})

        corr = df.corr(method="pearson").iloc[0, 1]

        return corr


class Viz:
    """
    Functions for visualizing distributions.

    """

    @classmethod
    def make_plot_tbl(cls, width, height, plot_size,
                      tbl_size, df, col, tgt_col,
                      title, xlabel, ylabel, agg_df, plot_type, yaxis_fmt, xaxis_fmt, stat, font_size):
        """
        Creates a visualization of a data table next to a plot of the data. Intended for use in Jupyter Notebook.

        :param width: Width of plot.
        :param height: Height of plot.
        :param plot_size: Size of overall plot.
        :param tbl_size: Size of data table.
        :param df: Pandas DataFrame of Data to plot.
        :param col: Dimension column for plot.
        :param tgt_col: KPI column for plot.
        :param title: Title for plot.
        :param xlabel: Xlabel for plot.
        :param ylabel: YLabel for plot.
        :param agg_df: Pandas DataFrame for data table.
        :param plot_type: Type of visualization to plot (box, box_h, scatter, dist, agg_dist)
        :param yaxis_fmt: Format for yaxis.
        :param xaxis_fmt: Format for xaxis.
        :param stat: Type of statistic to add to the plot if box plot (currently only T-Test P-value supported).
        :param font_size: Size of font for table.
        :return: Visualization of a data table next to a plot of the data.
        """

        fmt = '${x:,.0f}'
        fig = plt.figure(figsize=(width, height))
        gs = gridspec.GridSpec(1, 2, width_ratios=[plot_size, tbl_size])
        ax1 = plt.subplot(gs[0])
        current_palette_7 = sns.color_palette("Set1", 2)
        sns.set_palette(current_palette_7)
        sns.set(style="ticks")

        if plot_type == "box":

            sns.swarmplot(x=df[col], y=df[tgt_col], data=df,
                          size=2, color="blue", linewidth=0, alpha=0.7)

            with sns.diverging_palette(10, 220, sep=80, n=7):

                sns.boxplot(x=df[col], y=df[tgt_col], data=df, showmeans=True)

            for patch in ax1.artists:
                r, g, b, a = patch.get_facecolor()
                patch.set_facecolor((r, g, b, .3))

            if stat == "t":
                p_val = ttest_ind(df, col)
                p_val = p_val[0]
                ax1.text(.0, .99, 'T-Test p-value=%s' % p_val, ha='center', va='center',
                         transform=ax1.transAxes, color="red", size=18)

        elif plot_type == "box_h":

            sns.swarmplot(order=agg_df.index, x=tgt_col, y=col, data=df,
                          size=2, color="blue", linewidth=0)

            with sns.diverging_palette(10, 220, sep=80, n=7):
                # find colors for boxes

                sns.boxplot(order=agg_df.index, x=tgt_col, y=col,
                            data=df, orient="h", showmeans=True)

            for patch in ax1.artists:
                r, g, b, a = patch.get_facecolor()
                patch.set_facecolor((r, g, b, .3))

        elif plot_type == "scatter":
            df_sub = df[pd.notnull(df[col])]
            from scipy import stats
            def r2(x, y):
                return stats.pearsonr(x, y)[0] ** 2

            rsq = r2(df_sub[col], df_sub[tgt_col])
            sns.regplot(x=col, y=tgt_col, marker="+", data=df_sub, scatter_kws={"alpha": 0.7})
            ax1.text(.1, .1, 'R^2=%s' % round(rsq, 5), ha='center',
                     va='center', transform=ax1.transAxes, color="red", size=18)

        elif plot_type == "dist":
            sns.distplot(df[col].dropna(), bins=50, color='#40466e', kde=False, hist_kws={"alpha": 0.7})

            median1 = np.nanmedian(df[col])
            median1 = round(median1)

            ax1.text(.9, .9, 'Median=%s' % median1, ha='center', va='center',
                     transform=ax1.transAxes, color="b", size=18)
            plt.axvline(median1, color='b', linestyle='dashed', linewidth=3)
            ax1.grid(linestyle='--', linewidth=1, axis="y")

        elif plot_type == "agg_dist":
            sns.barplot(x=df.index, y='count', data=df, ax=ax1, linewidth=2.5, color='#40466e');
            ax1.grid(linestyle='--', linewidth=1, axis="y")

        # set formatting aesthetics
        sns.despine(left=True, bottom=True)
        ax1.set_title(title, fontsize=22)
        ax1.set_xlabel(xlabel, fontsize=18)
        ax1.set_ylabel(ylabel, fontsize=18)
        ax1.xaxis.set_tick_params(labelsize=14)
        ax1.yaxis.set_tick_params(labelsize=14)
        tick = mtick.StrMethodFormatter(fmt)
        if yaxis_fmt:
            ax1.yaxis.set_major_formatter(tick)
        if xaxis_fmt:
            ax1.xaxis.set_major_formatter(tick)

        ax2 = plt.subplot(gs[1])
        ax2.axis('off')
        font_size = font_size
        header_color = '#40466e'
        row_colors = ['#f1f1f2', 'w']
        edge_color = 'w'
        bbox = [0, 0, 1, 1]
        header_columns = 0

        agg_df = agg_df.copy()

        if yaxis_fmt == "$" or xaxis_fmt == "$":
            agg_df['median'] = agg_df['median'].map('${:,.2f}'.format)
        else:
            agg_df['median'] = agg_df['median'].map('{:,.2f}'.format)

        agg_df['pct'] = agg_df['pct'].map('{:,.1f}%'.format)

        if plot_type == "dist" or plot_type == "agg_dist":
            del agg_df['median']
            mpl_table = ax2.table(cellText=agg_df.values, rowLabels=agg_df.index,
                                  bbox=bbox, colLabels=["N", "%"])

        else:
            mpl_table = ax2.table(cellText=agg_df.values, rowLabels=agg_df.index,
                                  bbox=bbox, colLabels=["N", "%", "Median $"])

        mpl_table.auto_set_font_size(False)
        mpl_table.set_fontsize(font_size)
        for k, cell in six.iteritems(mpl_table._cells):
            cell.set_edgecolor(edge_color)
            if k[0] == 0 or k[1] < header_columns:
                cell.set_text_props(weight='bold', color='w', wrap=True)
                cell.set_facecolor(header_color)
            else:
                cell.set_facecolor(row_colors[k[0] % len(row_colors)])
        return gs;

    @classmethod
    def make_plotting_tbl(cls, width, height, plot_size, tbl_size, df_plot, plot_col_x, plot_col_y, plot_col_hue,
                          plot_title, df_tbl, font_size):
        """
        Create a visualization of a data table + a bar graph.

        :param width: Width of plot.
        :param height: Height of plot.
        :param plot_size: Size of overall plot.
        :param tbl_size: Size of data table.
        :param df_plot: Pandas DataFrame of data to plot.
        :param plot_col_x: Column name to plot on X axis.
        :param plot_col_y: Column name to plot on Y axis (bars).
        :param plot_col_hue: Color for column on Y axis (bars).
        :param plot_title: Title for plot.
        :param df_tbl:  Pandas DataFrame of data to show in datatable.
        :param font_size: Font size for data table.
        :return: Visualization of a data table next to a bar plot of the data.
        """
        fig = plt.figure(figsize=(width, height))
        gs = gridspec.GridSpec(1, 2, width_ratios=[plot_size, tbl_size])
        ax1 = plt.subplot(gs[0])
        current_palette_7 = sns.color_palette("Set1", 2)
        sns.set_palette(current_palette_7)
        sns.barplot(x=plot_col_x, y=plot_col_y, hue=plot_col_hue,
                    data=df_plot, ax=ax1,
                    linewidth=2.5);

        sns.despine(left=True, bottom=True)
        ax1.set_title(plot_title)
        ax1.set_xlabel("")
        ax1.set_ylabel("")
        ax2 = plt.subplot(gs[1])
        ax2.axis('off')
        font_size = font_size
        header_color = '#40466e'
        row_colors = ['#f1f1f2', 'w']
        edge_color = 'w'
        bbox = [0, 0, 1, 1]
        header_columns = 0

        mpl_table = ax2.table(cellText=df_tbl.values, rowLabels=df_tbl.index,
                              bbox=bbox, colLabels=df_tbl.columns)

        mpl_table.auto_set_font_size(False)
        mpl_table.set_fontsize(font_size)
        for k, cell in six.iteritems(mpl_table._cells):
            cell.set_edgecolor(edge_color)
            if k[0] == 0 or k[1] < header_columns:
                cell.set_text_props(weight='bold', color='w', wrap=True)
                cell.set_facecolor(header_color)
            else:
                cell.set_facecolor(row_colors[k[0] % len(row_colors)])
        return gs;

    @classmethod
    def dist_plot(cls, df, col, sav_dir=""):
        """
        Creates a histogram of data.

        :param df: Pandas DataFrame of data to plot.
        :param col: Column to plot on y-axis (bars).
        :param sav_dir: Directory to save plot in.
        :return: Saved plot.
        """
        kwargs = dict(hist_kws={'alpha': .6}, kde_kws={'linewidth': 0})
        plt.figure(figsize=(10, 7), dpi=80)

        sns.distplot(df[col], color="orange", label="All", **kwargs)
        # sns.distplot(X_train[X_train.index.isin(y_train[y_train==1].index)][col],
        #             color="dodgerblue", label="Responder", **kwargs)

        plt.legend()
        plt.title(col)
        plt.savefig(sav_dir + col + ".png")
        plt.close()
