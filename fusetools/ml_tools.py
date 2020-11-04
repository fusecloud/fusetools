"""
Functions for interacting with Machine Learning Tools.

|pic1|
    .. |pic1| image:: ../images_source/ml_tools/scikit1.png
        :width: 50%

"""

import pandas as pd
from sklearn import metrics
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import SGDClassifier, LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
from sklearn.svm import SVC
import numpy as np
from fusetools.analytics_tools import Pandas
import seaborn as sns


class Prep:
    """
    Functions for preparing data for machine learning tasks.

    """

    @classmethod
    def make_model_feature_df(cls, df, cat, model_list):
        """
        Creates Pandas DataFrame with cumulatively trained combined feature names, coefficients, absolute coefficients in order of trained together.

        :param df: Pandas DataFrame of a fitted SckitLearn estimator's features, coefficients absolute coefficients.
        :param cat: Name of ScikitLearn estimator or self defines estimator type.
        :param model_list: List of ScikitLearn estimators.
        :return: Pandas DataFrame with cumulatively trained combined feature names, coefficients, absolute coefficients and performance placeholders in order of trained together.
        """
        for idx, m in enumerate(model_list):

            df = df.copy()

            feature_list = []
            df['feature_list'] = ""
            df['feature_count'] = ""
            df["auc"] = ""
            df["acc"] = ""

            for idxx, row in df.iterrows():
                feature_list.append(row['feature'])
                df.at[idxx, 'feature_list'] = feature_list.copy()
                df.at[idxx, 'feature_count'] = len(feature_list)

            df['model'] = m
            df["cat"] = cat

            if idx == 0:
                df_all = df.copy()
            else:
                df_all = pd.concat([df_all, df])

            return df_all

    @classmethod
    def label_encode_df(cls, df, col, col_new):
        """
        Assigns an incremental number for each string value and then converts the new number to a float so we can add the missing value (NaNs) back in for later imputation.

        :param df: Pandas DataFrame with atleast one feature to encode.
        :param col: Name of column to encode.
        :param col_new: Name of new, encoded column.
        :return: Pandas DataFrame with new, encoded column.
        """

        lb = LabelEncoder()
        df[col_new] = lb.fit_transform(df[col].fillna("NaN")).astype(
            float)  # apply label encoder (creates incremental number for each string)
        df[col_new] = np.where(df[col_new] == Pandas.find_na_holder(col, col_new)[1], np.nan,
                               df[col_new])  # add missing values back
        df.drop(col, axis=1, inplace=True)  # drop the old column

        return df


class Viz:
    """
    Functions for visualizing results from machine learning tasks.

    """

    @classmethod
    def show_model_results(cls, estimator, y_test, y_pred, y_score):
        """
        Prints the Accuracy, AUC and Metrics Classification report for an estimator.

        :param estimator: A trained estimator.
        :param y_test: Output labels for the test dataset.
        :param y_pred: Output predictions labels for the test dataset.
        :param y_score: Probabilities of certainty for output prediction labels.
        :return: Accuracy, AUC and Metrics Classification report for an estimator.
        """

        y_wrong = y_test[y_test != y_pred]
        print("Model Performance Metrics:")
        print(".................................................................")
        print('Test Accuracy: %.3f' % accuracy_score(y_true=y_test, y_pred=y_pred))
        print('Test AUC: %.3f' % roc_auc_score(y_true=y_test, y_score=y_score))

        # store model accuracy
        clf_acc = accuracy_score(y_true=y_test.values, y_pred=y_pred)
        print("")
        print("")
        print("Model Diagnostics:")
        print(".................................................................")
        print(metrics.classification_report(y_test, y_pred))
        Viz.show_clf_perf(14, 5, estimator, y_test, y_pred, y_score);

    @classmethod
    def show_clf_perf(cls, width, height, y_test, y_pred, y_score):
        """
        Prints a confusion matrix of an estimator's binary classification performance.

        :param width: Plot width.
        :param height: Plot height.
        :param y_test: Output labels for the test dataset.
        :param y_pred: Output predictions labels for the test dataset.
        :param y_score: Probabilities of certainty for output prediction labels.
        :return: Confusion matrix of an estimator's binary classification performance.
        """

        import matplotlib.pyplot as plt
        from matplotlib import gridspec
        import seaborn as sns
        fig = plt.figure(figsize=(width, height))
        gs = gridspec.GridSpec(1, 2, width_ratios=[1, 1])
        ax1 = plt.subplot(gs[0])
        from sklearn.metrics import confusion_matrix
        confmat = confusion_matrix(y_true=y_test, y_pred=y_pred)
        ax1.matshow(confmat, cmap=plt.cm.Blues, alpha=0.3)
        for i in range(confmat.shape[0]):
            for j in range(confmat.shape[1]):
                ax1.text(x=j, y=i, s=confmat[i, j],
                         va='center', ha='center', size=20)

        from sklearn.metrics import accuracy_score
        accuracy_score = accuracy_score(y_true=y_test, y_pred=y_pred)

        ax1.text(.9, .1, 'Test Accuracy=%s' % round(accuracy_score, 5),
                 ha='center', size=20, va='center', transform=ax1.transAxes, color="red")

        ax1.set_title("Confusion Matrix")
        ax1.set_xlabel("Predicted Success")
        ax1.set_ylabel("Actual Success")

        sns.despine(left=True, bottom=True)

        ax2 = plt.subplot(gs[1])

        from sklearn.metrics import roc_curve, roc_auc_score

        fpr, tpr, thresholds = roc_curve(y_true=y_test, y_score=y_score, pos_label=1)
        auc = roc_auc_score(y_true=y_test, y_score=y_score)

        ax2.plot(fpr, tpr, linewidth=2, label=None)
        ax2.plot([0, 1], [0, 1], 'k--')
        ax2.text(.9, .1, 'Test AUC=%s' % round(auc, 5), size=20,
                 ha='center', va='center', transform=ax2.transAxes, color="red")

        ax2.set_title("ROC Curve")
        plt.show()

        return gs;

    @classmethod
    def show_clf_perf_features(cls, width, height, df, title, model_list, df_max_stats):
        """
        Creates a plot of given ScikitLearn estimator Accuracies (Y Axis) by number of features in model (X Axis).

        :param width: Plot width.
        :param height: Plot height.
        :param df: Pandas DataFrame with cumulatively trained combined feature names, coefficients, absolute coefficients in order of trained together.
        :param title: Plot title.
        :param model_list: List of ScikitLearn estimators to iterate through and plot cumulative performance for.
        :param df_max_stats: Pandas DataFrame of estimator names, max accuracy achieved and number of features for fitted estimator. Used for plot annotation.
        :return: Plot of given ScikitLearn estimator Accuracies (Y Axis) by number of features in model (X Axis).
        """

        alpha = 0.3
        for idx, m in enumerate(model_list):
            df_sub = df[df['model'] == m]
            max_acc = df_max_stats[df_max_stats['model'] == m]['acc'].values[0]
            max_acc_ind = df_max_stats[df_max_stats['model'] == m]['feature_count'].values[0]

            ax = sns.lineplot(x=df_sub.index, y=df_sub.acc,
                              markers=df_sub.model, dashes=df_sub.model, data=df_sub, alpha=alpha)

            color = ax.get_lines()[-1].get_c()

            ax.text(max_acc_ind, max_acc, str(round(max_acc, 4)), color=color)

            alpha += 0.1

        ax.set_title(title, size=20)
        ax.set_ylabel("Accuracy", size=12)
        ax.set_xlabel("Feature Count", size=12)
        ax.set_ylim(0.60, 0.75)

        ax.legend(title='Classifier Type', labels=model_list)

        fig = plt.gcf()
        fig.set_size_inches(width, height)


class Train:
    """
    Functions for training machine learning models.

    """

    @classmethod
    def train_model(cls, estimator, X_train, X_test, y_train, predict_method):
        """
        Takes a ScikitLearn estimator instance and train and test dataframes
        returns: an estimator as well as the predictions and probabilities for the test set.

        :param estimator: ScikitLearn estimator instance
        :param X_train: Input training dataset
        :param X_test: Input test dataset
        :param y_train: Target training dataset
        :param predict_method: Type of prediction to make.
        :return: Fitted SckitLearn estimator, test predictions and test scores for each prediction.
        """

        estimator.fit(X_train, y_train)
        y_pred = estimator.predict(X_test)

        if predict_method == "prob":
            y_score = estimator.predict_proba(X_test)

        else:
            y_score = estimator.decision_function(X_test)

        return estimator, y_pred, y_score
