################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

"""A module including sklearn-compliant transformers"""
from sklearn.base import BaseEstimator, TransformerMixin


class DataFrameSelector(BaseEstimator, TransformerMixin):
    """
    A class for intake and sub-selection of Pandas dataframes

    From Hands-On Machine Learning with Scikit-Learn & Tensorflow,
    Aurelien Geron, 2017. Used with permission.
    """
    def __init__(self, attribute_names):
        self.attribute_names = attribute_names

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X[self.attribute_names].values
