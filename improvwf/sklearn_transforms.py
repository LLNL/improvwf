################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

"""An explicit enumeration of allowed transformers and estimators"""
# All imports below work with 0.19.2 and 0.20.0
from sklearn.decomposition import DictionaryLearning, FastICA, \
    IncrementalPCA, KernelPCA, NMF, PCA, SparseCoder, \
    SparsePCA, FactorAnalysis, TruncatedSVD, LatentDirichletAllocation  #, RandomizedPCA
from sklearn.gaussian_process import GaussianProcessRegressor, \
    GaussianProcessClassifier  #, GaussianProcess
from sklearn.gaussian_process.kernels import RBF, Matern
from sklearn.semi_supervised import LabelPropagation, LabelSpreading
from sklearn.cross_decomposition import CCA
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis, \
    QuadraticDiscriminantAnalysis
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, \
    RandomTreesEmbedding, ExtraTreesClassifier, ExtraTreesRegressor, \
    BaggingClassifier, BaggingRegressor, IsolationForest, \
    GradientBoostingClassifier, GradientBoostingRegressor, \
    AdaBoostClassifier, AdaBoostRegressor, VotingClassifier
from sklearn.feature_selection import RFE, RFECV, SelectFdr, SelectFpr, \
    SelectFwe, SelectKBest, SelectFromModel, SelectPercentile, \
    VarianceThreshold
from sklearn.manifold import LocallyLinearEmbedding, Isomap, MDS, \
    SpectralEmbedding, TSNE
# import sklearn.multioutput  # TODO: Investigate this
from sklearn.naive_bayes import BernoulliNB, GaussianNB, MultinomialNB
from sklearn.linear_model import ARDRegression, BayesianRidge, ElasticNet, \
    ElasticNetCV, Hinge, Huber, HuberRegressor, Lars, LarsCV, Lasso, LassoCV, \
    LassoLars, LassoLarsCV, LassoLarsIC, LinearRegression, Log, \
    LogisticRegression, LogisticRegressionCV, ModifiedHuber, \
    MultiTaskElasticNet, MultiTaskElasticNetCV, MultiTaskLasso, \
    MultiTaskLassoCV, OrthogonalMatchingPursuit, OrthogonalMatchingPursuitCV, \
    PassiveAggressiveClassifier, PassiveAggressiveRegressor, Perceptron, \
    RandomizedLasso, RandomizedLogisticRegression, Ridge, RidgeCV, \
    RidgeClassifier, RidgeClassifierCV, SGDClassifier, SGDRegressor, \
    SquaredLoss, TheilSenRegressor, RANSACRegressor
from sklearn.mixture import GaussianMixture, BayesianGaussianMixture
# from sklearn.mixture import DPGMM, GMM, VBGMM
from sklearn.pipeline import FeatureUnion
from sklearn.preprocessing import Binarizer, FunctionTransformer, Imputer, \
    KernelCenterer, LabelBinarizer, LabelEncoder, MultiLabelBinarizer, \
    MinMaxScaler, MaxAbsScaler, QuantileTransformer, Normalizer,  \
    OneHotEncoder, RobustScaler, StandardScaler, PolynomialFeatures
from sklearn.random_projection import SparseRandomProjection, \
    GaussianRandomProjection
from sklearn.svm import LinearSVC, LinearSVR, NuSVC, NuSVR, OneClassSVM, SVC,\
    SVR
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, \
    ExtraTreeClassifier, ExtraTreeRegressor

from improvwf.transformers import DataFrameSelector


def get_transformer_or_estimator(type=None, parameters=None, name=None):
    """
    Retrieve an sklearn-internal transform or estimator from a given key
    :param type: string specifying a path into the sklearn package
    :param parameters: the dictionary of parameters to be passed to the
    function.
    :param name: not used.
    :return: the specified transformer or estimator, filled in with the
        chosen parameters.
    """
    if parameters is None:
        parameters = {}

    if not type.startswith("improvwf.") and not type.startswith("sklearn."):
        type = "sklearn." + type

    improvwf_dict = {"improvwf.transformers.DataFrameSelector":
                         DataFrameSelector
    }

    sklearn_dict = {
            "sklearn.decomposition.DictionaryLearning":  DictionaryLearning,
            "sklearn.decomposition.FastICA": FastICA,
            "sklearn.decomposition.IncrementalPCA": IncrementalPCA,
            "sklearn.decomposition.KernelPCA": KernelPCA,
            "sklearn.decomposition.NMF": NMF,
            "sklearn.decomposition.PCA": PCA,
            # "sklearn.decomposition.RandomizedPCA": RandomizedPCA,
            "sklearn.decomposition.SparseCoder": SparseCoder,
            "sklearn.decomposition.SparsePCA": SparsePCA,
            "sklearn.decomposition.FactorAnalysis": FactorAnalysis,
            "sklearn.decomposition.TruncatedSVD": TruncatedSVD,
            "sklearn.decomposition.LatentDirichletAllocation":
             LatentDirichletAllocation,
            #"sklearn.gaussian_process.GaussianProcess": GaussianProcess,
            "sklearn.gaussian_process.GaussianProcessRegressor":
             GaussianProcessRegressor,
            "sklearn.gaussian_process.GaussianProcessClassifier":
             GaussianProcessClassifier,
            "sklearn.gaussian_process.kernels.RBF": RBF,
            "sklearn.gaussian_process.kernels.Matern": Matern,
            "sklearn.semi_supervised.LabelPropagation": LabelPropagation,
            "sklearn.semi_supervised.LabelSpreading": LabelSpreading,
            "sklearn.cross_decomposition.CCA": CCA,
            "sklearn.discriminant_analysis.LinearDiscriminantAnalysis":
             LinearDiscriminantAnalysis,
            "sklearn.discriminant_analysis.QuadraticDiscriminantAnalysis":
             QuadraticDiscriminantAnalysis,
            "sklearn.ensemble.RandomForestClassifier": RandomForestClassifier,
            "sklearn.ensemble.RandomForestRegressor": RandomForestRegressor,
            "sklearn.ensemble.RandomTreesEmbedding": RandomTreesEmbedding,
            "sklearn.ensemble.ExtraTreesClassifier": ExtraTreesClassifier,
            "sklearn.ensemble.ExtraTreesRegressor": ExtraTreesRegressor,
            "sklearn.ensemble.BaggingClassifier": BaggingClassifier,
            "sklearn.ensemble.BaggingRegressor": BaggingRegressor,
            "sklearn.ensemble.IsolationForest": IsolationForest,
            "sklearn.ensemble.GradientBoostingClassifier": GradientBoostingClassifier,
            "sklearn.ensemble.GradientBoostingRegressor": GradientBoostingRegressor,
            "sklearn.ensemble.AdaBoostClassifier": AdaBoostClassifier,
            "sklearn.ensemble.AdaBoostRegressor": AdaBoostRegressor,
            "sklearn.ensemble.VotingClassifier": VotingClassifier,
            "sklearn.feature_selection.RFE": RFE,
            "sklearn.feature_selection.RFECV": RFECV,
            "sklearn.feature_selection.SelectFdr": SelectFdr,
            "sklearn.feature_selection.SelectFpr": SelectFpr,
            "sklearn.feature_selection.SelectFwe": SelectFwe,
            "sklearn.feature_selection.SelectKBest": SelectKBest,
            "sklearn.feature_selection.SelectFromModel": SelectFromModel,
            "sklearn.feature_selection.SelectPercentile": SelectPercentile,
            "sklearn.feature_selection.VarianceThreshold": VarianceThreshold,
            "sklearn.manifold.LocallyLinearEmbedding": LocallyLinearEmbedding,
            "sklearn.manifold.Isomap": Isomap,
            "sklearn.manifold.MDS": MDS,
            "sklearn.manifold.SpectralEmbedding": SpectralEmbedding,
            "sklearn.manifold.TSNE": TSNE,
            "sklearn.naive_bayes.BernoulliNB": BernoulliNB,
            "sklearn.naive_bayes.GaussianNB": GaussianNB,
            "sklearn.naive_bayes.MultinomialNB": MultinomialNB,
            "sklearn.linear_model.ARDRegression": ARDRegression,
            "sklearn.linear_model.BayesianRidge": BayesianRidge,
            "sklearn.linear_model.ElasticNet": ElasticNet,
            "sklearn.linear_model.ElasticNetCV": ElasticNetCV,
            "sklearn.linear_model.Hinge": Hinge,
            "sklearn.linear_model.Huber": Huber,
            "sklearn.linear_model.HuberRegressor": HuberRegressor,
            "sklearn.linear_model.Lars": Lars,
            "sklearn.linear_model.LarsCV": LarsCV,
            "sklearn.linear_model.Lasso": Lasso,
            "sklearn.linear_model.LassoCV": LassoCV,
            "sklearn.linear_model.LassoLars": LassoLars,
            "sklearn.linear_model.LassoLarsCV": LassoLarsCV,
            "sklearn.linear_model.LassoLarsIC": LassoLarsIC,
            "sklearn.linear_model.LinearRegression": LinearRegression,
            "sklearn.linear_model.Log": Log,
            "sklearn.linear_model.LogisticRegression": LogisticRegression,
            "sklearn.linear_model.LogisticRegressionCV": LogisticRegressionCV,
            "sklearn.linear_model.ModifiedHuber": ModifiedHuber,
            "sklearn.linear_model.MultiTaskElasticNet": MultiTaskElasticNet,
            "sklearn.linear_model.MultiTaskElasticNetCV": MultiTaskElasticNetCV,
            "sklearn.linear_model.MultiTaskLasso": MultiTaskLasso,
            "sklearn.linear_model.MultiTaskLassoCV": MultiTaskLassoCV,
            "sklearn.linear_model.OrthogonalMatchingPursuit":
             OrthogonalMatchingPursuit,
            "sklearn.linear_model.OrthogonalMatchingPursuitCV":
             OrthogonalMatchingPursuitCV,
            "sklearn.linear_model.PassiveAggressiveClassifier":
             PassiveAggressiveClassifier,
            "sklearn.linear_model.PassiveAggressiveRegressor":
             PassiveAggressiveRegressor,
            "sklearn.linear_model.Perceptron": Perceptron,
            "sklearn.linear_model.RandomizedLasso": RandomizedLasso,
            "sklearn.linear_model.RandomizedLogisticRegression":
             RandomizedLogisticRegression,
            "sklearn.linear_model.Ridge": Ridge,
            "sklearn.linear_model.RidgeCV": RidgeCV,
            "sklearn.linear_model.RidgeClassifier": RidgeClassifier,
            "sklearn.linear_model.RidgeClassifierCV": RidgeClassifierCV,
            "sklearn.linear_model.SGDClassifier": SGDClassifier,
            "sklearn.linear_model.SGDRegressor": SGDRegressor,
            "sklearn.linear_model.SquaredLoss": SquaredLoss,
            "sklearn.linear_model.TheilSenRegressor": TheilSenRegressor,
            "sklearn.linear_model.RANSACRegressor": RANSACRegressor,
            # "sklearn.mixture.DPGMM": DPGMM,
            # "sklearn.mixture.GMM": GMM,
            # "sklearn.mixture.VBGMM": VBGMM,
            "sklearn.mixture.GaussianMixture": GaussianMixture,
            "sklearn.mixture.BayesianGaussianMixture": BayesianGaussianMixture,
            "sklearn.pipeline.FeatureUnion": FeatureUnion,
            "sklearn.preprocessing.Binarizer": Binarizer,
            "sklearn.preprocessing.FunctionTransformer": FunctionTransformer,
            "sklearn.preprocessing.Imputer": Imputer,
            "sklearn.preprocessing.KernelCenterer": KernelCenterer,
            "sklearn.preprocessing.LabelBinarizer": LabelBinarizer,
            "sklearn.preprocessing.LabelEncoder": LabelEncoder,
            "sklearn.preprocessing.MultiLabelBinarizer": MultiLabelBinarizer,
            "sklearn.preprocessing.MinMaxScaler": MinMaxScaler,
            "sklearn.preprocessing.MaxAbsScaler": MaxAbsScaler,
            "sklearn.preprocessing.QuantileTransformer": QuantileTransformer,
            "sklearn.preprocessing.Normalizer": Normalizer,
            "sklearn.preprocessing.OneHotEncoder": OneHotEncoder,
            "sklearn.preprocessing.RobustScaler": RobustScaler,
            "sklearn.preprocessing.StandardScaler": StandardScaler,
            "sklearn.preprocessing.PolynomialFeatures": PolynomialFeatures,
            "sklearn.random_projection.SparseRandomProjection": SparseRandomProjection,
            "sklearn.random_projection.GaussianRandomProjection":
             GaussianRandomProjection,
            "sklearn.svm.LinearSVC": LinearSVC,
            "sklearn.svm.LinearSVR": LinearSVR,
            "sklearn.svm.NuSVC": NuSVC,
            "sklearn.svm.NuSVR": NuSVR,
            "sklearn.svm.OneClassSVM": OneClassSVM,
            "sklearn.svm.SVC": SVC,
            "sklearn.svm.SVR": SVR,
            "sklearn.tree.DecisionTreeClassifier": DecisionTreeClassifier,
            "sklearn.tree.DecisionTreeRegressor": DecisionTreeRegressor,
            "sklearn.tree.ExtraTreeClassifier": ExtraTreeClassifier,
            "sklearn.tree.ExtraTreeRegressor": ExtraTreeRegressor
         }

    if type in improvwf_dict:
        transformer_or_estimator = improvwf_dict[type]
    elif type in sklearn_dict:
        transformer_or_estimator = sklearn_dict[type]
    else:
        raise(
            KeyError,
            "type {} not present in improvwf_dict (improvwf-defined "
            "transformers and estimators) or sklearn_dict (imported sklearn "
            "transformers and estimators); consider adding it to the dictionary"
            " and imports in improvwf/sklearn_transforms.py.".format(type)
        )

    return transformer_or_estimator(**parameters)



if __name__ is "__main__":
    a = get_transformer_or_estimator("linear_model.LassoCV")

    b = get_transformer_or_estimator("linear_model.LassoCV",
                                 {"eps": 0.00001, "alphas":
                                     [0.1, 0.5, 1.0], "fit_intercept": False})
    c = get_transformer_or_estimator("improvwf.transformers.DataFrameSelector")
