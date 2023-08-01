################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

"""An interface module from improvwf to sklearn models and pipelines"""
import pandas as pd
from sklearn.pipeline import Pipeline

from improvwf.sklearn_transforms import get_transformer_or_estimator


def sklearn_model_from_spec(model_spec):
    """Create an sklearn model from a user's specification"""

    steps = []
    # Follow the user's spec, one step at a time
    for step_i in model_spec:
        # Get the step name:
        if "name" in step_i:
            name_i = step_i["name"]
        else:
            name_i = str(step_i["type"]) + "_" + str(len(steps))

        # Safely retrieve the estimator or transformer
        transformer_or_estimator = get_transformer_or_estimator(
            **step_i)

        # Stack it onto steps
        steps.append((name_i, transformer_or_estimator))

    # Construct the final Pipeline
    return Pipeline(steps=steps, memory=None)


def sklearn_model_from_dataframe(model_spec, history_dataframe_features,
                                 history_dataframe_results, imputation=None,
                                 cross_validation=None):
    """
    Fit a scikit-learn model from data in the history

    :argument model_spec: A list containing dictionaries, themselves
    consisting of two string name-value pairs (name: <human_readable_name>,
    type: <sklearn transform or estimator, within the sklearn base namespace>,
    and another dictionary, parameters: {name-value pairs of parameters of
    the estimator or transform, where value is a string in each case}.  These
    will be converted to an sklearn Pipeline.
    :argument history_dataframe_features: pandas dataframe, containing the
        experimental run_parameters and study_types (the features)
    :argument history_dataframe_result: pandas dataframe, indexed the same as
        history_dataframe_features, and containing the corresponding results.
    :argument: imputation: string specifying imputation scheme
    :argument: cross_validation: specification of cross-validation parameters.
    :return: a fitted sklearn model, including a predict function
    """
    # ### Input handling ###

    # ### Impute missing data ###
    if imputation is not None:
        raise("Imputation not implemented!")

    # ### Construct the Pipeline ###
    # print("Constructing the pipeline...")
    pl = sklearn_model_from_spec(model_spec)
    # print("Done.")
    # For the love of sanity:
    # print(history_dataframe_results)
    # print(history_dataframe_features)

    # Fit the Pipeline
    # print("Fitting the pipeline...")
    pl.fit(history_dataframe_features, history_dataframe_results)
    # print("Done.")
    return pl


def dataframe_from_history(history, feature_names=None, study_types=None,
                           result_types=None):
    """
    Return the  features associated with the inputs and
    observations
    :param history: improv's standard history representation.
    :param feature_names: list of strings specifying which features and in
        what order should be pulled out and returned.
    :argument study_types: A list of strings containing the study types
    :argument result_types: either None or a list of strings specifying the
        sub-information within results which will contribute to the (multi-
        variate) observations.
    :return: pandas DataFrame of feature values and observation values
    """
    if feature_names is None:
        raise(NotImplementedError, "Autodetection of features is not yet "
                                   "implemented!")
    if study_types is None:
        raise(NotImplementedError, "Auto-detection of study types is not yet "
                                   "implemented!")
    if result_types is None:
        raise(NotImplementedError, "Autodetection of result types is not yet "
                                   "implemented!")

    # Get the request IDs
    data = {"request_id": [req_id for req_id in history["history"]]}

    # Use the request ID values to fill in the study type in the same order
    data["study_type"] = [history["history"][req_id]["study_type"] for req_id
                          in data["request_id"]]

    # Extract the feature values
    for fni in feature_names:
        data[fni] = []
        for req_id in data["request_id"]:
            if "study_parameters" in history["history"][req_id] \
                    and fni in history["history"][req_id]["study_parameters"]:
                data[fni].append(
                    history["history"][req_id]["study_parameters"][fni]
                    ["values"][0]
                )

            elif fni in history["history"][req_id]:
                data[fni].append(history["history"][req_id][fni])
            else:
                data[fni].append(None)

    # Extract the result values
    for rti in result_types:
        data[rti] = []
        for req_id in data["request_id"]:
            if "result" in history["history"][req_id] and \
                    rti in history["history"][req_id]["result"]:
                data[rti].append(history["history"][req_id]["result"][rti])
            else:
                data[rti].append(None)

    # Filter on allowed study types:
    allowed_study_type = [st_i in study_types for st_i in data["study_type"]]
    for ki, vi in data.items():
        if ki is "study_type":
            data[ki] = [study_types.index(st_i) for st_i, ast_i in
                        zip(vi, allowed_study_type) if ast_i]
        else:
            data[ki] = [vi_i for vi_i, ast_i in zip(vi, allowed_study_type)
                        if ast_i]

    # Construct the DataFrame
    df_out = pd.DataFrame(data)

    try:
        df_out = df_out.set_index("request_id")
    except KeyError:
        return df_out
    return df_out


def dataframe_from_menu(menu, feature_names=None, study_types=None):
    """
    Return the values of the features associated with the inputs
    :param menu: improv's standard menu representation.
    :param feature_names: list of strings specifying which features and in
    what order should be pulled out and returned.
    :argument study_types: A list of strings containing the study types
    :return: pandas DataFrame of feature values
    """
    if feature_names is None:
        raise(NotImplementedError, "Autodetection of features is not yet "
                                   "implemented!")
    if study_types is None:
        raise(NotImplementedError, "Auto-detection of study types is not yet "
                                   "implemented!")

    # Pull the feature value data out of the menu
    data = {}
    for fni in feature_names:
        data[fni] = [si["study_parameters"][fni]["values"][0]  # param value
                     if fni in si["study_parameters"]  # If this param active
                     else None   # Else fill with None
                     for si in menu["studies"]
                     ]
    data["study_type"] = [study_types.index(mi["study_type"])
                          for mi in menu["studies"]]

    # Above: Makes study_type an integer-encoded categorical variable
    # TODO: Detect which data types are categoricals, etc?

    frame_out = pd.DataFrame(data)  # Creates a DataFrame with integer
        # index, where columns are named by the keys in data
    return frame_out


def transform_dataframe(dataframe_in, transformations):
    """
    Transform a dataframe in a series of steps
    :param dataframe_in: pandas DataFrame
    :param transformations: list of transformations to apply to the data.
    :return: pandas DataFrame containing the altered data
    """

    dataframe_out = dataframe_in.copy()
    for ti in transformations:
        # Apply the corresponding transformation
        print("Transformations not yet implemented!")
    return dataframe_out


if __name__ == "__main__":
    spec = [{"type": "sklearn.ensemble.RandomForestRegressor",
                       "parameters":
                           {"n_estimators": 5, "max_depth": 2, "verbose": 1}
                       }]
    pl = sklearn_model_from_spec(spec)

    import yaml
    with open("/Users/desautels2/GitRepositories/improvwf/samples"
              "/nontrivial_decision_makers/global_history.yaml", 'r') as f:
        h = yaml.safe_load(f)

    feature_names = ["ITERATIONS", "SIZE"]
    study_types = ["lulesh_sample_energy"]
    result_types = ["energy"]

    df_hist = dataframe_from_history(h, feature_names=feature_names, study_types=study_types,
            result_types=result_types)


    # I'm not sure that directly feeding in the pandas dataframes works
    # here... the sklearn internal .fit is complaining about the
    # dimensionality of these guys
    pl = sklearn_model_from_dataframe(spec, df_hist[feature_names], df_hist[result_types])
