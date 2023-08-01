################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

"""A simple decision-maker demonstrating an interface to scikit-learn models."""
import sys
import random
from datetime import datetime

# Import improv components
from improvwf.utils import get_history, get_menu, get_studies, \
    expand_menu_study_params, remove_from_menu, write_selected_study
from improvwf.sklearn_interface import sklearn_model_from_dataframe, \
    sklearn_model_from_spec, dataframe_from_history, dataframe_from_menu, \
    transform_dataframe

EPSILON = 0.05


def select_eps_greedy(f_hat_values, menu, eps_value):
    """
    With probability epsilon, select uniformly at random, else, select greedily
    :param f_hat_values: a list of predicted function values, corresponding
        with the list of studies in menu["studies"]
    :param menu: improv standard menu object (dictionary, with menu[
        "studies"] as a list of candidate studies
    :param eps_value: with probability = eps_value, select uniformly at
        random from the menu; else, select the maximizer of the f_hat_values.
    :return: tuple of si (the selected study) and the function value f_hat.
    """

    if random.uniform(0, 1) < eps_value:
        # print("Selecting at random.")
        si, f_hat = \
            random.choice([(mi, f_hat_i) for mi, f_hat_i in
                           zip(menu["studies"], f_hat_values)])
    else:
        # print("Selecting the maximizer.")
        # Be greedy: select the maximizer of the prediction among the
        # available choices
        decision_rule = list(f_hat_values)
        si = menu["studies"][decision_rule.index(max(decision_rule))]
        f_hat = f_hat_values[decision_rule.index(max(
            decision_rule))]

    return si, f_hat


def main(history_path=None, menu_path=None, studies_path=None,
         improv_inbox=None, to_submit_this_round=1, max_total_queries=100):
    """
    Select a study and parameters.

    Read the history_file, select a study and a set of parameters for
    that study from the menu_file, and write this to the registry_file

    :param history_path: Path to .yaml file containing the history of
    selected study types and parameters.
    :param menu_path: Path to .yaml file containing the requested studies and
    their allowed parameters.
    :param studies_path: Path to the studies directory, containing the
    template studies.
    :param improv_inbox: Path to directory where the requested studies will be
    placed
    :param to_submit_this_round: The integer number of studies to submit in
    this decision cycle [Default: 1]
    """
    submitted_this_round = 0

    studies = {}

    # ### Acquire the history, menu, and studies
    history = get_history(history_path)
    menu = get_menu(menu_path)  # menu and history are both dictionaries
    studies = get_studies(studies_path)

    # For convenience, list the features and study types which we need to model
    feature_names = ["X0", "X1", "Z"]
    # TODO: Fill in feature_names and study_types
    study_types = ["closed_form_function_query"]
    result_types = ["value"]
    # TODO: Implement auto-detection of categorical and numerical features
    # TODO: Implement solution robust to different test/train categories
    categorical_features = ["study_type"]
    num_categories_per_feature = [len(study_types)]
    numerical_features = feature_names

    # ### Use the history to remove elements from the menu
    menu = expand_menu_study_params(menu)
    menu = remove_from_menu(menu, list(history["history"].values()))

    # ### Quit if no studies left to run. ###

    # If we have run everything on the menu, do not submit a study.
    if not menu:
        # print("All studies on the menu have been run; returning.")
        return 0

    # If there remain elements in the menu (and we haven't exceeded our
    # budget), submit element(s).
    while submitted_this_round < to_submit_this_round and len(list(
            history["history"].keys())) < max_total_queries:
        # print("Beginning to select study #{} of {}.".format(
        #     submitted_this_round+1, to_submit_this_round))

        # pop_in_random_order does this:
        # si = random.choice(menu["studies"])

        # Instead, we can use a model here.
        # # Prepare the data:
        history_as_df = dataframe_from_history(
            history, feature_names=feature_names, study_types=study_types,
            result_types=result_types)

        # # Model the data, assuming all of the studies have the same features
        # model_spec = [{"type": "sklearn.preprocessing.LabelEncoder"},
        #               {"type":
        #                             "sklearn.ensemble.RandomForestRegressor",
        #                "parameters":
        #                    {"n_estimators": 100, "max_depth": 5, "verbose": 1}
        #                }]

        # Train an sklearn model from the univariate scalar data in history
        try:
            # print("Attempting to train an sklearn model.")
            # print("The history is: {}".format(history_as_df))
            # print("Of which, we are using the subset:")
            # print("{} as X and {} as "
            #       "y".format(history_as_df[feature_names + ["study_type"]],
            #                  history_as_df[result_types[0]]
            #                  )
            #       )

            # model = sklearn_model_from_dataframe(
            #     model_spec, history_as_df[feature_names + ["study_type"]],
            #     history_as_df[result_types[0]])
            spec_numerical = [{"type":
                                   "improvwf.transformers.DataFrameSelector",
                               "parameters":
                                   {"attribute_names": numerical_features}
                               },
                              {"type": "sklearn.preprocessing.StandardScaler"}
                              ]
            spec_categorical = [{"type":
                                     "improvwf.transformers.DataFrameSelector",
                                 "parameters":
                                     {"attribute_names": categorical_features}
                                 },
                                {"type": "sklearn.preprocessing.OneHotEncoder",
                                 "parameters": {
                                     "n_values": num_categories_per_feature,
                                     "handle_unknown": "ignore",
                                     "sparse": False
                                 }
                                 }
                                ]

            pipeline_numerical = sklearn_model_from_spec(spec_numerical)
            pipeline_categorical = sklearn_model_from_spec(spec_categorical)
            merge_spec = [{"type": "sklearn.pipeline.FeatureUnion",
                          "parameters": {
                              "transformer_list": [
                                  ("pipeline_numerical", pipeline_numerical),
                                  ("pipeline_categorical", pipeline_categorical)
                              ]}},
                          {"type":"sklearn.ensemble.RandomForestRegressor",
                           "parameters":
                               {"n_estimators": 20, "max_depth": 3,
                                "verbose": 1}
                           }
                          ]

            # print("Submodels created; Train merged model")
            merged_pipeline = sklearn_model_from_dataframe(
                merge_spec, history_as_df[feature_names + ["study_type"]],
                history_as_df[result_types[0]]
            )

            # Make predictions using that model for the expanded menu
            # print("Model trained successfully; now making predictions on the "
            #       "decision set.")
            predicted_values = merged_pipeline.predict(
                dataframe_from_menu(
                    menu, feature_names=feature_names, study_types=study_types
                )
            )
        except ValueError as err:
            # print(err)
            # print("Training of the sklearn model failed; attempting to"
            #       "select using a flat decision rule value.")
            predicted_values = [0 for mi in menu]
            model = None

        # # Use an epsilon-greedy decision rule to select the si with the best
        # value (p = 1-epsilon) or uniformly at random (epsilon).
        # print("Selecting from the decision rule value using an epsilon"
        #       "greedy rule.")
        si, f_hat_si = select_eps_greedy(predicted_values, menu, EPSILON)
        request_id = "request_" + str(datetime.now()).replace(" ", "_")

        # Write out the study:
        write_selected_study(si, studies, request_id, improv_inbox)

        # Remove this item from the menu
        menu = remove_from_menu(menu, [si])

        # "hallucinate" the result equal to the predicted observation
        history["history"][request_id] = {
            "request_id": request_id,
            "status": "FINISHED",
            "result": {"result": [f_hat_si]}
            }
        for k in si.keys():
            history["history"][request_id][k] = si[k]
        submitted_this_round += 1

    # print("Submitted {} studies. Returning.".format(submitted_this_round))
    return 0


if __name__ == "__main__":
    args = sys.argv[1:]
    main(*args)
