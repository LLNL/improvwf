################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################


"""A template decision-maker"""
import sys
from datetime import datetime

from improvwf.utils import get_history, get_menu, get_studies, \
    expand_menu_study_params, remove_from_menu, write_selected_study


def my_model(history):
    """Return a function, object, or other 'model'

    The returned entity should be predictive of the result values that would be
    obtained by further experiments at new locations. A good choice would be
    a scikit-learn model.
    :argument history: An improv-standard history dictionary: the dictionary
        of all studies which have been run is in history["history"], keyed by
        request_id.
    :return model: a function, object, or other predictive encapsulation of
        the history data."""
    return lambda inputs: [0 for input_i in inputs]


def my_predict(model, studies_list):
    """
    Make a prediction of the results from each candidate study

    If the model is a scikit-learn model, it will implement the
    model.predict() method, which should be used here.
    :param model: A function, object, or other predictive encapsulation of
    the history data;
    :param studies_list: The list of studies which could be run; each is a
        dictionary, containing various improv-standard descriptors.
    :return: predictions for each item in the studies_list
    """
    return model(studies_list)


def my_decision_rule(studies_list, predictions):
    """
    Use the predictions to select a study from the list of studies.
    :param studies_list: A list of studies which could be run; each element
        is a dictionary containing various improv-standard descriptors.
    :param predictions: The output of the model, corresponding to the studies
        in studies_list.
    :return: A selected study from the list.
    """
    return studies_list[0]


def main(history_path=None, menu_path=None, studies_path=None,
         improv_inbox=None, to_submit_this_round=1):
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

    # ### Acquire the history, menu, and studies ###
    history = get_history(history_path)
    menu = get_menu(menu_path)
    studies = get_studies(studies_path)

    # ### Use the history to remove elements from the menu ###
    menu = expand_menu_study_params(menu)
    # Above: expand_menu_study_params assumes that the menu is  discrete;
    # continuous-range specification of menus is not yet implemented.

    menu = remove_from_menu(menu, list(history["history"].values()))
    # Above: remove_from_menu also assumes that the menu is discrete.
    # Removing previously-run objects from the menu assumes experiments
    # are deterministic (no information is gained by repetition), and further,
    # that there is no intrinsic "reward" for repeating "good" experiments;
    # thus there is no reason to repeat an identical experiment.

    # ### Select the study to run ###
    # If we have run everything on the menu, do not submit a study.
    if not menu:
        return 0

    # ### Select studies from the menu ###
    # If there remain elements in the menu, submit element(s).
    submitted_this_round = 0
    while submitted_this_round < to_submit_this_round:
        # Fit your model from the history
        model = my_model(history)

        # Use the model to make predictions about your available studies
        predictions = my_predict(model, menu["studies"])

        # Use the predictions to select a study from the available studies
        si = my_decision_rule(menu["studies"], predictions)

        # Generate a request_id (Unique to avoid collisions as the history is
        # updated; below will generate a request_id which is precise to
        # microsecond level. Using the hostname can also be helpful.)
        request_id = "request_" + str(datetime.now()).replace(" ", "_")

        # Write out the study to the improv inbox
        write_selected_study(si, studies, request_id, improv_inbox)

        # Remove this item from the menu
        menu = remove_from_menu(menu, [si])

        submitted_this_round += 1
    return 0


if __name__ == "__main__":
    args = sys.argv[1:]
    main(*args)
