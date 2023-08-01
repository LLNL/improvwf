################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################


"""A simple decision-maker demonstrating reading history writing to the inbox"""
import sys
import random
from argparse import ArgumentParser
from datetime import datetime

from improvwf.utils import get_history, get_history_db, get_menu, get_studies, \
    expand_menu_study_params, remove_from_menu, write_selected_study


def setup_argparser():
    """
    Create an ArgumentParser

    :return: parser, the completed argparser
    """

    parser = ArgumentParser()
    parser.add_argument("-y", "--history", type=str,
                        help="Path to history file")
    parser.add_argument("-b", "--database_url", type=str,
                        help="URL of a study requests database")
    parser.add_argument("-m", "--menu", type=str,
                        help="Path to the menu")
    parser.add_argument("-s", "--studies", type=str,
                        help="Path to studies")
    parser.add_argument("-i", "--inbox", type=str,
                        help="Path to the inbox")
    parser.add_argument('-n', '--nsubmit', type=int,
                        help='Integer number of studies to submit',
                        default=1
    )
    return parser


def main(history_path=None, database_url=None, menu_path=None, studies_path=None,
         improv_inbox=None, to_submit_this_round=1):
    """
    Select a study and parameters.

    Read the history_file, select a study and a set of parameters for
    that study from the menu_file, and write this to the registry_file

    :param history_path: Path to .yaml file containing the history of
    selected study types and parameters.
    :param database_url: URL of the database containing the history. If both
        database_url and history_path are supplied, database_url is preferred.
    :param menu_path: Path to .yaml file containing the requested studies and
    their allowed parameters.
    :param studies_path: Path to the studies directory, containing the
    template studies.
    :param improv_inbox: Path to directory where the requested studies will be
    placed
    :param to_submit_this_round: The integer number of studies to submit in
    this decision cycle [Default: 1]
    """

    if database_url is None and history_path is None:
        raise ValueError("One or the other of database_url or history_path "
                         "must be set to non-None value.")

    submitted_this_round = 0

    # ### Acquire the history, menu, and studies
    if database_url is not None:
        history_list = get_history_db(database_url)
        history = history_list[0]
        if history_path is not None:
            print('WARNING: both database_url and history_path supplied; using'
                  ' database_url and ignoring history_path.')
    else:
        history = get_history(history_path)
    menu = get_menu(menu_path)
    studies = get_studies(studies_path)

    # ### Use the history to remove elements from the menu
    menu = expand_menu_study_params(menu)
    menu = remove_from_menu(menu, list(history["history"].values()))

    # ### Select the study to run

    # If we have run everything on the menu, do not submit a study.
    if not menu:
        return 0

    # If there remain elements in the menu, submit element(s).
    while submitted_this_round < to_submit_this_round:
        si = random.choice(menu["studies"])

        request_id = "request_" + str(datetime.now()).replace(" ", "_")

        # Write out the study:
        write_selected_study(si, studies, request_id, improv_inbox)

        # Remove this item from the menu
        menu = remove_from_menu(menu, [si])

        submitted_this_round += 1
    return 0


if __name__ == "__main__":
    # args = sys.argv[1:]
    # main(*args)

    parser = setup_argparser()
    args = parser.parse_args()

    argparser_fields_to_kwargs = {
        'history': 'history_path',
        'database_url': 'database_url',
        'menu': 'menu_path',
        'studies': 'studies_path',
        'inbox': 'improv_inbox',
        'nsubmit': 'to_submit_this_round'
    }

    main(**{argparser_fields_to_kwargs[ki]: vi
            for ki, vi in vars(args).items()})
