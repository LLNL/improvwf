################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

"""A script for creating decision-making workers"""
from argparse import ArgumentParser, RawTextHelpFormatter
from filelock import FileLock, Timeout
import os
import shutil
import sys
import yaml
import logging  # TODO: Configure logging.
from time import sleep
import hashlib
import inspect

from mysql.connector.errors import IntegrityError, Error
from sina.model import Record


try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper

from maestrowf.utils import make_safe_path # , create_parentdir
from maestrowf.maestro import run_study  # This is being invoked via Popen in
#  launch_requested_studies().

from improvwf.utils import read_run_descriptors_full_study, \
    verify_source_directory, copy_files_from_string_argument, \
    get_Popen_DEVNULL, yaml_safe_dump_with_lock, yaml_safe_load_with_lock, \
    os_rename_with_lock, append_single_record_to_db
from improvwf.db_interface.utils_sina import DAO, FormatConverter

from improvwf.wrapper_utils import fill_parameters

# Names for global loggers: correspond with maestro
IMPROV_ROOTLOGGER = logging.getLogger(inspect.getmodule(__name__))

# Logging format: matches maestro
IMPROV_LOGGERFORMAT = "%(asctime)s - %(name)s:%(funcName)s:%(lineno)s - " \
          "%(levelname)s - %(message)s"
FILE_TYPE = ".yaml"
IMPROV_PREFIX = "IMPROV_"
# Above: string prefix used in "env":<sub environment> names within the
# decision_study

# Status that a study can take, least-dominant to most-dominant:
IMPROV_STATUS_STRINGS = ["QUEUED", "RUNNING", "FINISHED", "FAILED"]

IMPROV_GLOBAL_HISTORY_LOCK_NAME = ".history.lock" # TODO: Set at pkg level

# Relative to the improv run root directory
IMPROV_PRIVATE_SPACE = "improv"
IMPROV_MAESTRO_WORKSPACES = "workspace"
IMPROV_INBOX = os.path.join(IMPROV_PRIVATE_SPACE, "inbox")
IMPROV_OUTBOX = os.path.join(IMPROV_PRIVATE_SPACE, "outbox")
IMPROV_DECISION_MAKER_ROOT = os.path.join(IMPROV_PRIVATE_SPACE, "decision_maker")
IMPROV_DECISION_STUDY = os.path.join(IMPROV_DECISION_MAKER_ROOT,
                                     "decision_study.yaml")
IMPROV_MENU = os.path.join(IMPROV_DECISION_MAKER_ROOT, "menu.yaml")
IMPROV_STUDIES = os.path.join(IMPROV_DECISION_MAKER_ROOT, "studies")
IMPROV_STUDY_FILE_DIR = os.path.join(IMPROV_STUDIES, "study_files")

IMPROV_WRAPPER_STUDY = os.path.join(IMPROV_DECISION_MAKER_ROOT, "wrapper.yaml")

IMPROV_HISTORY = ""



def create_parentdir(path):
    """
    Utility function that recursively creates parent directories.

    :param path: Path to a directory to be created.
    """
    if not os.path.exists(path):
        path = os.path.expanduser(path)
        os.makedirs(path, exist_ok=True)

def cancel_worker(args):
    """Cancel the improv worker from the command line."""
    # TODO: Fill in function
    # Set the cancellation lock
    pass


def cancel_child_maestros(running_maestros):
    """Cancel all of the child maestros of an improv worker"""
    for mi in running_maestros:
        cmd = ["maestro", "cancel", mi["run_path"]]
        # _ = Popen(" ".join(cmd), shell=True, stdout=DEVNULL,
        #           stderr=DEVNULL)
        _ = get_Popen_DEVNULL(" ".join(cmd), shell=True)
        a, b = _.communicate()


def modify_study_environment_IMPROV(yaml_file_path, environment_vars_to_use):
    """
    Load, modify the environment field of the study, and save back to .yaml

    :param yaml_file_path: path to the yaml file
    :param environment_vars_to_use: dict of lists or dicts, corresponding to
        elements to be added to the Maestro env block.
    """

    # Load the yaml
    d = yaml_safe_load_with_lock(yaml_file_path)

    # Remove all IMPROV-prefixed variables from the yaml study's environment,
    # adding their values and to the appropriate container
    removed_improv_keys = {"env":{}}
    removed_improv_keys_names = set()
    for gk in d["env"].keys():
        if isinstance(d["env"][gk], list):
            removed_improv_keys[gk] = []
            for hk in d["env"][gk]:
                if isinstance(hk, dict) and "name" in hk \
                     and hk["name"].lower().startswith(IMPROV_PREFIX.lower()):
                    IMPROV_ROOTLOGGER.info(
                        "Removing item {} from env:{}".format(hk["name"], gk))
                    removed_improv_keys[gk].append(hk)
                    removed_improv_keys_names.add(hk["name"])
                    d["env"][gk].remove(hk)
        elif isinstance(d["env"][gk], dict):
            removed_improv_keys[gk] = {}
            for hk in d["env"][gk].keys():
                if hk.lower().startswith(IMPROV_PREFIX.lower()):
                    removed_improv_keys[gk][hk] = d["env"][gk].pop(hk)
                    removed_improv_keys_names.add(hk)
        else:
            IMPROV_ROOTLOGGER.warning("An unexpected type appeared in the "
                                      "env of the study being altered!")
            # Don't do anything in this case; shouldn't happen anyway.

    # Log the removed variables:
    IMPROV_ROOTLOGGER.info(
        "Removed the following variables from the environment spec: "
        "{}".format(removed_improv_keys_names))

    # Add the components required
    for vi in environment_vars_to_use.keys():
        # The top level is a dictionary,
        # corresponding to the "env" dictionary in the spec.
        try:
            if isinstance(environment_vars_to_use[vi], list):
                # Concatenate onto the list
                d["env"][vi] = d["env"][vi] + environment_vars_to_use[vi]
                for wi in environment_vars_to_use[vi]:
                    removed_improv_keys_names.discard(wi["name"])
            elif isinstance(environment_vars_to_use[vi], dict):
                for wi in environment_vars_to_use[vi].keys():
                    d["env"][vi][wi] = environment_vars_to_use[vi][wi]
                    removed_improv_keys_names.discard(wi)
        except TypeError:
            IMPROV_ROOTLOGGER.error("Mismatched types in the yaml spec and "
                                    "desired environment variables!")

    if removed_improv_keys_names:
        IMPROV_ROOTLOGGER.info("The keys {} were removed and not "
                               "replaced!".format(removed_improv_keys_names))
    # Write the yaml back to file
    yaml_safe_dump_with_lock(d, yaml_file_path)


def append_to_history(args):
    """Append an entry to the specified history file"""
    result = None
    if "result" in args:
        result = args.result

    if args.history == "" and args.database_url == "":
        raise ValueError("Only one of history_file or database_url may be given.")
    elif args.history != "" and args.database_url != "":
        raise ValueError("One of history_file or database_url must be given.")

    if args.history is not "":
        retcode = _append_to_history(args.history, [args.request_id],
                                     [args.run_descriptors], [args.status],
                                     [result])
    else:
        retcode = _append_to_history_db(
            DAO(args.database_url), [args.request_id], [args.run_descriptors],
            [args.status], [result], args.id)
    # Note that retcode can either be 0 (successful logging) or 1
    # TODO: Evaluate different handling for failure to log

    return retcode


def _append_to_history_db(dao, request_id_list, run_descriptors_list,
                          status_list, result_list, requester=None):
    """
    Private: Append an entry to history database.

    Note that this function does NOT initialize the database.

    :param dao: Database access object, providing a sina interface to the
        database.
    :param request_id_list: list of str, giving the request_id values for the
        individual studies.
    :param run_descriptors_list: list of dictionaries, giving the
        run_descriptors for the individual studies
    :param status_list: list of status str for the studies
    :param result_list: list of results; either None (for studies in progress)
        or a dictionary for completed studies.
    :param requester: The id of the agent that requested the study be run.
                      Used in tracking relationships.
    :return retcode, either 0 or 1.
    """
    all_lists = [isinstance(li, list) for li in
                 [request_id_list, run_descriptors_list, status_list,
                  result_list]]
    if not all(all_lists):
        IMPROV_ROOTLOGGER.error("Some of the inputs to _append_to_history_db "
                                "are not lists!")

    all_lengths = [len(li) for li in
                   [request_id_list, run_descriptors_list, status_list,
                    result_list]]
    if not all([li == all_lengths[0] for li in all_lengths]):
        IMPROV_ROOTLOGGER.error("Some of the inputs to _append_to_history_db "
                                "are not of the same length!")

    if dao and len(request_id_list) > 0:
        try:

            IMPROV_ROOTLOGGER.debug(
                "Preparing to append to DAO {}.".format(dao)
            )
            for request_id_i, run_descriptors_i, status_i, result_i in zip(
                    request_id_list, run_descriptors_list, status_list,
                    result_list):
                append_single_record_to_db(
                    dao=dao, logger=IMPROV_ROOTLOGGER,
                    request_id=request_id_i, status=status_i,
                    run_descriptors=run_descriptors_i, result=result_i,
                    requester=requester)

            IMPROV_ROOTLOGGER.debug(
                "Done appending to DAO {}.".format(dao))
        except Exception as e:
            IMPROV_ROOTLOGGER.warning(
                "Failed to append to DAO {}.".format(dao))
            IMPROV_ROOTLOGGER.warning("Exception reads: {}".format(e))
            return 1

    return 0


def _append_to_history(history_file, request_id_list, run_descriptors_list,
                       status_list, result_list, lock_acquire_time=None):
    """
    Private: Append an entry to history file; if req'd, initialize file

    :param history_file: str, giving path to the history file.
    :param request_id_list: list of str, giving the request_id values for the
        individual studies.
    :param run_descriptors_list: list of dictionaries, giving the
        run_descriptors for the individual studies
    :param status_list: list of status str for the studies
    :param result_list: list of results; either None (for studies in progress)
        or a dictionary for completed studies.
    :return retcode, either 0 or 1.
    """

    if lock_acquire_time is None:
        lock_acquire_time=30

    all_lists = [isinstance(li, list) for li in [request_id_list, run_descriptors_list, status_list, result_list]]
    if not all(all_lists):
        IMPROV_ROOTLOGGER.error("Some of the inputs to _append_to_history are "
                                "not lists!")

    all_lengths = [len(li) for li in [request_id_list, run_descriptors_list, status_list, result_list]]
    if not all([li == all_lengths[0] for li in all_lengths]):
        IMPROV_ROOTLOGGER.error("Some of the inputs to _append_to_history are "
                                "not of the same length!")

    history_path = history_file
    lock_path = os.path.join(os.path.split(history_path)[0], IMPROV_GLOBAL_HISTORY_LOCK_NAME)
    _ = dict()
    _["history"] = dict()
    if not os.path.exists(history_path):
        try:
            lock = FileLock(lock_path)
            with lock.acquire(timeout=lock_acquire_time):
                with open(history_path, "w") as history_file:
                    yaml.dump(_, history_file, Dumper=SafeDumper)
        except Timeout:
            IMPROV_ROOTLOGGER.error(
                "Failed to initialize history file {}.".format(history_path))
            pass

    if os.path.exists(history_path) and len(request_id_list) > 0:
        # And the request_id, run_descriptors, and status are not None?
        try:

            lock = FileLock(lock_path)

            with lock.acquire(timeout=lock_acquire_time):
                # TODO: Evaluate better locking and timeout for history append.
                # Consequences could be severe if we fail to append correctly.

                # Load the history yaml file
                with open(history_path, "r") as history_file:
                    _ = yaml.load(history_file, Loader=SafeLoader)

                IMPROV_ROOTLOGGER.debug(
                    "Preparing to append to history file {}.".format(
                        history_path
                    )
                )
                for request_id_i, run_descriptors_i, status_i, result_i in zip(
                        request_id_list, run_descriptors_list, status_list,
                        result_list):
                    IMPROV_ROOTLOGGER.debug(
                        "Appending request_id {} to history file {}.".format(
                            request_id_i, history_path)
                    )
                    # OVERWRITE an existing history entry with this request_id
                    _["history"][request_id_i] = {"request_id": request_id_i}
                    for ki in run_descriptors_i.keys():
                        # TODO: Block the case where request_id shows up here?
                        _["history"][request_id_i][ki] = run_descriptors_i[ki]
                    # TODO: Check that status changes follow precedence
                    _["history"][request_id_i]["status"] = status_i
                    if result_i is not None:
                        _["history"][request_id_i]["result"] = result_i

                IMPROV_ROOTLOGGER.debug(
                    "Done appending to history file {}.".format(
                        history_path))
                # Write the history file back.
                with open(history_path, "w") as history_file:
                    yaml.dump(_, history_file, Dumper=SafeDumper)

                IMPROV_ROOTLOGGER.debug(
                    "Done writing back to history file {}.".format(
                        history_path))
        except Timeout as err:
            IMPROV_ROOTLOGGER.warning(
                "Failed to append to history file {}.".format(history_path))
            # raise(err)
            return 1

    return 0


def _append_to_history_with_pending_logging(
        history_file, request_id_list, run_descriptors_list,
        status_list, result_list, lock_acquire_time=None,
        pending_log_actions=None, requester=None):
    """
    Private: wrapper for _append_to_history with pending_log_actions

    Maintains dictionary of history files and any pending logging to them

    :param history_file: str or DAO. If str, gives path to the history file; if
        DAO, provides access interface.
    :param request_id_list: list of str, giving the request_id values for the
        individual studies.
    :param run_descriptors_list: list of dictionaries, giving the
        run_descriptors for the individual studies
    :param status_list: list of status str for the studies
    :param result_list: list of results; either None (for studies in progress)
        or a dictionary for completed studies.
    :param pending_log_actions: dict, keyed by history file paths (str),
        containing information on pending logging for studies.
    :param requester: str, the id of the agent that submitted the study. Only
                      used with a DAO. Used to track relationships.
    :return pending_log_actions
    """

    # TODO: Assess incompatibilities with DAO, presumably unhashable

    if pending_log_actions is None:
        pending_log_actions = {}

    IMPROV_ROOTLOGGER.debug(
        "At the start of _append_to_history_with_pending_logging, "
        "pending_log_actions is: {}".format(pending_log_actions)
    )

    # Consult the pending_log_actions to determine what study logging events
    # need to be logged to this history file
    # TODO: Assess incompatibilities with DAO, presumably unhashable
    loc_dict = _get_latest_logging_actions(
        pending_log_actions,
        str(history_file),
        {"request_id_list": request_id_list,
         "run_descriptors_list": run_descriptors_list,
         "status_list": status_list,
         "result_list": result_list
         }
    )

    IMPROV_ROOTLOGGER.debug("Attempting to log the following dictionary of "
                            "statuses: {}".format(loc_dict))

    # Attempt to append the correct elements to the history file
    # TODO: Switching for case that history_file is actually a DAO
    if isinstance(history_file, DAO):
        retcode = _append_to_history_db(
            history_file,
            loc_dict["request_id_list"],
            loc_dict["run_descriptors_list"],
            loc_dict["status_list"],
            loc_dict["result_list"],
            requester=requester)
    else:
        retcode = _append_to_history(
            history_file,
            loc_dict["request_id_list"],
            loc_dict["run_descriptors_list"],
            loc_dict["status_list"],
            loc_dict["result_list"],
            lock_acquire_time)

    # According to retcode (failure=1, success=0), update the pending actions
    # TODO: Assess incompatibilities with DAO, presumably unhashable
    _update_pending_log_actions(
        pending_log_actions,
        retcode,
        str(history_file),
        loc_dict
    )

    IMPROV_ROOTLOGGER.debug(
        "At the end of _append_to_history_with_pending_logging, where the "
        "_append_to_history retcode was {}, pending_log_actions "
        "is: {}".format(retcode, pending_log_actions))

    return pending_log_actions


def _update_pending_log_actions(pending_log_actions, retcode_from_append,
                                history_dest_str, dictionary_of_statuses):
    """

    :param pending_log_actions: dictionary of history files with a series of
        study status descriptions to write to those history files.
    :param retcode_from_append: 0 or 1: if 0, delete entries with the same
        history file, request_id, run_descriptors values and equal or lower
        status_list values from pending_log_actions; if 1, delete entries with
        the same history file, request_id, run_descriptors values and add these
        values; throw an error if encountering a pending value of higher
        precedence in status, or a case where there are results in the pending
        item but not in the overwriting item.
    :param history_dest_str: str, giving the path to the history file to which the
        writes are pending.
    :param dictionary_of_statuses: dict, containing keys "request_id_list",
        "run_descriptors_list", "status_list", and "result_list"; the values
        are lists of matching length.
    """
    # TODO: Assess compatibility with DAO, which is presumably not directly hashable

    # Reconcile any redundancies in dictionary_of_statuses: there shouldn't be
    # any present, but to be on the safe side, resolve.
    dictionary_of_statuses = _elim_from_stat_dict(dictionary_of_statuses)

    # Ensure pending_log_actions has this history file
    if history_dest_str not in pending_log_actions.keys():
        pending_log_actions[history_dest_str] = {
            "request_id_list": [],
            "run_descriptors_list": [],
            "status_list": [],
            "result_list": []
        }

    # Eliminate any redundancies in pending_log_actions and studies dominated
    # by those appearing in dictionary_of_statuses
    pending_log_actions[history_dest_str] = _elim_from_stat_dict(
        pending_log_actions[history_dest_str], dictionary_of_statuses
    )

    # An error was encountered; add the studies for which logging failed
    if retcode_from_append == 1:
        # Note that, due to _elim_from_stat_dict, we've already resolved
        # dominance and redundant keying
        # TODO: Handle case of no dominance, same rid
        pending_log_actions[history_dest_str] = {
            "request_id_list":
                pending_log_actions[history_dest_str]["request_id_list"]
                + dictionary_of_statuses["request_id_list"],
            "run_descriptors_list":
                pending_log_actions[history_dest_str]["run_descriptors_list"]
                + dictionary_of_statuses["run_descriptors_list"],
            "status_list":
                pending_log_actions[history_dest_str]["status_list"]
                + dictionary_of_statuses["status_list"],
            "result_list":
                pending_log_actions[history_dest_str]["result_list"]
                + dictionary_of_statuses["result_list"]
        }

    # Check if pending_log_actions[history_dest_str] is now 'empty';
    # if it is, pop.
    if not pending_log_actions[history_dest_str]["request_id_list"]:
        # Empty list
        pending_log_actions.pop(history_dest_str)

    return pending_log_actions


def _elim_from_stat_dict(dict_a, dict_b=None):
    """
    Remove redundant elements from status dictionary a, and any dominated by b

    :param dict_a: dict, containing keys "request_id_list",
        "run_descriptors_list", "status_list", and "result_list"; the values
        are lists of matching length.  Note that "result_list" will have None
        entries typically (no result yet from the study), whereas the others
        will always have non-None values.
    :param dict_b: None or dict containing keys "request_id_list",
        "run_descriptors_list", "status_list", and "result_list"; the values
        are lists of matching length.  If None, no check for domination.
    :return: dict_a
    """

    if dict_b is None:
        dict_b = {"request_id_list": [],
                  "run_descriptors_list": [],
                  "status_list": [],
                  "result_list": []
                  }

    to_eliminate_a = []
    for i, tup_i in enumerate(zip(
            dict_a["request_id_list"], dict_a["run_descriptors_list"],
            dict_a["status_list"], dict_a["result_list"]
    )):
        append_flag = False
        for j, tup_j in enumerate(zip(
            dict_a["request_id_list"], dict_a["run_descriptors_list"],
            dict_a["status_list"], dict_a["result_list"]
        )):

            if i == j:
                continue

            # If tup_i and tup_j have the same rid but neither is dominant and
            # they are not identical, throw an error.
            if tup_i[0] == tup_j[0] and not _study_tup_identity(tup_i, tup_j):
                neither_dominant = not _study_tup_g_precedence(tup_i, tup_j) \
                                   and not _study_tup_g_precedence(tup_j, tup_i)
                if neither_dominant:
                    IMPROV_ROOTLOGGER.error("Two studies encountered that share"
                                            " the same ID ({}) but neither is "
                                            "dominant; cannot resolve "
                                            "conflict.".format(tup_i[0])
                                            )

            # Does tup_j's precedence exceed tup_i's?
            if _study_tup_g_precedence(tup_i, tup_j):
                append_flag = True

            # Is tup_j identical and not yet eliminated?
            if _study_tup_identity(tup_i, tup_j) and j not in to_eliminate_a:
                append_flag = True

            if append_flag:
                break

        # Check if there exists an object in dict_b with the same rid that is
        # dominant or identical; if yes, add i to to_eliminate_a and
        # continue
        # TODO: handle case: rid equality but neither dominance but not identity
        if any([
            _study_tup_g_precedence(tup_i, tup_b_j) or
            _study_tup_identity(tup_i, tup_b_j)
            for tup_b_j in zip(
                dict_b["request_id_list"], dict_b["run_descriptors_list"],
                dict_b["status_list"], dict_b["result_list"]
            )
        ]):
            append_flag = True

        if append_flag:
            to_eliminate_a.append(i)

    dict_a = {
        "request_id_list": [
            rid_i for i, rid_i in enumerate(dict_a["request_id_list"])
            if i not in to_eliminate_a
        ],
        "run_descriptors_list": [
            rdesc_i for i, rdesc_i in enumerate(dict_a["run_descriptors_list"])
            if i not in to_eliminate_a
        ],
        "status_list": [
            stat_i for i, stat_i in enumerate(dict_a["status_list"])
            if i not in to_eliminate_a
        ],
        "result_list": [
            res_i for i, res_i in enumerate(dict_a["result_list"])
            if i not in to_eliminate_a
        ]
    }

    return dict_a


def _study_tup_g_precedence(tup_a, tup_b):
    """
    Check if tup_b has precedence over tup_a

    :param tup_a: tuple: (id, description, status, result); result may be None
    :param tup_b: tuple: (id, description, status, result); result may be None
    :return: True if tup_b has precedence over a, False otherwise
    """

    if tup_a[0] != tup_b[0]:  # IDs differ
        return False

    # IDs match
    # Descriptions should too
    if tup_a[1] != tup_b[1]:
        IMPROV_ROOTLOGGER.error("Two studies with the same ID ({}) but "
                                "different descriptions have been "
                                "detected.".format(tup_a[0]))

    # ID and description match:
    status_string_dominance = IMPROV_STATUS_STRINGS.index(tup_b[2]) > \
                              IMPROV_STATUS_STRINGS.index(tup_a[2])
    results_dominance = (tup_b[3] is not None) and (tup_a[3] is None)

    # Check for the weird XOR cases
    if status_string_dominance and (tup_a[3] is not None and tup_b[3] is None):
        IMPROV_ROOTLOGGER.error("Study {} has a conflict between logged status "
                                "and logged results in two versions of the "
                                "logging".format(tup_a[0]))

    if results_dominance and IMPROV_STATUS_STRINGS.index(tup_b[2]) < \
                              IMPROV_STATUS_STRINGS.index(tup_a[2]):
        IMPROV_ROOTLOGGER.error("Study {} has a conflict between logged status "
                                "and logged results in two versions of the "
                                "logging".format(tup_a[0]))
    if results_dominance or status_string_dominance:
        return True

    return False


def _study_tup_identity(tup_a, tup_b):
    """
    Check if tup_a and tup_b are identical

    :param tup_a: tuple: (id, description, status, result); result may be None
    :param tup_b: tuple: (id, description, status, result); result may be None
    :return: True if tup_b and tup_a are equal, False otherwise
    """

    if all([ai == bi for ai, bi in zip(tup_a[:-1], tup_b[:-1])]):
        if (tup_a[-1] is None) and (tup_b[-1] is None):
            return True
        elif tup_a[-1] == tup_b[-1]:
            return True

    return False


def _get_latest_logging_actions(pending_log_actions, history_dest_str,
                                dictionary_of_statuses):
    """
    Retrieve the pending log actions using key str for history file/DAO

    :param pending_log_actions: dict, keyed by str, giving lists of
        requests pending for a particular history.
    :param history_dest_str: str, giving either a file path or a database URL.
    :param dictionary_of_statuses: dict containing updated statuses of studies.
    :return: pending log actions for the given history destination, merging the
        contents of pending_log_actions with the latest status information.
    """

    # Retrieve the relevant part of pending_log_actions
    pla_this_hf = {"request_id_list": [],
                   "run_descriptors_list": [],
                   "status_list": [],
                   "result_list": []
                   }
    if history_dest_str in pending_log_actions.keys():
         # TODO: Assess compatibility with DAO, presumably unhashable
         pla_this_hf = pending_log_actions[history_dest_str]

    # Eliminate any redundancies and anything dominated by dictionary_of_statuses
    to_add_pla_this_hf = _elim_from_stat_dict(pla_this_hf, dictionary_of_statuses)

    # Create the dictionary to write out
    dict_out = {
            "request_id_list":
                to_add_pla_this_hf["request_id_list"]
                + dictionary_of_statuses["request_id_list"],
            "run_descriptors_list":
                to_add_pla_this_hf["run_descriptors_list"]
                + dictionary_of_statuses["run_descriptors_list"],
            "status_list":
                to_add_pla_this_hf["status_list"]
                + dictionary_of_statuses["status_list"],
            "result_list":
                to_add_pla_this_hf["result_list"]
                + dictionary_of_statuses["result_list"]
        }

    return dict_out


def check_in_history(args):
    """Check the history: does named study appear with a given status?

    Returns 0 if the study appears with the given status, 1 otherwise.
    """
    hp = args.history
    db = args.database_url

    if hp == "":
        hp = None
    if db == "":
        db = None

    retcode = _check_in_history(args.request_id, args.status, history_path=hp,
                                db_url=db)
    return retcode


# TODO: Check DB/DAO version
def _check_in_history(request_id, status, history_path=None, db_url=None):
    """Private function for checking specified study's status in history"""

    if history_path is None and db_url is None:
        raise ValueError("One or the other of history_path or db_url must be given.")
    elif history_path is not None and db_url is not None:
        raise ValueError("Only one of history_path or db_url may be given.")

    retcode = 1

    # Database case:
    if db_url is not None:
        dao = DAO(db_url)
        rec = dao.__getitem__(request_id)
        if rec["status"] == status:
            retcode = 0
        return retcode

    # History file case:
    lock_path = os.path.join(os.path.split(history_path)[0], IMPROV_GLOBAL_HISTORY_LOCK_NAME)
    if os.path.exists(history_path):
        lock = FileLock(lock_path)
        try:
            with lock.acquire(timeout=30):
                with open(history_path, "r") as history_file:
                    _ = yaml.load(history_file, Loader=SafeLoader)
                    if request_id in _["history"]:
                        if _["history"][request_id]["status"] == status:
                            retcode = 0
                            return retcode
        except Timeout:  # TODO: Add FileNotFoundError
            IMPROV_ROOTLOGGER.warning("Failed to acquire lock or check in "
                                      "history file {}.".format(history_path))
            pass

    return retcode


# TODO: Not sure what the right file is for this one. Should it go in utils_sina instead?
def ingest_menu_file(yaml_path, dao, yaml_lockpath=None, yaml_acquiretime=30):
    """
    Insert the information from some menu file into a Sina database, return the name of the menu inserted.

    :param yaml_path: (str) Path to the yaml representation of the menu
    :param dao: (DAO) Dao to insert through.
    :param yaml_lockpath: (str) Path to its lockfile
    :param yaml_acquiretime: (int) Timeout for acquiring it (seconds?)
    :returns: (str) The menu's name
    """
    menu = yaml_safe_load_with_lock(yaml_path, yaml_lockpath, yaml_acquiretime)
    studies = menu["studies"]
    study_hashes = []
    converter = FormatConverter()
    for study in studies:
        study_hash = hashlib.md5(str(study).encode('utf-8')).hexdigest()
        study_hashes.append(study_hash)
        # Check if the study exists:
        try:
            dao.recs.get(study_hash)
        # Our study doesn't exist yet; make it
        except ValueError:
            study_record = converter.get_sina_study_template_from_yaml(study, study_hash)
            dao.recs.insert(study_record)

    # Unlike studies, menus are named
    # It also seems like menus can be reused, so we'll just skip the insert if it's already in
    try:
        dao.recs.get(menu["description"]["name"])
    except ValueError:
        menu_record = converter.get_sina_menu_from_yaml(menu)
        dao.recs.insert(menu_record)

        # Now that both the menu and studies are inserted, we can insert the
        # relationships binding the two.
        for study_hash in study_hashes:
            dao.rels.insert(subject_id=menu_record.id,
                            predicate="provides template",
                            object_id=study_hash)

    return menu["description"]["name"]

def list_requested_studies(inbox=None, file_type=None):
        """
        Check the inbox for newly-requested studies.
        :param inbox: path to directory or file with new studies
        :param file_type: the file type of the expected study requests
        :return: files_to_read: a list of newly requested study files
        """
        files_to_read = []
        if inbox is None:
            IMPROV_ROOTLOGGER.warning("No inbox directory or file passed.")
            return files_to_read

        if file_type is None:
            file_type = ".yaml"

        # Main check for requests:
        if os.path.isfile(inbox):
            # There is a single file; read it
            files_to_read = [inbox]
        elif os.path.isdir(inbox):
            files_to_read = os.listdir(inbox)
            files_to_read = [os.path.join(inbox, fi) for fi
                             in files_to_read if
                             os.path.splitext(fi)[-1].lower() ==
                             file_type.lower()]
        else:
            IMPROV_ROOTLOGGER.debug("No study files found.")
            return files_to_read

        IMPROV_ROOTLOGGER.debug("Found new study requests.")
        return files_to_read


def launch_requested_studies(running_maestros, study_request_files,
                             workspace_path, outbox,
                             log_global=None, maestro_flags=None,
                             history_file=None, database_access_obj=None,
                             local_history_file=None,
                             pending_log_actions=None,
                             requester=None, srun=False):
    """
    Launch all requested studies

    :param running_maestros: dictionary of currently running child maestros,
        keyed by request_id.
    :param study_request_files: list of paths to request .yaml files in
        inbox; launch a child maestro for each.
    :param workspace_path: string, giving path to directory in which the
        child maestros' roots will be initalized
    :param outbox: string, giving path to outbox directory, where request
        .yaml files will be moved.
    :param log_global: logical or None; if False, no global logging, else,
        global logging. Allows passing separately to the running_maestros
        dictionary.
    :param maestro_flags: string or list, givng a set of flags to be passed
        to each child maestro.
    :param history_file: None or string, giving path to global history file.
        If None, no global (.yaml) logging, even if log_global is not False.
    :param database_access_obj: None or DAO, giving an interface to the history
        database.
    :param local_history_file: None or string, giving path to local history
        file. If None, no local logging.
    :param pending_log_actions: dict, keyed by history file paths (str),
        containing information on pending logging for studies.
    :param requester: str, id of the agent that requested the study be run.
                      Only ued with database_access_obj for logging
                      relationships.
    :param srun: bool, determine if subprocesses should be launched with srun -n 1.
                      Used in the case where child maestros are parellelized across
                      multiple nodes.
    :returns: running_maestros: dictionary of running child maestros,
            including subprocess Popen handles.
        pending_log_actions: a dict of dicts of failed logging, keyed by history
            file path; each has a 'history_file' key giving a path to the
            particular history file to which the logging failed.
    """

    # TODO: Troubleshoot DAO use

    if maestro_flags is None:
        maestro_flags = []
    elif isinstance(maestro_flags, str):
        maestro_flags = maestro_flags.split(" ")

    if log_global is None:
        log_global = True  # Default to doing full, global logging.

    if pending_log_actions is None:
        pending_log_actions = {}

    request_ids_launching = []
    run_descriptors_launching = []
    for sfi in study_request_files:
        # ### Move the study spec file to the outbox ###
        # This is done here to avoid doing so during maestro's startup
        # process, which causes maestro to crash
        if outbox is not None:
            sfo = os.path.join(outbox, os.path.split(sfi)[-1])
            os_rename_with_lock(sfi, sfo)
        else:
            sfo = sfi

        # ### Gather all required information to launch the study ###
        # Obtain the request_id, run_descriptors from the study's
        # specification.
        run_descriptors, request_id = read_run_descriptors_full_study(sfo)


        # Specify the study's output path:
        try:
            study_out_path = "_".join([run_descriptors["study_type"],
                                       request_id])
        except (KeyError, TypeError):
            study_out_path = "req_" + request_id
        study_out_path = os.path.join(workspace_path, study_out_path)

        # Command-line invocation of maestro run ...
        if srun:
            cmd = ["srun",  "-n", "1", "--exclusive", "maestro", "run"] + maestro_flags + ["-o", study_out_path] \
              + ["-fg", "-y", sfo]
        else:
            cmd = ["maestro", "run"] + maestro_flags + ["-o", study_out_path] \
              + ["-fg", "-y", sfo]
        # Since we're launching this via Popen, we want a foreground
        # execution of the conductor; include the -fg flag.

        # ### Actually launch the study ###
        IMPROV_ROOTLOGGER.debug("Invoking maestro:" + " ".join(cmd))
        running_maestros[request_id] = {
            "request_id": request_id,
            "run_descriptors": run_descriptors,
            "run_path": study_out_path,
            "Popen": get_Popen_DEVNULL(" ".join(cmd), shell=True),
            # Previously: Popen(" ".join(cmd), shell=True,
            # stdout=DEVNULL, stderr=DEVNULL),
            "log_global": log_global,
            "retcode": None}
        # Above: passing stdout and stderr to PIPE quickly overflows the
        # buffer.

        request_ids_launching.append(request_id)
        run_descriptors_launching.append(run_descriptors)

    # Generate the other two lists needed for logging the newly-launched studies
    status_list = ["RUNNING" for rid in request_ids_launching]
    result_list = [None for rid in request_ids_launching]

    # ### Log the launch to the history files ###
    valid_hfs = [local_history_file]
    if log_global:
        valid_hfs = [history_file, database_access_obj, local_history_file]
    valid_hfs = [hf for hf in valid_hfs if hf is not None]

    for hf in valid_hfs:
        pending_log_actions = _append_to_history_with_pending_logging(
            hf, request_ids_launching,
            run_descriptors_launching,
            status_list,
            result_list,
            lock_acquire_time=300,  # Critical to log the start of the studies
            pending_log_actions=pending_log_actions,
            requester=requester
        )


    return running_maestros, pending_log_actions


def monitor_and_log(running_maestros, history_file=None,
                    database_access_obj=None,
                    local_history_file=None, pending_log_actions=None):
    """
    Update the status of the maestro run subprocesses.

    :param running_maestros: A dictionary of the maestro subprocesses; keyed
        by the request_ID with which the request was submitted. The Popen field
        contains the subprocess handle, which will be polled to check its
        status.
    :param history_file: string giving path to the global history file. If
        None, no global history logging.
    :param database_access_obj: DAO or None, giving interface to the global
        history database.
    :param local_history_file: string giving path to the local history file.
        If None, no local logging.
    :param pending_log_actions: dict, keyed by history file paths (str),
        containing information on pending logging for studies.
    :return: running_maestros and pending_log_actions: dictionary, with any
        finished/failed maestros removed; dictionary, with any pending (i.e.,
        failed) logging actions.
    """

    # TODO: Troubleshoot DAO use

    # Check the status of the running studies:
    for ki in running_maestros:
        running_maestros[ki]["retcode"] = running_maestros[ki][
            "Popen"].poll()

    keys_to_pop = []
    run_descriptors_keys_to_pop = []
    status_keys_to_pop = []
    results_keys_to_pop = []
    log_global_keys_to_pop = []
    for ki in running_maestros:
        # valid_hfs = [hf for hf, log in
        #              zip([history_file, local_history_file],
        #                  [running_maestros[ki]["log_global"], True])
        #              if log and hf is not None]
        if running_maestros[ki]["retcode"] is None:
            pass
        elif running_maestros[ki]["retcode"] == 0:
            # Record success
            try:  # Logic below; a study without a results.yaml is a failure.
                # Obtain the fields needed to write to the history
                p = os.path.join(running_maestros[ki]["run_path"],
                                 "results.yaml")
                result = yaml_safe_load_with_lock(p)


                # # Append to the history file
                # for hf in valid_hfs:
                #     _append_to_history(
                #         hf, ki, running_maestros[ki]["run_descriptors"],
                #         "FINISHED", result)
                status_keys_to_pop.append("FINISHED")
                results_keys_to_pop.append(result)
            except FileNotFoundError:
                IMPROV_ROOTLOGGER.warning(
                    "Results not found for study {} in directory {}.".format(
                        running_maestros[ki]["request_id"],
                        running_maestros[ki]["run_path"]))
                # for hf in valid_hfs:
                #     _append_to_history(
                #         hf, ki, running_maestros[ki]["run_descriptors"],
                #         "FAILED")
                status_keys_to_pop.append("FAILED")
                results_keys_to_pop.append(None)
            # Mark for deletion from running_maestros
            keys_to_pop.append(ki)
            run_descriptors_keys_to_pop.append(
                running_maestros[ki]["run_descriptors"]
            )
            log_global_keys_to_pop.append(running_maestros[ki]["log_global"])
        else:  # The return code is some failure.
            # # Record failure
            # for hf in valid_hfs:
            #     _append_to_history(
            #         hf, ki, running_maestros[ki]["run_descriptors"],
            #         "FAILED")
            status_keys_to_pop.append("FAILED")
            results_keys_to_pop.append(None)
            # Mark for deletion from running_maestros
            keys_to_pop.append(ki)
            run_descriptors_keys_to_pop.append(
                running_maestros[ki]["run_descriptors"]
            )
            log_global_keys_to_pop.append(running_maestros[ki]["log_global"])

    # Do all of the logging from above:
    # Global logging:
    for globhist in [history_file, database_access_obj]:
        if globhist is None:
            continue
        pending_log_actions = _append_to_history_with_pending_logging(
            globhist,
            [ktp_i    for ktp_i, g_i    in zip(keys_to_pop,                 log_global_keys_to_pop) if g_i],
            [rd_ktp_i for rd_ktp_i, g_i in zip(run_descriptors_keys_to_pop, log_global_keys_to_pop) if g_i],
            [s_ktp_i  for s_ktp_i, g_i  in zip(status_keys_to_pop,          log_global_keys_to_pop) if g_i],
            [r_ktp_i  for r_ktp_i, g_i  in zip(results_keys_to_pop,         log_global_keys_to_pop) if g_i],
            lock_acquire_time=30,
            pending_log_actions=pending_log_actions
        )

    # Local logging:
    if local_history_file is not None:
        # Local history will not be a db
        pending_log_actions = _append_to_history_with_pending_logging(
            local_history_file,
            keys_to_pop,
            run_descriptors_keys_to_pop,
            status_keys_to_pop,
            results_keys_to_pop,  # Using default lock_acquire_time
            pending_log_actions = pending_log_actions
        )

    # Remove keys which correspond to finished maestro processes
    if keys_to_pop:
        IMPROV_ROOTLOGGER.info("Study(-ies) {} completed.".format(keys_to_pop))

    for ki in keys_to_pop:
        del running_maestros[ki]

    return running_maestros, pending_log_actions


def setup_argparser():
    """Set up the argument parser.

    Code follows Maestro's implementation of maestro.setup_argparser
    Note that the shared -l and -d options must be invoked BEFORE the subcommand
    if used.
    """

    # TODO: Assess if command-line access to DB needed in check, append

    parser = ArgumentParser(
        prog="improv",
        description="The Improv add-on for dynamic decision-making in Maestro.",
        formatter_class=RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest="subparser")

    # ### append_history ###
    append_history = subparsers.add_parser(
        "append_history", help="Append information on a sub-study to the "
                               "history file.")
    append_history.add_argument("-H", "--history", type=str,
                                help="The path to the history file.",
                                default="")
    append_history.add_argument("-b", "--database_url", type=str,
                                help="URL of a study requests database.",
                                default="")
    append_history.add_argument(
        "request_id", type=str,
        help="The string indicating the unique request ID.")
    append_history.add_argument(
        "run_descriptors", type=yaml.safe_load,  # TODO: remove func or change
        help="The set of descriptive strings regarding this run.")
    append_history.add_argument("status", type=str,
                                choices=IMPROV_STATUS_STRINGS,
                                help="The string of QUEUED, FAILED, RUNNING, "
                                     "FINISHED matching the sub-study's "
                                     "status.")
    append_history.add_argument("-r", "--result", type=str,
                                help="A string containing results for this "
                                     "sub-study.")
    append_history.set_defaults(func=append_to_history)

    # ### check_history ###
    check_history = subparsers.add_parser("check_history",
                                          help="Check if the history file "
                                               "contains a job matching a given"
                                               " request_id and status.")
    check_history.add_argument("-H", "--history", type=str,
                               help="The path to the history file.",
                               default="")
    check_history.add_argument("-b", "--database_url", type=str,
                               help="URL of a study requests database.",
                               default="")
    check_history.add_argument(
        "request_id", type=str,
        help="The string indicating the unique request ID.")
    check_history.add_argument("status", type=str,
                               choices=IMPROV_STATUS_STRINGS,
                               help="The string of QUEUED, FAILED, RUNNING, "
                                    "FINISHED matching the sub-study's "
                                    "status.")
    check_history.set_defaults(func=check_in_history)

    # ### run ###
    run = subparsers.add_parser("run", help="Configure and run an improv "
                                            "worker daemon.")

    run.add_argument("worker_path", type=str, help="The root directory of the "
                                                   "worker's file structure.")
    run.add_argument("source_directory", type=str,
                     help="The source directory from which the worker will copy"
                          " the decision-making files (typically created by the"
                          " master)")
    # ID is optional because it's only used for logging
    run.add_argument("-i", "--id", type=str, default=None,
                     help="String giving the ID of the improv run daemon, "
                          "used in specifying directories, paths, "
                          "and other per-worker settings.")
    run.add_argument("-H", "--history", type=str,
                     help="The full path to the GLOBAL history file.",
                     default="")
    run.add_argument("-b", "--database_url", type=str,
                     help="URL of a study requests database.", default="")
    run.add_argument("-cl", "--cancel_lock_path", default="",
                     type=str, help="The name of the cancel lock file; "
                                    "either in the study's root directory or "
                                    "an absolute path. [Default: "
                                    "<worker_path>/.cancel.lock]")
    run.add_argument("-tl", "--term_lock_path", default="",
                     type=str, help="The name of the termination lock file; "
                                    "either in the study's root directory or "
                                    "an absolute path. [Default: "
                                    "<worker_path>/.term.lock]")
    run.add_argument("-s", "--sleeptime", default=5, type=int,
                     help="The number of seconds for the improv run daemon to "
                          "sleep between cycles. [Default: %(default)s]")
    run.add_argument("-m", "--maestro_flags", default="", type=str,
                     help="Any command line flags which should be passed to "
                          "the child maestro runs. [Default: Empty string (no flags)]")
    run.add_argument("-sr", "--srun", action="store_true",
                     help="whether studies should be launched using srun -n 1 --exclusive"
                     " which will allow multinode jobs to use all node cores")
    run.add_argument("-w", "--wrapper", default=False, type=bool,
                     help="Use the wrapper workflow with tail-end decision makers.")
    run.set_defaults(func=run_worker)

    # ### prepare ###
    prepare = subparsers.add_parser(
        "prepare", help="On the master file system, prepare and arrange files "
                        "for the worker improv run daemons.")
    prepare.add_argument("-o", "--output_path", type=str,
                         help="Path to the desired output directory, where "
                              "source will be created. Defaults to cwd.",
                         default=None)
    prepare.add_argument("-i", "--id", type=str, default=None,
                         help="String giving the ID of the improv run daemon, "
                          "used in specifying directories, paths, "
                          "and other per-worker settings.")
    prepare.add_argument("decision_study", type=str,
                         help="Path to the .yaml file containing the "
                              "specification for the decision-making study.")
    prepare.add_argument("menu", type=str,
                         help="Path to the .yaml file containing menu of "
                              "studies and parameter values to be available to "
                              "the improv run daemon")
    prepare.add_argument("-w","--wrapper", type=str,
                         help="Path to the .yaml file containing wrapper template")
    prepare.add_argument("-b", "--database_url", type=str,
                         help="URL of a database to store study info to.", default="")
    prepare.add_argument("studies", type=str,
                         help="Path to the directory containing the studies "
                              "matching the menu.")
    prepare.add_argument("-Df", "--decision_study_files", default="", type=str,
                         help="Path to a single file or a directory "
                              "containing any other files needed for the "
                              "execution of the decision_study, e.g., "
                              "a Python function called from the "
                              "decision_study.")
    prepare.add_argument("-Sf", "--study_files", default="", type=str,
                         help="Path to any files or directories containing "
                              "additional files required for experimental "
                              "study execution; these could be local scripts, "
                              "Python functions, etc., which are not specified "
                              "already in the study.")
    prepare.set_defaults(func=prepare_push)

    # global options: Implementation matches maestro ... invocations
    # Note that these options must be invoked BEFORE the subcommand, e.g.,
    #
    #  improv -d 1 run <daemon_root> -H global_history.yaml decision_maker.yaml
    #
    # would call improv with debug level 1 (most verbose) and the run
    # sub-command with the various following arguments.
    parser.add_argument("-l", "--logpath", type=str,
                        help="Alternative path to store program log.")
    parser.add_argument("-d", "--debug_lvl", type=int, default=2,
                        help="Level of logging messages [1 (most verbose) - 5 ("
                             "least verbose)].")

    # ### fill_params ###
    fill_params = subparsers.add_parser(
        "fill_params", help="Generate a global parameters block from a maestro study meta/parameters.yaml file."
    )

    fill_params.set_defaults(func=fill_parameters)

    return parser


def setup_logging(args, path, name):
    """
    Set up logging using argument parser inputs; copied from Maestro
    implementation.

    :param args:
    :param path: string, giving logging path. Overridden if args.logpath is
        present.
    :param name: string, giving the name of the eventual logfile
    """
    if args.logpath:
        logpath = args.logpath
    else:
        logpath = make_safe_path(path, "logs")

    loglevel = args.debug_lvl * 10

    formatter = logging.Formatter(IMPROV_LOGGERFORMAT)
    IMPROV_ROOTLOGGER.setLevel(loglevel)

    logname = make_safe_path("{}.log".format(name))
    create_parentdir(os.path.abspath(logpath))
    fh = logging.FileHandler(os.path.join(logpath, logname))
    fh.setLevel(loglevel)
    fh.setFormatter(formatter)
    IMPROV_ROOTLOGGER.addHandler(fh)


def prepare_push(args):
    """Prepare the hierarchical structure of an Improv study.

    Improv is intended to have multiple workers which have access to a common
    file system, all of which are communicating via a common history file.
    prepare_push sets up an individual worker's menus, studies, and
    decision_study.yaml and decision_study_files, where this last contains
    any dependencies of the decision_study.
    :param args
    """

    if args.output_path is not None:
        path = os.path.abspath(args.output_path)
        create_parentdir(path)
    else:
        path = os.getcwd()
    os.chdir(path)

    if args.id:
        name = "improv_prepare_{}".format(args.id)
        setup_logging(args, path, name)

    source_dirs = [["source"],
                   ["source", "studies"],
                   ["source", "decision_study_files"]]
    # TODO: Set using module vars

    for source_dir in source_dirs:
        try:
            os.mkdir(os.path.abspath(os.path.join(os.getcwd(), *source_dir)))
        except FileExistsError:
            IMPROV_ROOTLOGGER.warning(
                "The target {} directory already exists!".format(source_dir[-1])
            )
    # Create directory structure:
    # <decision_maker>
    #   studies
    #   decision_study_files

    # If specified, copy the decision_study.yaml file and menu.yaml file into
    # their places
    study_files = []
    source_dir = os.path.join(os.getcwd(), "source")
    study_files.append(shutil.copy(args.decision_study,
                                   os.path.join(source_dir, "decision_study.yaml")))
    study_files.append(shutil.copy(args.menu,
                                   os.path.join(source_dir, "menu.yaml")))
    if args.wrapper:
        study_files.append(shutil.copy(args.wrapper,
                                   os.path.join(source_dir, "wrapper.yaml")))

    # Copy any other required files into decision_study_files
    if args.decision_study_files:
        study_files += copy_files_from_string_argument(args.decision_study_files,
                                                       os.path.join(source_dir,
                                                                    "decision_study_files")
                                                       )

    # Copy the specified study .yaml files into studies
    study_files += copy_files_from_string_argument(args.studies,
                                                   os.path.join(source_dir,
                                                                "studies")
                                                   )

    # Copy any additional study_files into studies/study_files
    if args.study_files:
        study_files += copy_files_from_string_argument(args.study_files, os.path.join(
            source_dir, "studies", "study_files"))

    # No logging if no id, no db logging if no db url
    if args.database_url and args.id:
        dao = DAO(args.database_url)
        dao.connect()
        # The "agent record" is prepared from the run daemon's info, but is better
        # understood as the agent than as a daemon, as it's associated with further
        # info as time goes on.
        # Agent ids can be hand-specified, and an agent can be run more than once.
        if dao.recs.exist(args.id):
            agent_record = dao.recs.get(args.id)
            add_agent_func = dao.recs.update
        else:
            agent_record = Record(id=args.id, type="agent")
            add_agent_func = dao.recs.insert
        # This is (should be) the initial insert for the agent
        # If one by the id already exists, it IS an error.
        for file in study_files:
            agent_record.add_file(file)
        # Multiple agents can share a menu
        menu_id = ingest_menu_file(args.menu, dao, yaml_lockpath=None, yaml_acquiretime=30)
        add_agent_func(agent_record)
        # Insert menu relationship if it doesn't already exist
        if not list(dao.rels.get(subject_id=args.id,
                                 predicate="uses menu",
                                 object_id=menu_id)):
            dao.rels.insert(subject_id=args.id,
                            predicate="uses menu",
                            object_id=menu_id)
        dao.disconnect()


def create_wrapper_flavors(target_dir, studies):
    """Creates wrappers for inner experimental studies."""
    IMPROV_ROOTLOGGER.info("Experimental studies to make wrappers for: {}".format(studies))
    for study in studies:
        inner_study = yaml_safe_load_with_lock(study)

        wrapper = IMPROV_WRAPPER_STUDY
        wrapper = yaml_safe_load_with_lock(wrapper)
        wrapper['description']['name'] = inner_study['description']['name']
        wrapper['env']['variables']['INNER_STUDY_TYPE'] = study
        path = target_dir + '/' + 'wrapper_{}.yaml'.format(inner_study['description']['name'])
        yaml_safe_dump_with_lock(wrapper, path)

        inner_study['description']['name'] = inner_study['description']['name'] + '_inner'
        yaml_safe_dump_with_lock(inner_study, study)


def prepare_path_dependencies(path_dependencies, target_directory,
                                     path_to_global_history=None):
    """Create a dictionary of path dependencies for injection into a study"""
    path_dependencies = [
        {"name": ki, "path": os.path.abspath(os.path.join(
            target_directory, pi))} for ki, pi in
        path_dependencies.items()]

    if path_to_global_history is not None:
        path_dependencies.append({"name": IMPROV_PREFIX + "HISTORY",
                                  "path": path_to_global_history})

    path_dependencies = {
        "env": {
            "dependencies": {
                "paths": path_dependencies
            }
        }
    }

    return path_dependencies


def prepare_pull(source_directory, target_directory,
                 path_to_global_history=None, database_url=None, wrapper=False):
    """
    Pull the source directory from the master.

    :param source_directory: A directory on the master node, created with
        prepare_push, from which the decision-maker components will be
        copied (decision_maker and its contents; see below).
    :param target_directory: A local directory, into which the following file
        structure will be created:
            workspace
            improv
                inbox
                outbox
                decision_maker
                    decision_study_files  # any dependencies of decision_study
                    studies
                        <*.yaml>  # template studies
                        study_files # any dependencies of experimental studies
                    decision_study.yaml  # Decision-maker study
                    menu.yaml  # menu of studies and parameters.
    :param path_to_global_history: string or None, giving the global history
        .yaml file's location; must be accessible to the worker.
    :param database_url: string or None, giving the global history database's
        url.
    :param wrapper: bool, setup run with wrapper.
    """

    # Verify that the source directory contains decision_study_files (with
    # any contents), studies (with one or more .yaml files),
    # decision_study.yaml (may be absent), and menu.yaml, with NO OTHER
    # directories or files present.
    allowed_files_dir_and_re = {
        "decision_study_files": ["[\w\.]*"],
        "studies": ["[\w\.]*.yaml", "study_files"],
        ".": ["wrapper.yaml", "menu.yaml", "decision_study.yaml", "decision_study_files",
              "studies"]
    }  # TODO: Set using environment variables

    # Verify the contents of the menu against the studies?
    retcode = verify_source_directory(source_directory, allowed_files_dir_and_re)

    if retcode != 0:
        IMPROV_ROOTLOGGER.warning("The source directory does not comply with "
                                  "the specified format!")

    try:
        os.mkdir(target_directory)
    except OSError:
        if os.path.exists(target_directory) and os.listdir(target_directory):
            IMPROV_ROOTLOGGER.info(
                "The target_directory {} still contains "
                "files!".format(target_directory))
    if not os.path.exists(target_directory):
        IMPROV_ROOTLOGGER.error("Unable to create target directory "
                                "{}!".format(target_directory))

    # Create the improv directory structure in the target directory
    # <target_directory>
    #   improv  <- IMPROV_PRIVATE_SPACE
    #       decision_maker  <- created below in the copy of the source_directory
    #       inbox  <- IMPROV_INBOX
    #       outbox <- IMPROV_OUTBOX
    #   workspace  <- IMPROV_MAESTRO_WORKSPACES

    directories = [os.path.join(target_directory, IMPROV_PRIVATE_SPACE),
                   os.path.join(target_directory, IMPROV_INBOX),
                   os.path.join(target_directory, IMPROV_OUTBOX),
                   os.path.join(target_directory, IMPROV_MAESTRO_WORKSPACES),
                   ]
    for d in directories:
        create_parentdir(d)

    # Copy the source directory's contents into the decision_maker directory
    shutil.copytree(source_directory, os.path.join(target_directory,
                                                   IMPROV_DECISION_MAKER_ROOT))

    # ### Modify the decision_study
    # Write the IMPROV_* variables into decision_study.yaml
    # Prepare these values:
    decision_maker_path_deps = {IMPROV_PREFIX + "INBOX": IMPROV_INBOX,
                         IMPROV_PREFIX + "MENU": IMPROV_MENU,
                         IMPROV_PREFIX + "STUDIES": IMPROV_STUDIES,
                         IMPROV_PREFIX + "DECISION_MAKER_ROOT":
                             IMPROV_DECISION_MAKER_ROOT}

    if wrapper:
        wrapper_path_deps = {IMPROV_PREFIX + "INBOX": IMPROV_INBOX,
                            IMPROV_PREFIX + "MENU": IMPROV_MENU,
                            IMPROV_PREFIX + "STUDIES": IMPROV_STUDIES,
                            IMPROV_PREFIX + "DECISION_MAKER_ROOT":
                                IMPROV_DECISION_MAKER_ROOT}

    # ### Modify the decision_study.yaml file to have the correct values of the
    # ### environment variables.
    IMPROV_ROOTLOGGER.info("Preparing path dependencies for the decision-making study.")
    decision_maker_path_deps = prepare_path_dependencies(
        decision_maker_path_deps, target_directory, path_to_global_history)

    # Setup wrappper IMPROV paths for it's tail end decision maker
    if wrapper:
        wrapper_path_deps = prepare_path_dependencies(
            wrapper_path_deps, target_directory, path_to_global_history)

    # add_to_yaml(os.path.abspath(IMPROV_DECISION_STUDY), decision_maker_path_deps)
    # TODO: Add IMPROV_DATABASE with database URL in env: variables:
    #  Others are in env: dependencies: paths:
    #  This will require post-hoc editing. It's safe to always try to fill the
    #  IMPROV_DATABASE value (if non-None), though, as any existing value will
    #  be removed via the first part of modify_study_environment_IMPROV and
    #  if not called later, the value does not matter

    if "variables" not in decision_maker_path_deps["env"]:
        decision_maker_path_deps["env"]["variables"] = {}
    decision_maker_path_deps["env"]["variables"][IMPROV_PREFIX + "DATABASE"] = \
        ""
    if database_url is not None:
        decision_maker_path_deps["env"]["variables"][IMPROV_PREFIX + "DATABASE"] = database_url

    IMPROV_ROOTLOGGER.info("Modifying the decision-making study's env.")
    modify_study_environment_IMPROV(
        os.path.abspath(os.path.join(target_directory, IMPROV_DECISION_STUDY)),
        decision_maker_path_deps["env"])


    if wrapper:
        if "variables" not in wrapper_path_deps["env"]:
            wrapper_path_deps["env"]["variables"] = {}
        wrapper_path_deps["env"]["variables"][IMPROV_PREFIX + "DATABASE"] = \
            ""
        if database_url is not None:
            wrapper_path_deps["env"]["variables"][IMPROV_PREFIX + "DATABASE"] = database_url

        # Modify the wrapper with IMPROV paths for it's tail end decision maker
        modify_study_environment_IMPROV(
            os.path.abspath(os.path.join(target_directory, IMPROV_WRAPPER_STUDY)),
            wrapper_path_deps["env"])

    # ### Modify the individual studies:
    # First, check if this is necessary:
    necessary_study_modification = \
        os.path.exists(os.path.join(target_directory, IMPROV_STUDY_FILE_DIR))
    # Currently, the only reason the studies might need to be modified is if
    # there exist some files in the study_files directory (i.e., supporting
    # files needed by the experimental studies.

    if necessary_study_modification:  # TODO: decide: always modify studies?
        # Create the new path dependencies
        experimental_studies_path_deps = {IMPROV_PREFIX + "STUDY_FILE_DIR":
                                          IMPROV_STUDY_FILE_DIR}

        IMPROV_ROOTLOGGER.info("Pre-preparation path dependencies for "
                               "experimental studies: {}".format(
                                    experimental_studies_path_deps))

        experimental_studies_path_deps = prepare_path_dependencies(
            experimental_studies_path_deps, target_directory)

        IMPROV_ROOTLOGGER.info("Post-preparation path dependencies for "
                               "experimental studies: {}".format(
                                    experimental_studies_path_deps))

        # List the study .yaml files that need modification:
        studies_list = os.listdir(os.path.join(target_directory,
                                               IMPROV_STUDIES))
        IMPROV_ROOTLOGGER.info("The (rough) list of experimental studies to be "
                               "modified is: {}".format(studies_list))
        studies_list = [
            os.path.abspath(os.path.join(target_directory, IMPROV_STUDIES, si))
            for si in studies_list if os.path.splitext(si)[-1] == ".yaml"
        ]

        IMPROV_ROOTLOGGER.info("The list of experimental studies to be "
                               "modified is: {}".format(studies_list))

        # Turn experimental studies into wrappers
        if wrapper:
            create_wrapper_flavors(target_dir=os.path.abspath(os.path.join(target_directory,
                                                                           IMPROV_STUDIES)),
                                   studies=studies_list)

        # Perform the modification
        for study_i in studies_list:
            modify_study_environment_IMPROV(
                study_i, experimental_studies_path_deps["env"]
            )
            IMPROV_ROOTLOGGER.info("Modified {} .".format(study_i))


def run_worker(args):
    """
    Run the local improv daemon, which executes maestro run commands for
    studies in its inbox.

    :param args: a structure derived from argparser with the following fields:
        worker_path: The root path of the worker's "file system"; this is the
            worker's private domain, in which the worker will create folders
            improv (containing all decision-making functions), logs, and
            workspace (in turn containing the folders of the individual maestro
            runs which carry out the selected studies and make the decisions).
        history: str, relative or absolute path to the 'global' history .yaml
            file, shareable among all improv run daemons, including on separate
            allocations which share a common file system. If "", the empty
            string, no such path is defined.
        database_url: str, url of a database to be accessed via a DAO. If "",
            the empty string, no such path is defined.
        sleeptime: integer; the number of seconds to sleep between cycles of
            the improv run daemon.
        maestro_flags: A list or string, containing a set of flags to be
            passed to child maestros, both study-running and decision-making.
            For example, this might include -s 1, i.e., sleep the conductor
            daemon for 1 second at the end of each conductor cycle.
        cancel_lock_path: path to the cancel lock file; if present, the improv
            run daemon will cancel its child maestros and then itself.
        term_lock_path: path to the terminate lock file; if present,
            the improv run daemon will terminate its child maestros and then
            itself.
        wrapper: path to the wrapper study (supplied if running the wrapper workflow)
                 otherwise it is ignored and the regular workflow is followed.
    """

    # ### Input handling ###
    if args.maestro_flags is "":
        args.maestro_flags = None

    if args.history == "":
        args.history = None

    if args.database_url == "":
        args.database_url = None

    # ### Set paths ###
    # Root path:
    worker_root_path = args.worker_path

    # Workspace directory:
    worker_workspace_path = os.path.abspath(
        os.path.join(worker_root_path, "workspace"))

    # Decision-making path dependencies:
    decision_study = os.path.abspath(
        os.path.join(worker_root_path, IMPROV_DECISION_STUDY)
    )

    inbox = os.path.abspath(os.path.join(worker_root_path, IMPROV_INBOX))

    outbox = os.path.abspath(os.path.join(worker_root_path, IMPROV_OUTBOX))

    if args.cancel_lock_path is "":
        args.cancel_lock_path = os.path.join(worker_root_path, ".cancel.lock")
    args.cancel_lock_path = os.path.abspath(args.cancel_lock_path)

    if args.term_lock_path is "":
        args.term_lock_path = os.path.join(worker_root_path, ".term.lock")
    args.term_lock_path = os.path.abspath(args.term_lock_path)

    # for each of the paths above, ensure that the directory exists.
    # for p in [
    #           os.path.split(os.path.abspath(args.cancel_lock_path))[0],
    #           os.path.split(os.path.abspath(args.term_lock_path))[0],
    #           ]:
    #     create_parentdir(p)

    local_history_file = os.path.abspath(os.path.join(
        worker_root_path, IMPROV_PRIVATE_SPACE, ".local_history.yaml"))

    # ### Set up logging ###
    setup_logging(args, path=os.path.abspath(worker_root_path),
                                             name="improv_run")

    # ### For auditing purposes, print the paths here ###
    names = ["root", "workspace", "decision study", "inbox",
             "outbox", "local history", "global history", "database_url"]
    paths = [worker_root_path, worker_workspace_path, decision_study, inbox,
             outbox, local_history_file, args.history, args.database_url]
    for ni, pi in zip(names, paths):
        if pi is not "" and pi is not None:
            IMPROV_ROOTLOGGER.info(
                "The improv run daemon's {} path is: \n{}"
                "\n{}.".format(ni, pi, os.path.abspath(pi)))
        else:
            IMPROV_ROOTLOGGER.info(
                "The improv run daemon's {} path is unset."
                "\n{}.".format(ni, pi))

    # Do the setup, etc. via prepare_pull

    # TODO: Include database_url (IMPROV_DATABASE) in the decision study
    IMPROV_ROOTLOGGER.info("Beginning prepare_pull.")
    prepare_pull(args.source_directory, args.worker_path, args.history, args.database_url, args.wrapper)

    IMPROV_ROOTLOGGER.info("Completed prepare_pull.")
    # If using a DAO, set it up:
    dao = None
    if args.database_url is not None:
        # TODO: ID for this improv daemon --> dao
        IMPROV_ROOTLOGGER.info("Creating initial DAO, connecting "
                               "to url {}.".format(args.database_url))
        try:
            dao = DAO(args.database_url)
        except Error as e:
            IMPROV_ROOTLOGGER.error("Encountered error {}"
                                    " in db creation.".format(e))
    # ### DONE WITH SETUP ###

    # ### Run main loop ###
    running_maestros = {}  # running_maestros carries job information and the
    #  subprocess object. Keyed by request_id.
    pending_logs = {}  # dictionary of history files and any pending items that
    # still need to be logged to them. Keyed by history file path or db url str.

    last_was_decision = False
    while True:
        # Attempt to reconnect the dao if disconnected
        if dao is not None and dao.conn is None:
            IMPROV_ROOTLOGGER.info("Reconnecting DAO to "
                                   "url {}.".format(args.database_url))
            try:
                dao.connect(args.database_url)
            except Error as e:
                IMPROV_ROOTLOGGER.error("Encountered mysql error {}"
                                        " in db connection.".format(e))

        # ### Check for cancellation/termination signals ###
        # Check for external cancellation signal:
        if os.path.exists(args.cancel_lock_path):
            # Cancel the study, as in conductor.py
            cancel_lock = FileLock(args.cancel_lock_path)
            try:
                with cancel_lock.acquire(timeout=10):
                    IMPROV_ROOTLOGGER.info("Cancelling improv run per external "
                                           "signal.")
                    # Send termination signal to the maestro processes
                    # TODO: Implement maestro cancel calls
                    cancel_child_maestros(running_maestros)

                os.remove(args.cancel_lock_path)
                IMPROV_ROOTLOGGER.info(
                    "Improv run cancelled per external signal.")
            except Timeout:
                IMPROV_ROOTLOGGER.error("Failed to acquire cancellation lock.")
                pass

        # Check for study termination as determined by a decision-maker:
        if os.path.exists(args.term_lock_path):
            # Cancel the study:
            termination_lock = FileLock(args.term_lock_path)
            try:
                with termination_lock.acquire(timeout=10):
                    IMPROV_ROOTLOGGER.info(
                        "Terminating improv run per decision-maker signal.")
                    # Send termination signal to the maestro processes.
                    cancel_child_maestros(running_maestros)

                os.remove(args.term_lock_path)
                IMPROV_ROOTLOGGER.info(
                    "Improv run terminated per decision-maker signal.")
                break
            except Timeout:
                IMPROV_ROOTLOGGER.error(
                    "Failed to acquire termination lock.")
                pass

        # ### Check the study progress ###
        # Below: order matters! A study (i.e., a decision study) might submit
        # a new study and then end itself; if we get the list of running
        # studies, and THEN look for requests, we may count both a parent and
        # child, but we won't fail to count both of them, as might occur if
        # the opposite order is used.

        # Check for any finished studies; log their FINISHED/FAILED status
        running_maestros, pending_logs = monitor_and_log(
            running_maestros, history_file=args.history,
            database_access_obj=dao,
            local_history_file=local_history_file,
            pending_log_actions=pending_logs
        )



        # Check for study requests:
        study_files = list_requested_studies(inbox, FILE_TYPE)
        if study_files:
            IMPROV_ROOTLOGGER.info("Found studies {} in the inbox.".format(
                study_files))
            last_was_decision = False
            # This conclusion can be drawn because the decision study is
            # launched separately below, and so is never in the inbox; if it
            # has already submitted a study to the inbox, we clear the
            # last_was_decision flag, and if not, we may terminate below.

        # ### Respond to the state of the study ###
        # Launch any new studies from the inbox
        running_maestros, pending_logs = launch_requested_studies(
            running_maestros, study_files,
            maestro_flags=args.maestro_flags,
            workspace_path=worker_workspace_path, outbox=outbox,
            history_file=args.history,
            database_access_obj=dao,
            local_history_file=local_history_file,
            pending_log_actions=pending_logs,
            requester=args.id,
            srun=args.srun
        )

        IMPROV_ROOTLOGGER.info("After launching, the following maestros are "
                         "running: {}".format(running_maestros))

        # If there are no studies currently running, ask the decision-maker
        # for new studies to be placed into the inbox.
        if not running_maestros and not last_was_decision:
            last_was_decision = True
            IMPROV_ROOTLOGGER.info("No running studies; attempting to make a "
                                    "decision.")
            if decision_study and os.path.exists(decision_study):  # i.e.,
                # if there is a non-empty string and if the file is present
                # here, and thus a study to be invoked:
                # Add and launch a decision-maker study.
                running_maestros, pending_logs = launch_requested_studies(
                    running_maestros, [decision_study],
                    maestro_flags=args.maestro_flags,
                    workspace_path=worker_workspace_path, outbox=None,
                    log_global=False,
                    pending_log_actions=pending_logs
                )
                # The decision_study is responsible for setting the improv
                # termination flag under appropriate conditions OR failing to
                # submit a decision into the inbox.

        if pending_logs:
            IMPROV_ROOTLOGGER.info(
                "Pending logging persists: {}".format(pending_logs)
            )

        # ### Either break or sleep ###
        # If no studies are running, nothing in the inbox (possible because a
        # .yaml could be added to the inbox while studies are being
        # launched) and no decisions are pending, terminate improv run
        if not running_maestros and last_was_decision \
                and not list_requested_studies(inbox, FILE_TYPE):
            IMPROV_ROOTLOGGER.info("No decisions pending, nothing in the "
                                    "inbox, and no studies live; terminating "
                                    "the improv run daemon.")
            # TODO: DAO can't actually be a key: replace / use str() below.
            k_in_pending_logs = list(pending_logs.keys())
            for ki in k_in_pending_logs:
                if pending_logs[ki]:
                    IMPROV_ROOTLOGGER.info(
                        "Performing cleanup logging for pending_logs "
                        "element {}.".format(ki)
                    )
                    try:
                        # TODO: Assess incompatibilities with DAO, presumably unhashable
                        pending_logs = _append_to_history_with_pending_logging(
                            ki, [], [], [], [],
                            lock_acquire_time=600,
                            pending_log_actions=pending_logs
                        )
                    except Exception as e:  # TODO: Narrow this exception clause
                        IMPROV_ROOTLOGGER.warning(
                            "Encountered an error in cleanup logging "
                            "to {}.".format(ki)
                        )
            break

        if dao is not None:
            # End the session while the improv daemon sleeps
            IMPROV_ROOTLOGGER.info("Disconnecting DAO from database.")
            dao.disconnect()
        sleep(args.sleeptime)


def main():
    """
    Execute the invoked improv command.

    return: The return code of the sub-command.
    """

    parser = setup_argparser()
    args = parser.parse_args()

    if "func" not in args:
        args.func = run_worker
        improv_root = os.path.dirname(os.path.dirname(os.path.abspath(
            inspect.getfile(
                inspect.currentframe()))))  # improv.py is in the improvwf
        # directory, one level down from the gitrep root.
        args.worker_path = os.path.join(improv_root, "sample_output",
                                        "worker_1_file_system")
        decision_making_root = os.path.join(improv_root,
                                            "sample_output",
                                            "worker_1_file_system",
                                            "improv")
        global_history_root = os.path.join(improv_root, "sample_output",
                                           "remote_master")
        args.history = os.path.join(global_history_root,
                                    "global_history.yaml")
        args.cancel_lock_path = ".cancel.lock"
        args.term_lock_path = ".term.lock"
        args.sleeptime = 5
        args.debug_lvl = 1
        args.maestro_flags = "-s 1"

        args.source_directory = \
            os.path.abspath(
                os.path.join(
                    improv_root, "sample_output",
                    "remote_master", "worker_1_source"))

    retcode = args.func(args)
    sys.exit(retcode)


if __name__ == "__main__":
    main()
