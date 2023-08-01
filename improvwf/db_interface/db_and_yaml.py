################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

"""
A script for loading improv history to or and dumping from a sina database

"""

import os
import sys
import yaml
from argparse import ArgumentParser, RawTextHelpFormatter

from improvwf.utils import get_history, get_history_db, yaml_safe_dump_with_lock, \
    append_single_record_to_db
from improvwf.db_interface.utils_sina import DAO
from improvwf.improv import _append_to_history_db

# Used to provide updates for long-running operations, will print status after
# every X insertions/deletions/etc.
UPDATE_EVERY = 10000


def fail_running(database_url=None):
    """
    turn all studies with status "running" into failed
    possible issue could arise if you do this while things
    are actually running.

    Psuedo code:
        1. connect to db (DAO)
        2. pull all records (DAO) (seems like this is done with self.recs?)
        3. seach all records for status "RUNNING"
            4. update record as we search
            5. save it in a list
        6. update records (DAO)

    :returns: 0 if successful else 1
    """
    # Verify the paths
    if database_url is None:
        print("DB is None! Exiting 1.")
        return 1

    # Instantiate the dao
    dao = DAO(database_url)
    _do_fail_running(dao)


def _do_fail_running(dao):
    """
    Handle post-dao logic of fail_running.

    Allows for unit testing.
    """
    # get all running requests
    running_reqs = list(dao.recs._find(types="request", data={"status": "RUNNING"}))
    for curr_rec in running_reqs:
        reqid = curr_rec.id
        # get request dictionary using ID
        if "run_descriptors" in curr_rec.data:
            print("Fixing run descriptors in " + str(reqid))
            curr_rec.user_defined["run_descriptors"] = curr_rec.data["run_descriptors"]["value"]
            del(curr_rec.data["run_descriptors"])
            assert(curr_rec.user_defined["run_descriptors"])
        curr_rec.set_data('status', 'FAILED')
        dao.recs.update(curr_rec)

    return 0


def empty_db(database_url=None):
    """
    Delete all records from the database at the specified URL

    :param database_url: str, giving the url to the database
    :return: 0 if successful, 1 otherwise
    """

    # Verify the paths
    if database_url is None:
        print("DB is None! Exiting 1.")
        return 1

    # Instantiate the dao
    dao = DAO(database_url)
    _do_empty_db(dao)


def delete_stubs(database_url=None, field=None, value=None):
    """
    Delete all stubs matching some criteria.
    """

    # Verify the paths
    if database_url is None:
        print("DB is None! Exiting 1.")
        return 1

    if field is None or value is None:
        print("Found incomplete criterion! If you wish to delete all stubs regardless of content, use the args `study_type` and `history_stub`")
        return 1

    criteria_dict = {field: value}
    criteria_dict["study_type"] = "history_stub"

    dao = DAO(database_url)
    match_recs = list(dao.recs.get_given_data(**criteria_dict))
    partner_recs = []
    print("Found {} history stubs to delete! Searching for their reqs/res"
          .format(len(match_recs)))
    for id in match_recs:
        if id.endswith("_result"):
            partner_recs.append(id[:-7])
        else:
            partner_recs.append(id+"_result")
    print("Found {} additional related stubs (requestor or result) to delete!"
          .format(len(partner_recs)))
    print("Performing the deletion now. This may take some time.")

    # Delete rows in chunks (we prefer a partial deletion go through for this function
    # rather than losing the whole thing; stubs can be re-inserted)
    max_chunk_size = 5000
    total_deleted = 0
    deleted_this_chunk = 0
    # Order is important! We want to get the found ones first.
    full_list = partner_recs + match_recs
    for idx, offset in enumerate(full_list[::max_chunk_size]):
        chunk = full_list[idx*max_chunk_size:idx*max_chunk_size+max_chunk_size]
        total_deleted += len(chunk)
        deleted_this_chunk += len(chunk)
        dao.recs.delete(chunk)
        if deleted_this_chunk >= UPDATE_EVERY:
            print("{} total studies deleted".format(total_deleted))
            deleted_this_chunk = 0
    return 0


def _do_empty_db(dao):
    """
    Handle post-dao logic of empty_db.

    Allows for unit testing.
    """
    # Delete rows in chunks (passing 75k ids to a delete call fails)
    max_chunk_size = 5000
    request_ids = list(dao.recs.get_all_of_type(types="request", ids_only=True))
    while(len(request_ids) > 0):
        dao.recs.delete(request_ids[:max_chunk_size])
        request_ids = request_ids[max_chunk_size:]
        print("Deleting requests by blocks of " + str(max_chunk_size) + ". Remaining items = " + str(len(request_ids)))

    result_ids = list(dao.recs.get_all_of_type(types="result", ids_only=True))
    while(len(result_ids) > 0):
        dao.recs.delete(result_ids[:max_chunk_size])
        result_ids = result_ids[max_chunk_size:]
        print("Deleting results by blocks of " + str(max_chunk_size) + ". Remaining items = " + str(len(result_ids)))

    # TODO: Delete relationships? RH indicates that removing all records will
    #  also remove all relationships

    return 0


def dump_db_to_file(database_url=None, history=None, nrandom=None, max_cpu=1, frag_cpu=1):
    """
    Dump the contents of the database to a .yaml file

    :param database_url: str, giving the url to the database
    :param history: str, giving the name of the file
    :param nrandom: int or None, giving the number of random records
        to be retrieved or retrieving all records (default).
    :return: 0 if successful, 1 otherwise
    """

    if database_url is None:
        print("DB is None! Exiting 1.")
        return 1

    if history is None:
        print("File path is None! Exiting 1.")
        return 1

    # confirm that the file path is accessible
    if not os.path.isdir(os.path.split(history)[0]):
        print('Directory containing the history path is not accessible!')

    # Pull the database contents
    history_list = get_history_db(database_url, nrandom=nrandom, max_cpu=max_cpu, frag_cpu=frag_cpu)

    # Write out the .yaml file(s)
    if len(history_list) == 1:
        # instead of a list of history chunks we have a single history
        print("Writing " + history)
        yaml_safe_dump_with_lock(history_list[0], filepath=history)
    else:
        for i,h in enumerate(history_list):
            chunk_path = history[:-5] # chop off ".yaml"
            chunk_path = chunk_path + '_outchunk_{:04d}.yaml'.format(i+1)
            print("Writing " + chunk_path)
            yaml_safe_dump_with_lock(h, filepath=chunk_path)

    return 0


def load_file_to_db(history=None, database_url=None, dballownotempty=False):
    """
    Load history from a file and push it into the database

    :param history: str, giving path to the history .yaml file
    :param database_url: str, giving the url to the database
    :param allow_non_empty_db: bool or str, allow
    :return: 0 if successful, 1 otherwise
    """

    # Verify the paths
    if database_url is None:
        print("DB is None! Exiting 1.")
        return 1

    if history is None:
        print("File path is None! Exiting 1.")
        return 1

    # Instantiate the dao
    dao = DAO(database_url)

    # Check that the database is empty
    db_empty = True  # TODO: get this

    if not db_empty and not dballownotempty:
        print("Only empty databases are allowed without the --dballownotempty"
              " flag! Exiting 1.")
        return 1

    try:
        # Acquire the history
        print("Reading the history file. This may take some time.")
        h = get_history(history)
        print("History ready! Found {} entries".format(len(h["history"])))
        num_added = 0

        # Add to the db
        # TODO: Implement addition code without taboo _append_to_history_db use
        for hki, hvi in h['history'].items():
            # TODO: check for any assignment problems
            if 'result' not in hvi:
                res = None
            else:
                res = hvi['result']
            # TODO: Evaluate impact of Improv logging here.
            # retcode = _append_to_history_db(
            #     dao=dao, request_id_list=[hki],
            #     run_descriptors_list=[
            #         {'study_type': hvi['study_type'],
            #          'study_parameters': hvi['study_parameters']}
            #     ],
            #     status_list=[hvi['status']],
            #     result_list=[res]
            # )
            retcode = 0
            try:
                append_single_record_to_db(
                    dao=dao, request_id=hki, run_descriptors={
                        "study_type": hvi["study_type"],
                        "study_parameters": hvi["study_parameters"]
                    },
                    status=hvi["status"],
                    result=res
                )
                num_added += 1
                if num_added % UPDATE_EVERY == 0:
                    print("{} total studies inserted".format(num_added))
            except Exception as e:
                print("Hit error within append_single_record!")
                print(e)
                # Recapitulate earlier functionality, since _append... returns 1
                # if a failure occurred in the internval version of this block
                retcode = 1

            if retcode != 0:
                # TODO: Choose better exception type
                raise ValueError('Failed to append to database!')
        print("Insertion complete! {} total studies have been added.".format(num_added))

    except Exception as e:
        print('Hit exception in db loader:')
        raise(e)
        return 1

    return 0


def setup_argparser():
    """
    Set up the argument parser

    :return: parser, the fully-constituted argument parser
    """

    parser = ArgumentParser(
        prog="db_loaddump",
        description="The Improv history loader/dumper for sina history databases",
        formatter_class=RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="subparser")

    # ### dump_db_to_file ###
    dump = subparsers.add_parser(
        "dump",
        help="Dump the contents of the database to a specified .yaml file"
    )
    dump.add_argument("-b", "--database_url", type=str,
                         help="URL of a study requests database.",
                      default="")
    dump.add_argument("-H", "--history", type=str,
                     help="The full path to the history file.",
                     default="")
    dump.add_argument("-n", "--nrandom", type=int,
                      help="The number of random records to retrieve.",
                      )
    dump.add_argument("-m", "--max_cpu", type=int, default=1,
                      help="The maximum number of CPUs to use in parallel data retrieval (single file output.)")
    dump.add_argument("-f", "--frag_cpu", type=int, default=1,
                      help="The number of CPUs to write fragmented history parts in parallel (use instead of max_cpu.)")
    dump.set_defaults(func=dump_db_to_file)

    # ### empty_db ###
    empty = subparsers.add_parser(
        "empty",
        help="Remove all records in the database."
    )
    empty.add_argument("-b", "--database_url", type=str,
                         help="URL of a study requests database.",
                         default="")
    empty.set_defaults(func=empty_db)

    # ### fail_running_db ###
    fail = subparsers.add_parser(
        "fail",
        help="Turn all studies with status running to status fail."
    )
    fail.add_argument("-b", "--database_url", type=str,
                         help="URL of a study requests database.",
                         default="")
    fail.set_defaults(func=fail_running)

    # ### delete_stubs ### #
    delete_stubs_subparser = subparsers.add_parser(
        "delete_stubs",
        help="Delete stubs that match some criterion.")
    delete_stubs_subparser.add_argument("-b", "--database_url", type=str,
                                        help="URL of a study requests database.",
                                        default=None)
    delete_stubs_subparser.add_argument("-f", "--field", type=str,
                                        help="The criterion's field, ex: MASTER_ANTIGEN_FASTA_HASH_2",
                                        default=None)
    delete_stubs_subparser.add_argument("-v", "--value", type=str,
                                        help="The criterion's value, ex: f8213fc8e79e428c9171a2399ab10f40",
                                        default=None)
    delete_stubs_subparser.set_defaults(func=delete_stubs)

    # ### load_file_to_db ###
    load = subparsers.add_parser(
        "load",
        help="Load a specified .yaml file's contents into the history"
    )
    load.add_argument("-b", "--database_url", type=str,
                         help="URL of a study requests database.",
                         default="")
    load.add_argument("-H", "--history", type=str,
                     help="The full path to the history file.",
                     default="")

    group = load.add_mutually_exclusive_group()
    group.add_argument('--dballownotempty', action='store_true', dest='dballownotempty')
    group.add_argument('--dbmustbeempty', action='store_false', dest='dballownotempty')
    load.set_defaults(func=load_file_to_db, dballownotempty=False)

    return parser


if __name__ == "__main__":
    parser = setup_argparser()
    args = parser.parse_args()

    # for ki, vi in vars(args).items():
    #     print((ki, vi))
    #     print(isinstance(vi, int))
    retcode = args.func(
        **{ki: vi for ki, vi in vars(args).items() if ki not in ['func', 'subparser']}
    )

    sys.exit(retcode)
