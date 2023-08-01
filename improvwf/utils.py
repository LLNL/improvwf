################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

"""A utility module for working with the improvwf package."""
import yaml
import os
import shutil
import logging
from itertools import product
import copy
import random
import re
from subprocess import Popen
from filelock import FileLock, Timeout
import multiprocessing as mp
import time
import hashlib
import uuid
from mysql.connector.errors import IntegrityError, Error

from sina.model import Record
from improvwf.db_interface.utils_sina import DAO, AgentIdForm

try:
    from subprocess import DEVNULL
except ImportError:
    DEVNULL = None

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

try:
    from yaml import CSafeDumper as SafeDumper
except ImportError:
    from yaml import SafeDumper


LOGGER = logging.getLogger(__name__)
IMPROV_GLOBAL_HISTORY_LOCK_NAME = ".history.lock"  # TODO: Set at pkg level


def get_Popen_DEVNULL(cmdstring, shell=True):
    """
    Compatibility function for Python 2.7/3.x calls to Popen with DEVNULL

    :param cmdstring: string giving a command to be passed to Popen
    :param shell: boolean giving the shell value to be passed to Popen
    :return: a Popen object with appropriate piping to /dev/null
    """
    if DEVNULL is None:
        with open(os.devnull, 'w') as FNULL:
            return Popen(cmdstring, shell=shell, stdout=FNULL, stderr=FNULL)
    else:
        return Popen(cmdstring, shell=shell, stdout=DEVNULL, stderr=DEVNULL)


def read_run_descriptors_full_study(study_path, file_type=None):
    """
    Extract the standardized run_descriptors from a study

    :param study_path: One of a string giving a path to a study .yaml file or a
        dictionary specifying a maestro study.
    :param file_type: String specifying the type of file to be examined.
        Defaults to .yaml.
    :return: run_descriptors (dictionary) and request_id. The
        run_descriptors should uniquely and fully determine the study relative
        to a parameter-less template study of the study_type. The request ID
        is the key used to distinguish the history entry from others.
    """
    if file_type is None:
        file_type = ".yaml"

    request_id = ""
    run_descriptors = {}
    study = {}
    if isinstance(study_path, str) and os.path.isfile(study_path):
        if os.path.splitext(study_path)[-1].lower() == file_type.lower():
            study = yaml_safe_load_with_lock(study_path)
        else:
            # TODO: Log this failure
            return run_descriptors, request_id
    elif not isinstance(study_path, dict):
        # TODO: Log this failure
        return run_descriptors, request_id

    # Read the run_descriptors from the standard location
    request_id = study["description"]["name"]
    run_descriptors = study["description"]["run_descriptors"]
    return run_descriptors, request_id

    pass


def read_run_descriptors_history(study):
    """
    From a history or menu version of a study, extract the run descriptors.
    :param study: a dictionary describing the study; contains three
        improv-standard keys, request_id, status, and result (this last is
        absent until the study completes successfully.
    :return: the components of the study which are NOT the improv-standard
        keys. Presently standardized to have study_parameters and study_type
        fields.
    """
    run_descriptors = {}
    for ki in study:
        if ki not in ["request_id", "status", "result"]:
            run_descriptors[ki] = study[ki]  # This is a catch-all choice;
            # the three keys above are used by improv to log the study's
            # history, but anything else is just "cargo" to improv.
    return run_descriptors


def remove_from_menu(expanded_menu, list_of_history_studies):
    """
    Remove previously-executed studies from expanded, discrete-valued menu
    :argument expanded_menu: The dictionary containing the set of all allowed
        studies (for this worker) in its "studies" field.
    :argument list_of_history_studies: list containing previously executed
        studies, which will be removed from expanded_menu.
    :return expanded_menu: the input menu, with the previously-executed
        studies removed.
    """
    for si in list_of_history_studies:
        # run_parameters = {ki: si[ki] for ki in si
        #                   if ki not in ["request_id", "status", "result"]}
        run_descriptors = read_run_descriptors_history(si)
        expanded_menu_keep = [True for mi in expanded_menu["studies"]]
        for j, sj in enumerate(expanded_menu["studies"]):
            if run_descriptors == sj:
                expanded_menu_keep[j] = False
        expanded_menu["studies"] = [emi for emi, ki in
                         zip(expanded_menu["studies"], expanded_menu_keep) if ki]
    return expanded_menu


def remove_from_menu_db(db_url, query_data_for_studies, expanded_menu, quota_not_run=None, agent_id=None):
    """
    Remove previously-executed studies from expanded, discrete-valued menu
    :param db_url: url to database or DB file
    :param quota_not_run: int, specifying how many studies should be found as not run
        before terminating early, returning [bool,... bool, None, ... None]. This
        behavior is useful if we send in a ranked list and want (downstream) only
        the first quota_not_run studies not yet executed.
    :param query_data_for_studies: list of (dict of keys and values to match
        in 'study_parameters') corresponding to studies in expanded_menu
    :return expanded_menu: the input menu, with the previously-executed
        studies removed.
    """
    # drop existing records
    expanded_menu,  _ = cached_remove_from_menu_db(db_url, query_data_for_studies, expanded_menu, quota_not_run=quota_not_run, agent_id=agent_id)
    return expanded_menu

def cached_remove_from_menu_db(db_url, query_data_for_studies, expanded_menu, quota_not_run=None, agent_id=None):
    """
    Remove previously-executed studies from expanded, discrete-valued menu
    :param db_url: url to database or DB file
    :param query_data_for_studies: list of (dict of keys and values to match
        in 'study_parameters') corresponding to studies in expanded_menu
    :param quota_not_run: int, specifying how many studies should be found as not run
        before terminating early, returning [bool,... bool, None, ... None]. This
        behavior is useful if we send in a ranked list and want (downstream) only
        the first quota_not_run studies not yet executed.
    :return expanded_menu: the input menu, with the previously-executed
        studies removed.
        study_exists: list of strings indicating whether the study exists or if it is 'novel'
    """
    study_exists =  check_menu_db(db_url, query_data_for_studies, expanded_menu, quota_not_run=quota_not_run, agent_id=agent_id)
    # Remove studies that have been run
    expanded_menu["studies"] = [emi for emi, ki in
                                zip(expanded_menu["studies"], study_exists)
                                if (ki is not None and not ki)
                               ]
    return expanded_menu, study_exists

def check_menu_db(db_url, query_data_for_studies, expanded_menu, quota_not_run=None, agent_id=None):
    """
    check for previously-executed studies from expanded, discrete-valued menu
    :param db_url: url to database or DB file
    :param query_data_for_studies: list of (dict of keys and values to match
        in 'study_parameters') corresponding to studies in expanded_menu
    :param quota_not_run: int, specifying how many studies should be found as not run
        before terminating early, returning [bool,... bool, None, ... None]. This
        behavior is useful if we send in a ranked list and want (downstream) only
        the first quota_not_run studies not yet executed.
    :return study_exists: list of strings indicating whether the study exists or if it is 'novel'
    """
    # Convert expanded menu to SINA records
    study_recs = []

    # query_data_for_studies indicies correpond to studies in expanded_menu
    for i, study_query_data in enumerate(query_data_for_studies):
        #id and type are ignored currently
        rec = Record(id=("proposed_id_" + str(i)), type="request", data=study_query_data)
        study_recs.append(rec)

    study_exists = studies_have_been_run(db_url, study_recs, quota_not_run=quota_not_run, agent_id=agent_id)

    return study_exists


def expand_menu_study_params(menu):
    """
    Expand a menu into the set of all allowed combinations of
    parameters (products)

    :param menu: dictionary, giving allowed studies in its "studies" field;
    these are listed, giving the allowed values for each parameter
    separately. To select a combination of parameters, the product of these
    sets is enumerated.
    :return: expanded_menu: a dictionary of expanded studies, where the
    "studies" field contains one entry per allowed combination of parameters
    """
    menu_out = {k: menu[k] for k in menu if k != "studies"}
    menu_out["studies"] = []
    for i, si in enumerate(menu["studies"]):
        if "study_parameters" in si:
            parameter_lists = [(k, si["study_parameters"][k]["values"]) for k in
                               si["study_parameters"]]
            for ci in product(*[pi[1] for pi in parameter_lists]):
                menu_out["studies"].append({k: si[k] for k in si if k !=
                                            "study_parameters"})
                menu_out["studies"][-1]["study_parameters"] = \
                    {ki: {kj: si["study_parameters"][ki][kj] for kj in
                          si["study_parameters"][ki] if kj != "values"}
                     for ki in si["study_parameters"]}
                for ki, vi in zip([pi[0] for pi in parameter_lists], ci):
                    menu_out["studies"][-1]["study_parameters"][ki]["values"] \
                        = [vi]
        else:
            menu_out["studies"].append(si)  # No parameters to expand
    return menu_out


def _get_record_non_result_values(dao, rid):
    rec = dao[rid]
    run_descriptors = rec.data.get("run_descriptors")
    if run_descriptors is None:
        run_descriptors = rec.user_defined["run_descriptors"]
    else:
        run_descriptors = run_descriptors["value"]
    d = {
        "request_id": rec.id,
        "study_type": run_descriptors["study_type"],
        "status": dao.get_value_from_record(rec, "status"),
        "study_parameters": run_descriptors["study_parameters"]
    }
    return d


def _get_record_result_values(dao, result_id):
    r = dao.recs.get(result_id)
    return {ki: dao.get_value_from_record(r, ki) for ki in r.raw["data"].keys()}


def _get_complete_record(dao, rid):
    h_out = _get_record_non_result_values(dao, rid)
    try:
        # TODO: re-implement with dao.rels
        result_id = rid + "_result"

        h_out["result"] = _get_record_result_values(dao, result_id)
    except ValueError as e:
        pass

    return h_out


def _get_hdict(dao=None, db_url=None, recid_iter=None):
    """
    Create a dictionary from the incoming iterable of record ids

    :param dao:
    :param db_url:
    :param recid_iter: iterable
    :return: dictionary, keyed by the request_ids in recid_iter, of complete
        records (filled by _get_complete_record)
    """

    # Return empty dictionary of recid_iter is not supplied or is None
    if recid_iter is None:
        return {}

    # Check which of db_url or dao is supplied
    if dao is None and db_url is None:
        raise ValueError("One of dao or url must be given")

    if dao is not None and db_url is not None:
        raise ValueError("Cannot accept both dao and url!")

    # If we have the url but not an actual DAO, create a private one
    private_dao = False
    if dao is None:
        private_dao = True
        dao = DAO(db_url=db_url)

    hdict = {}
    for rid_i in recid_iter:
        hdict[rid_i] = _get_complete_record(dao, rid_i)

    # TODO: Evaluate how to parallelize this; below fails because dao
    # cannot be pickled
    # with mp.Pool(mp.cpu_count()) as p:
    #     history["history"] = {rid_i: p.apply(_get_complete_record, args=(dao, rid_i)) for rid_i in recids}

    # If the dao was private, release the connection to the DB.
    if private_dao:
        dao.disconnect()

    return hdict


def append_single_record_to_db(dao=None, logger=None,
                               request_id=None, status=None,
                               run_descriptors=None, result=None,
                               requester=None):
    """
    Append a single record to the specified DAO

    :param dao: sina/improv DAO object
    :param logger: a logging Logger object, or None (no logging)
    :param request_id: str, giving the request's ID, keying into the database
    :param status: improv status code, either "QUEUED", "RUNNING", "FINISHED",
        or "FAILED"
    :param run_descriptors: dict, contining the run_descriptors for this study
    :param result: either None (for studies in progress) or a dictionary for a
        completed study.
    :param requester: str, the id of the daemon that put in the request. Used
                      for logging relationships.
    """

    # Send logging to a dummy logger if none supplied
    if logger is None:
        logger = logging.getLogger('null')
        logger.addHandler(logging.NullHandler())

    logger.debug(
        "Appending request_id {} to DAO {}.".format(
            request_id, dao)
    )

    # TODO: Canonize rdata keys in some fashion, such that we have
    #  "fixed schema" for request entities
    rdata = {"status": status,
             "run_descriptors": run_descriptors}

    # TODO: Fault tolerance here for the case that the first
    #  succeeds but the second does not

    # TODO: Use .exists method on haluska2/collect_data_from_improv_prepare
    # if dao.recs.exists(request_id):
    try:
        logger.debug(
            'Attempting to log request {} to DAO {}.'.format(request_id, dao))
        _ = dao.__getitem__(request_id)
        if _ is None:
            logger.debug(
                'Obtained None-valued result for requestid {}.'.format(
                    request_id))
        else:
            logger.debug(
                'Obtained value {} for request_id {}'.format(_, request_id))
        dao.update_request(request_id, req_data=rdata)
        logger.debug(
            'Successfully updated record {}.'.format(request_id))
        if result is not None:
            logger.debug(
                'Setting the result for request {}.'.format(request_id))
            dao.set_result(request_id, result)
            logger.debug(
                'Successfully set the result for request {} to value {}.'.format(
                    request_id, result))

            # TODO: Evaluate whether update method necessary
    # else:
    except (ValueError, IntegrityError) as e0:
        logger.debug(
            'Excepting error {} and attempting to set_request.'.format(e0))
        dao.set_request(request_id, req_data=rdata)
        # It'll error if the record hasn't already been inserted, so this is our
        # first insertion and where we know to insert a relationship.
        # That said, relationships are only inserted if there's a requester specified
        if requester:
            logger.debug("Creating relationship between requester {} and requested {}".format(requester, request_id))
            dao.rels.insert(subject_id=requester, predicate="requests", object_id=request_id)
        else:
            logger.debug("No requester specified for {}, no relationship recorded".format(request_id))
        if result is not None:
            dao.set_result(request_id, result)


def update_pareto(dao, antigen_sequences):
    """
    Update the set of pareto records within the db.

    :param antigen_sequences: A list of sequences representing the pareto set.
    """
    sequence_datum = "ANTIGEN_SEQUENCE"

    # First, we get the IDs of existing pareto records, so we can delete any removed ones
    existing_pareto_recs = list(dao.recs.get_all_of_type("pareto_stub", ids_only=True))

    # Now we get the sequences represented there...
    existing_pareto_map = dao.recs.get_data_for_records([sequence_datum], existing_pareto_recs)
    existing_pareto_set = set(x[sequence_datum]["value"] for x in existing_pareto_map.values())

    # ...and figure out what new sequences need to be added
    new_pareto_set = set(antigen_sequences)
    pareto_to_add = new_pareto_set - existing_pareto_set

    # We create stubs for these
    for pareto_sequence in pareto_to_add:
        # Existing records aren't used as templates because there's no guarantee a
        # pareto sequence exists in the db (so we'd have a weird mixture of formats)
        new_rec = Record(str(uuid.uuid4())[:6]+"_pareto", "pareto_stub")
        new_rec.add_data("status", "FINISHED")
        new_rec.add_data("study_type", "pareto_stub")
        new_rec.add_data(sequence_datum, pareto_sequence)
        new_rec.add_data(sequence_datum+"_HASH",
                         str(hashlib.md5(pareto_sequence.encode('utf-8')).digest()))
        new_rec.add_data("timestamp", time.time())
        dao.recs.insert(new_rec)

    # And then delete any sequences no longer in the set
    pareto_to_remove = existing_pareto_set - new_pareto_set
    sequence_to_pareto_id_map = {y[sequence_datum]["value"]: x
                                 for x, y in existing_pareto_map.items()}
    dao.recs.delete(sequence_to_pareto_id_map[x] for x in pareto_to_remove)


def get_pareto(dao):
    """Fetch back a list of sequences found in the pareto set."""
    existing_pareto_recs = list(dao.recs.get_all_of_type("pareto_stub", ids_only=True))
    return [x["ANTIGEN_SEQUENCE"]["value"] for x in dao.recs.get_data_for_records(
                                                        ["ANTIGEN_SEQUENCE"],
                                                        existing_pareto_recs).values()]


def get_history_db(db_url, nrandom=None, study_type=None, distance_datum=None, distance_val=None, max_distance=None, max_cpu=1, frag_cpu=1, agent_id=None, agent_id_form=AgentIdForm.IS, structure_hash=None):
    """
    Using a database access object, load the history from the specified DB.

    :param db_url: Database access object, providing a sina interface to the
        database
    :param nrandom: int or None, giving the number of random "request" type
        records to retrieve. Can be used with <dist>; works as ceil, not floor.
    :param distance_datum: str or None. The name of the datum to use in calculating string distance.
    :param distance_val: str or None. The value to calculate distance from.
    :param max_distance: int or None. The maximum distance (inclusive) allowed from <distance_val>.
    :param max_cpu: int, provides a cap on the maximum number of cpus to use.
    :param agent_id: An optional agent_id. If specified, only requests requested by that
                     agent are returned.
    :param structure_hash: An optional structure_hash. If specified, only requests with that
                           STRUCTURE_HASH are returned.
    :return:  List of histories (len == 1 if frag_cpu == 1), formatted as Improv standard.
    """
    # TODO: Debug
    dao = DAO(db_url)
    return get_history_db_from_dao(dao, nrandom, study_type, distance_datum,
                                   distance_val, max_distance, max_cpu, frag_cpu=frag_cpu,
                                   db_url=db_url, agent_id=agent_id, agent_id_form=agent_id_form,
                                   structure_hash=structure_hash)


def get_history_db_from_dao(dao, nrandom=None, study_type=None, distance_datum=None,
                            distance_val=None, max_distance=None, max_cpu=1, frag_cpu=1,
                            db_url=None, agent_id=None, agent_id_form=AgentIdForm.IS,
                            structure_hash=None):
    """
    Use a preexisting Sina dao (rather than a URL) to load history.

    This is used for testing, as well as reusing DAOs (especially in-memory ones).
    Depending on your use case, you will likely prefer get_history_db. Params are the
    same, except for:

    :param dao: a preexisting Sina dao.
    :param agent_id: An optional agent_id. If specified, only requests requested by that
                     agent are returned.
    :param agent_id_form: How to match on the agent_id. By default, the function looks for
                          an exact match. See AgentIdForm for other options.
    :param structure_hash: An optional structure_hash. If specified, only requests with that
                           STRUCTURE_HASH are returned.
    """
    max_cpu = int(max(max_cpu, 1))
    frag_cpu = int(max(frag_cpu, 1))
    if frag_cpu > 1:
        max_cpu = frag_cpu
        print('Using at most {} cpus (fragmention is ON)'.format(max_cpu))
    else:
        print('Using at most {} cpus'.format(max_cpu))

    id_pool = None

    if distance_datum is not None:
        id_pool = dao.get_studies_by_string_distance(distance_datum, distance_val, max_distance)
        # Only the results contain the antigen sequence (request has only fasta hash), so
        # our id_pool ids will all end in "_result". We truncate that to create the
        # request id. May create nonsense IDs if any other types have the
        # distance_datum added, but those are filtered out below.
        id_pool = [x[:-7] for x in id_pool]  # len("_result") == 7

    kwarg = {}
    if study_type is not None:
        # We'll unpack so the kwarg name isn't overwritten by the arg
        kwarg["study_type"] = study_type
    if structure_hash is not None:
        kwarg["STRUCTURE_HASH"] = structure_hash
    if any(x is not None for x in [study_type, structure_hash]):
        if id_pool is not None:
            id_pool = set(id_pool).intersection(dao.recs.data_query(**kwarg))
        else:
            # As with distance_datum, we want to filter out the results
            id_pool = (x for x in dao.recs.data_query(**kwarg))

    # Now that we've performed any necessary narrowing of the pool, keep only
    # the records that are requests
    if id_pool is not None:
        id_pool = set(id_pool).intersection(dao.recs.get_all_of_type("request", ids_only=True))

    # And of those, only requests requested by a given agent (if specified)
    if agent_id is not None:
        if agent_id_form is AgentIdForm.IS:
            agent_pool = (x.object_id for x in
                          dao.rels.get(subject_id=agent_id, predicate="requests"))
        elif agent_id_form in [AgentIdForm.STARTS_WITH, AgentIdForm.ENDS_WITH]:
            agent_pool = dao.get_request_id_by_partial_match(agent_id, agent_id_form)
        else:
            raise ValueError("agent_id_form {} is not yet supported".format(agent_id_form))
        if id_pool is not None:
            id_pool = set(id_pool).intersection(agent_pool)
        else:
            id_pool = agent_pool

    # nrandom is special; it pulls from whatever pool we've already decided upon,
    # unlike the other args, which are constraints on the pool itself.
    if nrandom is not None:
        nrandom = int(nrandom)
        if id_pool is None:
            id_pool = dao.recs.get_random(count=nrandom, type="request", ids_only=True)
        else:
            if nrandom < len(id_pool):
                id_pool = random.sample(id_pool, nrandom)

    # Of course, it's always possible that we have no "filters" at all!
    if id_pool is None:
        id_pool = dao.recs.get_all_of_type("request", ids_only=True)

    # rec_ids = dao.recs.get_all_of_type(type="request", ids_only=True)
    # rids_keep = [ridi in rec_ids if ...]
    # dao.  __getitem__(ridi)
    # Note: RH suggests adding timestamp as part of the record; if formated
    # as unix standard, could be pulled using the max command in a query
    # like the above; it's a scalar that can be ranked and selected
    # Note: RH also notes that specifying "type" slows things down by 3x; this
    # is a relatively small absolute impact, though.

    # We have now pared our pool of ids down to the desired records.
    # If there are none, quit fast.

    # Safety-convert from generator and hand off to the rest of the logic
    recids = list(id_pool)

    # TODO: reformat the data field into the appropriate entry constituents

    # print(recs)

    # rels = dao.rels.get_all_of_type(type="result")
    # print(rels)

    history_list = []

    # # Original method
    if max_cpu == 1:
        history = {
            "description": {
                },
            "history": {}
        }
        if db_url:
            history["description"]["description"] = ("History database contents from {}."
                                                     .format(db_url))
            history["description"]["name"] = "history_{}".format(db_url)
        for rid_i in recids:
            history["history"][rid_i] = _get_complete_record(dao, rid_i)

        history_list.append(history)

        # Methods using _get_hdict:
        # # With the one DAO already established:
        # # This method essentially wraps _get_complete_record
        # history["history"] = _get_hdict(dao=dao, recid_iter=recids)

    else:
        # Alternatively, using a parallel implementation:
        nworkers = min(max_cpu, max(mp.cpu_count() - 4, 1))
        # slice up recids:
        chunklen = float(len(recids)) / float(nworkers)  # This is a float
        rec_chunks = [[] for i in range(nworkers)]
        for i, recid_i in enumerate(recids):
            idx = int(i / chunklen)  # Round the float down to the nearest integer
            rec_chunks[idx].append(recid_i)  # Append rec_id to appropriate chunk

        # Retrieve the history chunks in parallel
        with mp.Pool(nworkers) as pool:
            result_objs = [
                pool.apply_async(
                    _get_hdict,
                    kwds={
                        'db_url': db_url,
                        'recid_iter': rec_chunk_i
                       }
                )
                for rec_chunk_i in rec_chunks
            ]

            list_of_hdicts = [ri.get() for ri in result_objs]
        if frag_cpu > 1:
            for i, hdi in enumerate(list_of_hdicts):
                history = {
                    "description": {
                        },
                    "history": {}
                }
                if db_url:
                    history["description"]["description"] = "History database contents from " + str(db_url) + " fragment " + str(i+1) + " of " + str(len(list_of_hdicts))
                    history["description"]["name"] = "history_" + str(db_url) + "_" + str(i+1)
                for kij, vij in hdi.items():
                    history["history"][kij] = vij
                history_list.append(history)
        else:
            history = {
                "description": {
                    },
                "history": {}
            }
            if db_url:
                history["description"]["description"] = "History database contents from " + str(db_url)
                history["description"]["name"] = "history_" + str(db_url)
            for i, hdi in enumerate(list_of_hdicts):
                for kij, vij in hdi.items():
                    history["history"][kij] = vij
            history_list.append(history)


    # Release the database connection
    dao.disconnect()

    return history_list

def studies_have_been_run(db_url, proposed_studies, quota_not_run=None, agent_id=None):
    """
    Using a database access object, check if studies have been run.

    :param db_url: Database access object, providing a sina interface to the
        database
    :param proposed_studies: list of Sina Records, records representing the studies we
        want to check. A subset of its data will be used to perform the check.
        id and type are ignored.
    :param quota_not_run: int, specifying how many studies should be found as not run
        before terminating early, returning [bool,... bool, None, ... None]. This
        behavior is useful if we send in a ranked list and want (downstream) only
        the first quota_not_run studies not yet executed.
    :return studies_exist, list of bool or None whether studies exist in database
        (bool) or was not checked (None).
    """

    print('Accessing database at {}'.format(db_url))

    dao = DAO(db_url)
    studies_exist = []
    studies_absent_sum = 0

    if quota_not_run is None:
        studies_exist = dao.study_has_been_run(proposed_studies, agent_id=agent_id)
    else:
        batch = int(quota_not_run * 1.5) # try to get it on one call.
        num_still_needed = quota_not_run
        while(len(proposed_studies) > 0 and num_still_needed > 0):
            check_studies = proposed_studies[:batch]
            proposed_studies = proposed_studies[batch:]
            returned_run = dao.study_has_been_run(check_studies, agent_id=agent_id, quit_at=num_still_needed)
            num_still_needed -= returned_run.count(False)
            studies_exist += returned_run

    # Release the database connection
    dao.disconnect()

    return studies_exist


def get_history(history_path):
    """Load the history from the specified yaml file."""
    lock_path = os.path.join(os.path.split(history_path)[0],
                         IMPROV_GLOBAL_HISTORY_LOCK_NAME)
    # history = {}
    # try:
    #     try:
    #         lock = FileLock(lock_path)
    #         with lock.acquire(timeout=30):
    #             with open(history_path, 'r') as h:
    #                 history = yaml.safe_load(h)
    #     except Timeout as err:
    #         # TODO: Log timeout
    #         raise(err)
    # except FileNotFoundError as err:
    #     # TODO: Log failure to load history
    #     raise(err)
    history = yaml_safe_load_with_lock(
        filepath=history_path,
        lockpath=lock_path,
        acquiretime=30
    )

    return history


def get_menu(menu_path):
    """Load the menu from the specified yaml file."""
    menu = {}
    menu = yaml_safe_load_with_lock(menu_path)
    return menu


def get_studies(studies_path):
    """
    Load the .yaml studies located at studies_path

    For each, load the entire yaml file and add it to the studies dictionary,
    keyed by the description:name: value.

    :param studies_path: string giving the path to a directory containing
        .yaml template study files.
    :returns studies: a dictionary of studies, keyed by description:name (
        i.e., study_type).
    """

    studies = {}
    for sfile in os.listdir(studies_path):
        if os.path.splitext(sfile)[-1] == ".yaml":
            # TODO: Log which file we are attempting to load studies from
            _ = yaml_safe_load_with_lock(os.path.join(studies_path, sfile))
            studies[_["description"]["name"]] = _  # This is the study type
    return studies


def write_selected_study(selected_params, studies, request_id, output_path):
    """
    Merge selected parameters with the template and write to a .yaml

    :param selected_params: the dictionary describing the selected study.
    :param studies: dictionary containing the template studies, loaded from
        their individual .yaml files.
    :param request_id: string giving the name of the request
    :param output_path: directory into which the merged study will be written.
    """
    if selected_params["study_type"] not in studies:
        raise KeyError("The selected study type, {} does not appear "
                       "in the set of studies loaded from the study "
                       "directory.".format(selected_params["study_type"]))
    # Copy study over
    study_out = copy.deepcopy(studies[selected_params["study_type"]])
    # print("\nAfter addition of template study components:")
    # print(study_out)

    study_out["description"]["name"] = request_id
    study_out["description"]["run_descriptors"] = selected_params
    # print("\nAfter addition of name and run_descriptors:")
    # print(study_out)

    # Merge any fields contained in si besides study_type with the originals
    for ki in selected_params.keys():
        if ki == "study_type":
            continue
        elif ki == "study_parameters":
            study_out["global.parameters"] = selected_params[ki]
        else:
            raise KeyError("Unexpected key {} in selected study; menu "
                           "should not contain this key, as no handling is "
                           "implemented!".format(ki))

    # Note: in the above, the description:run_descriptors:study_parameters field
    #  and the global.parameters field are the same; this means that the written
    # .yaml file will contain an internal reference, rather than writing
    # the exact same data twice.

    # Write the yaml output to the registry file.
    yaml_safe_dump_with_lock(
        study_out, os.path.join(output_path, request_id + ".yaml"))


def verify_source_directory(source_directory, allowed_files):
    """
    Verify that the source directory contains EXACTLY the files specified by
    allowed_files
    :param source_directory: A path to the file structure to be verified.
    :param allowed_files: A dictionary, keyed by file paths, and with items
    giving a list of allowed files to reside in that directory.
    :return: 0 if passes, 1 if fails
    """

    # For each entry in the allowed_files:
    for di, alf_i in allowed_files.items():
        # List the files in the directory
        all_files = os.listdir(os.path.join(source_directory, di))
        all_files_keep = [True for fi in all_files]
        for alf_ii in alf_i:
            for idxi, fi in enumerate(all_files):
                if re.search(alf_ii, fi):
                    all_files_keep[idxi] = False
        all_files = [fi for fi, ki in zip(all_files, all_files_keep) if ki]
        # If any non-compliant files are found, return 1
        if all_files:
            LOGGER.warning("Found noncompliant files {} in directory {} and "
                            "with pattern {}!".format(all_files, di, alf_i))
            return 1

    # If all checks pass, return 0
    return 0


def copy_files_from_string_argument(string_argument, target_directory):
    """
    Copy files specified by a command line input to a target directory
    :param string_argument: String, specifying perhaps multiple files or
    directories. The CONTENTS of directories are copied to the target
    directory, in the order of the arguments given.
    :param target_directory: string, giving the destination
    :returns: A list of all files created
    """
    if not os.path.exists(target_directory):
        os.mkdir(target_directory)

    files_or_dirs = string_argument.split()
    created_files = []

    for fodi in files_or_dirs:
        if os.path.isfile(fodi):  # Copy an individual file
            created_files.append(shutil.copy(fodi, target_directory))
        elif os.path.isdir(fodi):  # Copy the CONTENTS of a directory
            for fi in os.listdir(fodi):
                if os.path.isfile(os.path.join(fodi, fi)):
                    LOGGER.debug("Copying file {} to target_directory.".format(fi))
                    created_files.append(
                        shutil.copy(os.path.join(fodi, fi), target_directory))
                else:
                    LOGGER.debug("Copying directory {} to first-level "
                                 "target.".format(fi))
                    copied_path = os.path.join(fodi, fi)
                    shutil.copytree(copied_path,
                                    os.path.join(target_directory, fi)
                                    )
                    copied = [os.path.join(target_directory, f) for f in os.listdir(copied_path)]
                    created_files += copied
    return created_files


def yaml_safe_load_with_lock(filepath, lockpath=None, acquiretime=30):
    """Safely load the contents of a yaml file"""

    if lockpath is None:
        lockpath = filepath + ".lock"
    lock = FileLock(lockpath)
    _ = {}
    try:
        with lock.acquire(timeout=acquiretime):
            try:
                with open(filepath, "r") as f:
                    _ = yaml.load(f, Loader=SafeLoader)
            except FileNotFoundError as err:
                raise(err)
    except Timeout as err:
        raise(err)
    return _


def yaml_safe_dump_with_lock(yaml_contents, filepath, lockpath=None, acquiretime=30):
    """Safely write to a yaml file while preventing access"""
    if lockpath is None:
        lockpath = filepath + ".lock"
    lock = FileLock(lockpath)
    try:
        with lock.acquire(timeout=acquiretime):
            with open(filepath, "w") as f:
                yaml.dump(yaml_contents, f, Dumper=SafeDumper)
    except Timeout as err:
        raise(err)


def os_rename_with_lock(src, dest):
    """Safely move a file while preventing reads or writes to src."""
    lockpath_src = src + ".lock"
    lock_src = FileLock(lockpath_src)

    # dest locking?
    # TODO: Evaluate destination locking
    try:
        with lock_src.acquire(timeout=30):
            os.rename(src, dest)
    except Timeout as err:
        raise(err)


if __name__ == "__main__":
    allowed_files = {
        "decision_study_files": ["[\w\.]*"],
        "studies": ["[\w\.]*.yaml"],
        ".": ["menu.yaml", "decision_study.yaml", "decision_study_files",
              "studies"]
    }

    verify_source_directory("/Users/desautels2/GitRepositories/improvwf"
                            "/sample_output/remote_master/worker_1_source/",
                            allowed_files)

    source_dirs_string = "/Users/desautels2/GitRepositories/improvwf" \
                         "/sample_output/remote_master/global_history2.yaml " \
                         "/Users/desautels2/GitRepositories/improvwf" \
                         "/sample_output/remote_master/worker_1_source"
    target_dirs_string = "/Users/desautels2/GitRepositories/improvwf" \
                         "/sample_output/"

    copy_files_from_string_argument(source_dirs_string, target_dirs_string)
