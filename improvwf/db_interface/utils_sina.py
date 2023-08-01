################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################
"""
A sina database access object for working with the improvwf package.
"""

import logging
import os
import hashlib
import time
from collections import defaultdict
from enum import Enum

import sqlalchemy
from sqlalchemy.pool import NullPool

from sina.datastores.sql import SQLITE_PREFIX
import sina.datastores.sql as sql
import sina.datastores.sql_schema as schema
from sina.model import Record, Relationship
try:
    # Python 3.3 and later
    from collections.abc import Sequence
except ImportError:
    # Python 2.7
    from collections import Sequence


LOGGER = logging.getLogger(__name__)

# ##################################################
# ### BEGIN MONKEY PATCH FOR RANDOM ACCESS TO DB ###
# ##################################################

def _get_random(self, count, type=None, ids_only=False):
    """
    Select <count> random Records, optionally of a specific type.

    This is a "beta" version of what will eventually be added into Sina proper.
    The logic for implementing it in SQL is much simpler than what Cassandra will require.

    :param type: The type of Record to restrict the random selection to, None to take any Record.
    :param count: The number of random Records to return
    :param ids_only: Whether to return only the ids (versus the entire Record object)
    """
    # Logic for selecting the random IDs
    query = self.session.query(schema.Record.id)
    if type is not None:
        query = query.filter(schema.Record.type == type)
    query = query.order_by(sqlalchemy.func.random()).limit(count)

    if ids_only:
        for record_id in query.all():
            yield str(record_id[0])
    else:
        filtered_ids = (str(x[0]) for x in query.all())
        for record in self.get(filtered_ids):
            yield record


# Apply monkey patch
sql.RecordDAO.get_random = _get_random
# Monkey patch application complete

# ###################################################
# ### FINISH MONKEY PATCH FOR RANDOM ACCESS TO DB ###
# ###################################################

# ######################################################
# ### BEGIN MONKEY PATCH FOR NON-POOLING CONNECTIONS ###
# ######################################################


def nullpool_init(self, db_path=None):
    """
    Initialize a Factory with a path to its backend and without pooling behavior.

    :param db_path: Path to the database to use as a backend. If None, will
                    use an in-memory database. If it contains a '://', it is assumed that
                    this is a URL which can be used to connect to the database. Otherwise,
                    this is treated as a path for a SQLite database.
    """
    self.db_path = db_path
    use_sqlite = False
    if db_path:
        if '://' not in db_path:
            self.engine = sqlalchemy.create_engine(SQLITE_PREFIX + db_path)
            create_db = not os.path.exists(db_path)
            use_sqlite = True
        else:
            self.engine = sqlalchemy.create_engine(db_path,
                                                   poolclass=NullPool)
            create_db = True
    else:
        # In-memory db does not play well with setting poolclass, apparently.
        # Inherently not a worry.
        self.engine = sqlalchemy.create_engine(SQLITE_PREFIX)
        use_sqlite = True
        create_db = True

    if use_sqlite:
        def configure_on_connect(connection, _):
            """Activate foreign key support on connection creation."""
            connection.execute('pragma foreign_keys=ON')

        sqlalchemy.event.listen(self.engine, 'connect',
                                configure_on_connect)

    if create_db:
        schema.Base.metadata.create_all(self.engine)

    session = sqlalchemy.orm.sessionmaker(bind=self.engine)
    self.session = session()


# Apply the patch
sql.DAOFactory.__init__ = nullpool_init
# Monkey patch application complete

# #######################################################
# ### FINISH MONKEY PATCH FOR NON-POOLING CONNECTIONS ###
# #######################################################

# ########################################################
# ### BEGIN MONKEY PATCHES FOR VARIOUS CUSTOM QUERIES ###
# ########################################################


def _exists(self, id):
    """
    Return whether a record exists.

    :param id: str, id whose existence should be checked.
    :return: bool, whether record with that id exists.
    """
    return bool(self.session.query(schema.Record.id)
                .filter(schema.Record.id == id).one_or_none())


# More recent versions of Sina have an exists(), but it's called exist() as
# it usually takes lists (but can still take a string)
if not hasattr(sql.RecordDAO, "exists"):
    sql.RecordDAO.exists = _exists
else:
    sql.RecordDAO.exists = sql.RecordDAO.exist
# Monkey patch application complete

# ########################################################
# ### END MONKEY PATCHES FOR VARIOUS CUSTOM QUERIES ###
# ########################################################


class NoConnection(Exception):
    pass


def _is_str(obj):
    "Check for string types "
    return isinstance(obj, (str, bytes, bytearray))


def _seq_but_not_str(obj):
    "Check for sequence but not string"
    return isinstance(obj, Sequence) and not _is_str(obj)


def _delist(param):
    """
    Return the item in a list if the list is unit length,
    else return the object
    """
    if _seq_but_not_str(param) and len(param)==1:
        return param[0]
    return param


def _relist(param):
    """
    Wrap an item back in a list

    :param param: a delisted value
    :return: single-element list wrapping param
    """

    return [param]


def check_sequences_are_similar(seq1, seq2, max):
    """Return whether two amino sequences are within some distance."""
    if len(seq1) != len(seq2):
        return False
    distance = 0
    for index, char in enumerate(seq2):
        if seq1[index] != char:
            distance += get_from_matrix(char, seq1, index)
        if distance > max:
            return False
    return True


def get_from_matrix(char, seq, index):
    """If there's a substitution matrix, use it."""
    # try:
    #     return SUBSTITUTION_MATRIX[char][seq[index]]
    # except KeyError:
    return 1


def reformat_dict(data):
    """Remove label and value nesting from data."""
    for key in data.keys():
        if key == "value" or key == "values":
            return data[key]
        elif data[key] == 'label':
            return {data[key]['label']: reformat_dict(data['values'])}
        elif type(data[key]) == dict:
            data[key] = reformat_dict(data[key])
    return data


# Arguably this makes more sense in utils.py. However, the function that would use
# it has a dependency on this file, so we put this here to avoid a circular dependency.
# Future implementations might clean this up.
class AgentIdForm(Enum):
    """Describes how to match an agent_id when using get_history_db_from_dao."""

    STARTS_WITH = 1
    ENDS_WITH = 2
    IS = 3
    REGEX = 4


class DAO:
    """An interface class for improv to Sina."""

    def __init__(self, db_url=None):
        """
        Given a databse url , create a SINA connection

        :param db_url: (str) The database access url
        """
        self.conn = None
        self.recs = None
        self.rels = None
        self.default_url = db_url
        if db_url:
            self.connect(db_url)

    def __str__(self):
        return str(self.default_url)

    def connect(self, db_url=None):
        """
        Create the DAO connection with a record, relationship and runs interface objects

        :param db_url: (str) The database access url
        """
        if db_url is not None:
            self.conn = sql.DAOFactory(db_path=db_url)
        else:
            self.conn = sql.DAOFactory(db_path=self.default_url)
        if self.conn:
            self.recs = self.conn.create_record_dao()
            self.rels = self.conn.create_relationship_dao()
        else:
            raise(NoConnection)

    def get_value(self, rid, key):
        """
        Get a value from the Record data object

        :param rid: The record id
        :param key: The key for the data requested
        """

        # tmp = self[rid]["data"][key]
        tmp = self[rid]
        return self.get_value_from_record(tmp, key)

    def get_value_from_record(self, rec, key):
        """
        Get a value from a retrieved Record

        :param rec: Record containing the data in question.
        :param key: The key for the data requested.
        """

        if "tags" in rec["data"][key].keys() and "delisted" in rec["data"][key]["tags"]:
            # print("Relisting required!")
            return _relist(rec["data"][key]["value"])
        return rec["data"][key]["value"]

    @staticmethod
    def create_request_from_data(reqid, req_data):
        """
        Given a request id and some data, create a request Record.

        :param reqid: str, the name describing the request
        :param req_data: dict, the data to set in the request

        :returns: Sina Record representataion of the request.
        """
        request = Record(id=reqid, type="request")
        request.add_data("status", req_data["status"])
        request.add_data("timestamp", time.time())
        request.user_defined["run_descriptors"] = req_data["run_descriptors"]
        for name, entry in req_data["run_descriptors"]["study_parameters"].items():
            value = (_delist(entry["values"]) if name != "ANTIGEN_CHAINS_IN_STRUCT"
                     else entry["values"])
            tags = ["delisted"] if value != entry["values"] else []
            # We can have either *_HASH or *_HASH_somenum signifying a hash
            # SOMENAMEHASH (no underscore before HASH) shouldn't show up
            split_name = name.split("_")
            if split_name[-1] == "HASH" or len(split_name) > 1 and split_name[-2] == "HASH":
                # Store the hash type as a tag
                tags.append(value[1])
                # Extract the actual value
                value = value[0]
            request.add_data(name, value, tags=tags)
        request.add_data("study_type", req_data["run_descriptors"]["study_type"])
        return request

    def create_data_from_request(self, reqid):
        """
        Given a request id, create data using request record.

        :param reqid: str, the name describing the request

        :returns: dictionary representataion of the request.
        """
        try:
            data = self.recs.get(reqid).data
        except ValueError:
            print("record does not exist")
        data = reformat_dict(data)
        return data

    def set_request(self, reqid, req_data):
        """
        Set a request object in the database.

        :param reqid: The name describing the request
        :param req_data: A dict of the data to set in the request
        """
        # Some of the initial pile of data comes in a nested form.
        # We need to separate and process it so it resembles what Sina can query
        if self.recs:
            request = self.create_request_from_data(reqid, req_data)
            self.recs.insert(request)
        else:
            raise(NoConnection)

    def __getitem__(self, rid):
        """
        Get a Record object from the database

        :param reqid: The name describing the Record
        """
        if self.recs:
            return self.recs.get(rid)
        return None

    def _precheck_study_has_been_run(self, proposed_studies, agent_id, equality_params):
        """
        Cheaply return whether one or more sets of study parameters definitely has not been run.

        Relies on theoretically cheaper checks than study_has_been_run; use it first to see if you can locate
        enough runs for your purpose.

        Something that returns True here *may* have been run, but you'd need to use the more expensive
        checks to know for sure, whereas False means that it *definitely hasn't*.

        :param proposed_studies: A list of Sina record[s] representing the study we want to check.
                                 A subset of its data will be used to perform the check.
        :param agent_id: The agent to restrict these checks to. If specified (instead of None), only studies handled by
                         the specified agent can be considered potential duplicates.
        :param equality_params: A list of things to check exact match on or None. If a list, all must match for the study to be a duplicate.
                                If None, default behavior: ANTIGEN_SEQUENCE_HASH, ANTIGEN_FASTA_HASH if the prior isn't
                                available. You may instead want ex: [ANTIGEN_SEQUENCE_HASH, ANTIGEN_SEQUENCE_HASH_2]
                                for a two-sequence study.
        :returns: a list of whether each proposed study may possibly have been run (True) or definitely hasn't been run (False).
                  Also returns a dictionary of {param: {value: [corresponding_id_1, corresponding_id_2, ...]}} that will
                  be used in the more expensive checks (if those checks are necessary).
        """
        # Before we do anything else, let's figure out which potential duplicate studies are even valid!
        # If agent_id is specified, we might have a pretty small pool to worry about.
        if agent_id is not None:
            ids_requested_by_agent = set(x.object_id for x in
                                         self.rels.get(subject_id=agent_id, predicate="requests"))
            if not ids_requested_by_agent:
                # Agent hasn't run anything, so there can't be any duplicates
                return [False]*len(proposed_studies), {}
        else:
            ids_requested_by_agent = None

        # There can be many studies already in the db that have the same params
        # We'll need them all if we want to check if something's been run before
        # {"ANTIGEN_FASTA_HASH": {antigen_fasta_hash_val_1: [preexisting_study_id_1, ...]}}
        found_values = defaultdict(lambda: defaultdict(set))

        s = 998  # fits default limit for mysql IN queries

        def get_results_that_match(vals_to_query, name_of_val):
            """
            Get requests whose value for <name_of_val> equals a value found in <vals_to_query>.

            You'll notice that this only operate on a single name_of_val at a time,
            despite the fact that the override behavior in equality_params is an
            AND. Basically, the old case was an OR (technically), the new case looks to have
            high cardinality, so I'm going with the simpler solution of set intersects
            for now.
            """
            for val_chunk in [vals_to_query[i:i+s]
                              for i in range(0, len(vals_to_query), s)]:
                found_vals = (self.recs.session
                              .query(schema.StringData.value, schema.StringData.id)
                              .filter(schema.StringData.name == name_of_val)
                              .filter(schema.StringData.value.in_(val_chunk)).all())
                for result in found_vals:
                    val, id = result
                    if ids_requested_by_agent is None or id in ids_requested_by_agent:
                        found_values[name_of_val][val].add(id)

        studies_might_exist = []
        # Byproduct of checks, also used in the more expensive checks if necessary.
        # Basically, if there's an existing record that MIGHT match a proposed one, the proposed
        # one can't be given a False in this function. While the key:val order is strange here, it's more
        # useful down the line, and there's no reason to do it both ways.
        existing_to_proposed = defaultdict(list)  # existing_id: [potential_match_id_1, potential_match_id_2...]
        if equality_params is None:
            sequence_hashes_to_check = []
            fasta_hashes_to_check = []
            for x in proposed_studies:
                if "ANTIGEN_SEQUENCE_HASH" in x.data:
                    sequence_hashes_to_check.append(x.data["ANTIGEN_SEQUENCE_HASH"]["value"])
                else:
                    fasta_hashes_to_check.append(x.data["ANTIGEN_FASTA_HASH"]["value"])
            get_results_that_match(sequence_hashes_to_check, "ANTIGEN_SEQUENCE_HASH")
            get_results_that_match(fasta_hashes_to_check, "ANTIGEN_FASTA_HASH")
            studies_might_exist = []
            for x in proposed_studies:
                target_param = "ANTIGEN_SEQUENCE_HASH"
                if "ANTIGEN_SEQUENCE_HASH" not in x.data:
                    target_param = "ANTIGEN_FASTA_HASH"
                proposed_val = x.data[target_param]["value"]
                studies_might_exist.append(proposed_val in found_values[target_param])
                for existing_id in found_values[target_param][proposed_val]:
                    existing_to_proposed[existing_id].append(x.id)
        else:
            for param_name in equality_params:
                get_results_that_match([x.data[param_name]["value"] for x in proposed_studies],
                                       param_name)
            for x in proposed_studies:
                required_sets = [found_values[y].get(x.data[y]["value"], set()) for y in equality_params]
                potential_matches = required_sets[0].intersection(*required_sets[1:])
                for existing_potential_match in potential_matches:
                    existing_to_proposed[existing_potential_match].append(x.id)
                studies_might_exist.append(bool(potential_matches))
        return studies_might_exist, existing_to_proposed

    def study_has_been_run(self, proposed_studies, agent_id=None, quit_at=None, equality_params=None):
        """
        Return whether one or more sets of study parameters have already been run.

        Should be more efficient when used on batches of proposed studies.

        :param proposed_studies: Sina Record or list of Sina Record[s] representing the stud[ies] we
                                 want to check. A subset of .data will be used to perform the check.
        :param agent_id: An optional additional criteria in the form of an agent_id. If passed,
                         a study will only be considered identical if it fulfills all other criteria
                         PLUS was requested by the same agent. This is to allow for reruns of past
                         requests without duplicating anything within the current agent.
        :param quit_at: If this many records are found to not exist in the database, return early.
                        This lets us avoid performing more expensive tests if we can find "enough" without them.
                        Note that any as-of-yet unknown entries will be set as bool True ("exists"),
                        even if they might not exist, since setting quit_at assures us
                        you don't care about the "individual" results, just finding enough to run.
                        If None (default), don't stop until every Record's known for sure.
        :param equality_params: A list of things to check exact match on. All must match for the study to be a duplicate.
                                By default, it's [ANTIGEN_SEQUENCE_HASH] (or ANTIGEN_FASTA_HASH if the prior isn't
                                available). You may instead want ex: [ANTIGEN_SEQUENCE_HASH, ANTIGEN_SEQUENCE_HASH_2]
                                for a two-sequence study. Exclusively used by _precheck_study_has_been_run, must be
                                a scalar or string.
        :return: bool, True if a study with those parameters exists, else False. In case
                 multiple sets were tested, returns a list of bools.
        """
        return_single_bool = False
        if isinstance(proposed_studies, Record):
            proposed_studies = [proposed_studies]
            return_single_bool = True
        proposed_studies_by_id = {x.id: x for x in proposed_studies}  # Convenient form

        if quit_at is None:
            quit_at = len(proposed_studies)

        # We first do the "easy" checks to see if we can quit out quickly.
        # Note that the easy checks take the agent_id into account! So any potential duplicate studies
        # have already been filtered with regards to agent id.
        might_have_been_run, existing_to_proposed_map = self._precheck_study_has_been_run(proposed_studies, agent_id, equality_params)

        # If the number False exceeds our quit_at, there's no need to continue
        if might_have_been_run.count(False) >= quit_at:
            return might_have_been_run[0] if return_single_bool else might_have_been_run

        # Then we compare the relevant features of each proposed study with its preexisting ones
        # We also check "status" and ignore pre-existing studies that aren't FINISHED or RUNNING
        study_followup_criteria = ["status",
                                   "MASTER_ANTIGEN_FASTA_HASH",
                                   "STRUCTURE_HASH",
                                   "study_type"]
        preexisting_rec_ids = list(existing_to_proposed_map.keys())
        s = 998  # fits default limit for mysql IN queries
        s_followup = s - len(study_followup_criteria)
        # This isn't necessarily our final check; we're also looking to see which preexisting studies
        # need to be checked using our most expensive criteria.
        third_round_existing_to_proposed = defaultdict(list)

        # Again, we chunk to avoid SQL errors.
        for followup_chunk in [preexisting_rec_ids[i:i+s_followup]
                               for i in range(0, len(preexisting_rec_ids), s_followup)]:
            followup_data = self.recs.get_data_for_records(data_list=study_followup_criteria,
                                                           id_list=followup_chunk)
            for existing_id in followup_chunk:
                existing_data = followup_data[existing_id]
                if existing_data["status"]["value"] not in ["RUNNING", "FINISHED"]:
                    existing_to_proposed_map.pop(existing_id)
                    continue
                for proposed_id in existing_to_proposed_map[existing_id]:
                    proposed_data = proposed_studies_by_id[proposed_id].data
                    is_duplicate = True
                    for criterion in study_followup_criteria[1:]:  # skip matching status
                        if proposed_data[criterion]["value"] != existing_data[criterion]["value"]:
                            is_duplicate = False
                            break
                    # If we don't find any differences, it's a potential duplicate
                    if is_duplicate:
                        third_round_existing_to_proposed[existing_id].append(proposed_id)

        third_round_potential_duplicates = list(third_round_existing_to_proposed.keys())

        # A Sina schema optimization limits get_data_for_records (I should rename it... -Becky)
        # to non-list data, as list data is stored in a totally different form. So we have
        # this final check to compare the antigen chains of the proposed studies to
        # potential pre-existing ones
        existing_study_chains_data = defaultdict(list)
        duplicate_studies = set()
        for study_chunk in [third_round_potential_duplicates[i:i+s] for i in range(0, len(third_round_potential_duplicates), s)]:
            antigen_chains_query = (self.recs.session.query(schema.ListStringDataEntry.id, schema.ListStringDataEntry.value)
                                        .filter(schema.ListStringDataEntry.name == "ANTIGEN_CHAINS_IN_STRUCT")
                                        .filter(schema.ListStringDataEntry.id.in_(study_chunk)).all())
            for found_chain in antigen_chains_query:
                existing_study_chains_data[found_chain[0]].append(found_chain[1])
            for existing_id, existing_chain in existing_study_chains_data.items():
                for proposed_id in third_round_existing_to_proposed[existing_id]:
                    if proposed_studies_by_id[proposed_id].data["ANTIGEN_CHAINS_IN_STRUCT"]["value"] == existing_chain:
                        duplicate_studies.add(proposed_id)

        preexistence_status = [x.id if x.id in duplicate_studies else False for x in proposed_studies]

        if return_single_bool:
            return preexistence_status[0]

        return preexistence_status

    def get_request_id_by_partial_match(self, agent_id, agent_id_form):
        """Handle the database-side logic for partial matches on agent id."""
        if agent_id_form == AgentIdForm.STARTS_WITH:
            filter_func = schema.Relationship.subject_id.startswith
        elif agent_id_form == AgentIdForm.ENDS_WITH:
            filter_func = schema.Relationship.subject_id.endswith
        else:
            raise ValueError("Agent id form {} is not yet supported!".format(agent_id_form))
        query = (self.recs.session.query(schema.Relationship.object_id)
                 .filter(schema.Relationship.predicate == "requests")
                 .filter(filter_func(agent_id)))
        return set(x[0] for x in query.all())

    def get_studies_by_string_distance(self, datum, value, max_distance):
        """
        Use simple string distance to find "nearby" studies.

        Provided the name of some datum and a value for it, looks up that datum in all
        available results and returns the ids of any results whose value of <datum> is
        within <max_distance> (inclusive) of <value>.

        For now, this is just a wrapper on get_nearest_studies_by_sequence. This is a quick
        stopgap so I can get this feature out ASAP without it breaking as I refactor. Expect
        the functionality of get_nearest_studies_by_sequence() to be split between this and
        something in abag_agent_setup.

        :param datum: str, the datum to match on.
        :param value: str, the value of the string to match on.
        :param max_distance: int, the max (inclusive) distance allowed from <value>.
        """
        # max_distance of 0 indicates an exact match, so we can shortcut
        if max_distance == 0:
            return list(self.recs.data_query(**{datum: value}))
        # *For now*, <datum> is entirely ignored outside that special case
        # That's because we expect AntigenSequence could/will itself be a special case
        # (using a score instead of absolute distance, based on amino substitutions)
        # and it's also the only one we use right now.
        return self.get_nearest_studies_by_sequence(value, max_distance)

    def get_nearest_studies_by_sequence(self, sequence, max_distance=5):
        """
        Use simple string distance to find "nearby" studies.

        This is a fairly "unintelligent" (but fast-ish) way of calculating
        distance that doesn't take into account deletions/insertions and could
        theoretically weight amino acid substitutions.

        "Distance" is inherently arbitrary, and the initial implementation is
        probably nonsense. But basically:
        cat
        bar
        rat

        Rat has a distance of 1 from cat, and bar has a distance of 2. We
        immediately drop anything where sequence length differs (cat vs. catatonic)
        and doing that is a big part of why we can be "fast".

        :param sequence: str, The sequence whose distance should be checked
        :param max_distance: int, Any "distance" above this amount will be dropped.
        :returns: [str], a list of ids of results with similar seqeuences
        """
        # TODO: possible improvement, could we cache this? I assume we'll want
        # to do this check many times, at least once per decision-maker round.
        sequence_map = {y["AntigenSequence"]["value"]: x
                        for x, y in self.recs.get_data_for_records(["AntigenSequence"]).items()}
        similar_sequences = []
        for entry in sequence_map.keys():
            if check_sequences_are_similar(sequence, entry, max_distance):
                similar_sequences.append(entry)
        return [sequence_map[similar_sequence] for similar_sequence in similar_sequences]

    def add_data(self, rec, key, val, units=None, tags=None):
        """
        Add or Set data in a Record

        :param name: The name describing the data (ex: "direction", "volume","time")
        :param value: The data's value (ex: "northwest", 12, [0, 1, 3, 6])
        :param units: Units for the value. Optional (ex: "cm^3", "seconds")
        :param tags: List of tags describing this data. Optional (ex: ["inputs", "opt"])
        """

        val_delisted = _delist(val)
        if val_delisted != val:
            if tags is None:
                tags = []
            tags = tags + ["delisted"]
            # print("Delisting executed for record {}, key {}!".format(rec.id, key)  )

        if key in rec.data:
            rec.set_data(key, val_delisted, units=units, tags=tags)
        else:
            rec.add_data(key, val_delisted, units=units, tags=tags)

    def update_request(self, reqid, req_data):
        """
        Update a request object in the database.

        :param reqid: The name describing the request
        :param req_data: A dict of the data to update in the request
        """
        if self.recs:
            request = self.recs.get(reqid)
            # Get relationships: keep the "requests" relationship from owning
            # daemon to the request when we remove and re-insert this request.
            all_relationships_obj = self.rels.get(object_id=reqid)
            all_relationships_subj = self.rels.get(subject_id=reqid)
            assert len(all_relationships_subj) == 0, \
                "Requests (studies) should have no results when their status " \
                "or other attributes are being updated."

            updated_request = self.create_request_from_data(reqid, req_data)
            for key, entry in updated_request.data.items():
                # We don't use self.add_data because we already did the delist.
                if key=="run_descriptors":
                    request.user_defined[key] = entry
                else:
                    add_data_func = request.set_data if key in request.data else request.add_data
                    add_data_func(key, entry["value"],
                                  units=entry.get("units", None), tags=entry.get("tags", []))
            self.recs.update(request)

        else:
            raise(NoConnection)

    def set_result(self, reqid, res_data):
        """
        Set a result object in the database

        :param reqid: The name describing the request
        :param req_data: A dict of the data to set in the result
        """
        resid = ""
        if self.recs and self.rels:
            resid = reqid + "_result"
            result = Record(id=resid, type="result")
            for key, val in res_data.items():
                self.add_data(result, key, val)
            # Special case: calculate ddg:
            if res_data.get("FoldXInterfaceDG") and not res_data.get("FoldXInterfaceDeltaDG"):
                assert isinstance(res_data.get("FoldXInterfaceDG"), list) \
                    and len(res_data.get("FoldXInterfaceDG")) == 1 \
                    and isinstance(res_data.get("WT_FoldXInterfaceDG"), list) \
                    and len(res_data.get("WT_FoldXInterfaceDG")) == 1, \
                    "FoldXInterfaceDG and WT_FoldXInterfaceDG must both be " \
                    "1-element lists!"
                self.add_data(result, "FoldXInterfaceDeltaDG",
                              res_data.get("FoldXInterfaceDG")[0] - res_data.get("WT_FoldXInterfaceDG")[0],
                              tags=["delisted"])
            self.recs.insert(result)

            rel = Relationship(subject_id=reqid,
                               predicate="yields",
                               object_id=resid)
            self.rels.insert(rel)
        else:
            raise(NoConnection)

    def disconnect(self):
        """
        Close existing session
        """

        if self.conn is not None:
            self.conn.close()


class FormatConverter():
    """Converts various YAML things to Sina (and vice-versa?)."""

    @staticmethod
    def get_sina_menu_from_yaml(yaml_obj):
        """
        Extract the metadata from a yaml menu and return it as a Sina Record.

        :param yaml_obj: (yaml object) The yaml representation of the menu
        :returns: (Sina Record) The Sina representation of the menu
        """
        menu_record = Record(id=yaml_obj["description"]["name"], type="menu")
        menu_record.add_data("description", yaml_obj["description"]["description"])
        study_hashes = []
        for study in yaml_obj["studies"]:
            study_hashes.append(hashlib.md5(str(study).encode('utf-8')).hexdigest())
        menu_record.add_data("contained_studies", study_hashes)
        return menu_record

    @staticmethod
    def get_sina_study_template_from_yaml(yaml_obj, template_id=None):
        """
        Extract the data from a yaml menu study template and return it as a Sina Record.

        TODO: This doesn't really belong in improv, but it's the best place for it right
        now. In the future, FormatConverter could be abstract and this implementation
        moved elsewhere.

        :param yaml_obj: (yaml object) The yaml representation of the template
        :param template_id: An id for the template. If not provided, will use the MD5
                         of the template's hash.
        :returns: (Sina Record) The Sina representation of the template
        """
        if template_id is None:
            template_id = hashlib.md5(str(yaml_obj)).hexdigest()
        template_record = Record(id=template_id, type="template")
        params = yaml_obj["study_parameters"]
        template_record.add_data("study_type", yaml_obj["study_type"])
        template_record.add_data("MasterAntigenID", params["MasterAntigenID"])
        template_record.add_data("MasterAntigenSequence", params["MasterAntigenSequence"])
        template_record.add_data("MasterAntigenFASTAHash",
                                 params["MasterAntigenFASTAHash"][0],
                                 tags=[params["MasterAntigenFASTAHash"][1]])
        template_record.add_file(params["MasterAntigenFASTAPath"])
        struct = params["Structure"]
        template_record.add_file(struct["StructureID"])
        template_record.add_file(struct["StructurePath"])
        template_record.add_data("StructureHash",
                                 struct["StructureHash"][0],
                                 tags=[struct["StructureHash"][1]])
        template_record.add_data("AntigenChainsInStructure", struct["AntigenChainsInStructure"])
        # We need to be able to ask for studies that allow a given mutation (36L),
        # allow mutations at a position (36), and allow specific mutations (A36L),
        # which we accomplish like this:
        allowed_mutation_positions = []
        allowed_mutations = []
        allowed_to_mutate = []
        for position, current, mutations in params["AllowedMutations"]["AllowedMutations"]:
            allowed_mutation_positions.append(position)
            # We store two halves of the mutation: the "from" and the "to". This way,
            # we can combine "what has A36" (from) and "what allows 36L" (to) for "A36L"
            # First the "from"
            allowed_to_mutate.append(current+str(position))
            # Now all the "to"s:
            for mutation in mutations:
                allowed_mutations.append(str(position)+mutation)
        # If desired, the "from" could be expanded to the entire sequence instead of just
        # positions where mutations are allowed, no idea if that's useful though.
        template_record.add_data("AllowedMutationPositions", allowed_mutation_positions)
        template_record.add_data("AllowedMutations", allowed_mutations)
        template_record.add_data("AllowedToMutate", allowed_to_mutate)
        return template_record
