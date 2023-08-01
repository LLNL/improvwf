#!/usr/bin/env python
"""Run tests for select functions from "utils"."""

import unittest

from sina.model import Record, Relationship

import improvwf.db_interface.utils_sina as sina_utils


def create_prepopulated_database(dao):
    """Use a dao to build a testable database."""
    # Create a good request
    req_rec = _make_minimal_request("successful_request")
    req_rec.set_data("status", "FINISHED")
    req_rec.add_data("study_type", "test_study")
    req_rec.add_data("ANTIGEN_CHAINS_IN_STRUCT", ["a", "b"])
    req_rec.add_data("ANTIGEN_FASTA_HASH", "mycoolantigenhash")
    req_rec.add_data("MASTER_ANTIGEN_FASTA_HASH", "mycoolantigenmasterhash",
                     tags=["delisted", "MD5"])
    req_rec.add_data("STRUCTURE_HASH", "mycoolstructurehash")
    # Misc. data used to test custom params
    req_rec.add_data("favorite_color", "green")
    dao.recs.insert(req_rec)
    # Use our good request to build a failed request
    req_rec.id = "unsuccessful_request"
    req_rec.set_data("status", "FAILED")
    req_rec.set_data("ANTIGEN_FASTA_HASH", "mylesscoolantigenhash")
    dao.recs.insert(req_rec)
    # Requests that differ from "successful" only on their chains in use & id
    # We use these for testing both chains and "many pre-existing studies" behavior
    # (That is, if multiple existing studies resemble a proposed one, all should be considered)
    req_rec.id = "successful_request_chain_a"
    req_rec.set_data("study_type", "study_with_chain")
    req_rec.set_data("status", "RUNNING")  # We're fine with either FINISHED or RUNNING
    req_rec.set_data("ANTIGEN_FASTA_HASH", "mycoolantigenhash")
    req_rec.set_data("ANTIGEN_CHAINS_IN_STRUCT", ["a"])
    dao.recs.insert(req_rec)
    req_rec.id = "successful_request_chain_b"
    req_rec.set_data("ANTIGEN_CHAINS_IN_STRUCT", ["b"])
    dao.recs.insert(req_rec)
    # A request that has all the data we need to test the create_request_from_data and its inverse.
    formatted_request = Record(id="formatted_request", type="request")
    formatted_request.data = {"status": {"value": "RUNNING"},
                              "SOME_HASH": {"value": "eggs", "tags": ["md5"]},
                              "ANTIGEN_CHAINS_IN_STRUCT": {"value": ["A", "B"], "tags": []},
                              "HASH_IM_NOT": {"value": "not_hashbrowns", "tags": []},
                              "delist_me": {"value": "please", "tags": ["delisted"]},
                              "study_type": {"value": "very_cool"}}
    dao.recs.insert(formatted_request)
    # These are only used in one test right now
    # Will probably flesh them out over time.
    rat_result = Record(id="rat_result", type="result", data={"AntigenSequence": {"value": "rat"}})
    tar_result = Record(id="tar_result", type="result", data={"AntigenSequence": {"value": "tar"}})
    at_result = Record(id="at_result", type="result", data={"AntigenSequence": {"value": "at"}})
    dao.recs.insert([rat_result, tar_result, at_result, _make_minimal_request("rat"),
                     _make_minimal_request("tar"), _make_minimal_request("at")])
    agent_rec = Record(id="my_agent", type="agent")
    dao.recs.insert(agent_rec)
    dao.rels.insert(subject_id="my_agent", predicate="requests", object_id="successful_request")
    dao.recs.insert(Record(id="agent_anne", type="agent"))
    dao.recs.insert(Record(id="broken_agent_anne", type="agent"))
    dao.recs.insert(Record(id="agent_bond", type="agent"))
    dao.rels.insert(subject_id="agent_anne", predicate="requests", object_id="successful_request")
    dao.rels.insert(subject_id="broken_agent_anne", predicate="requests",
                    object_id="unsuccessful_request")
    dao.rels.insert(subject_id="agent_bond", predicate="requests", object_id="successful_request")


def _make_minimal_request(id):
    """Return a Record with the requirements of a request (namely run_descriptors)."""
    return Record(id=id, type="request", data={"status": {"value": "FINISHED"}},
                  user_defined={"run_descriptors": {
                     "study_type": "test_study",
                     "study_parameters": {"eggs": {"values": [1, 2, 3, 12]}}}})


class TestNonAlteringDatabaseFunc(unittest.TestCase):
    """Test utils that don't alter the db."""

    # Used to prevent this base class from running. Overridden by children.
    __test__ = False

    @classmethod
    def setUpClass(cls):
        """Create a database to share between tests."""
        cls.dao = sina_utils.DAO()
        cls.dao.connect()
        create_prepopulated_database(cls.dao)

    @classmethod
    def tearDownClass(cls):
        """When all tests are done, shut down the connection."""
        cls.dao.disconnect()


class TestAlteringDatabaseFunc(unittest.TestCase):
    """
    Test db-altering functionality.

    The database is recreated for every test, so if you need that functionality
    (create, update, delete), the test class should use this.
    """

    # Used to prevent this base class from running. Overridden by children.
    __test__ = False

    def setUp(self):
        """Create a database to test against."""
        self.dao = sina_utils.DAO()
        self.dao.connect()
        create_prepopulated_database(self.dao)

    def tearDown(self):
        """When the test is done, shut down the connection."""
        self.dao.disconnect()
