#!/usr/bin/env python
"""Run tests for select functions from "utils"."""

import unittest

import improvwf.db_interface.utils_sina as sina_utils

from tests.shared_db_test_code import TestNonAlteringDatabaseFunc

import improvwf.utils as utils


class testNonAlteringUtils(TestNonAlteringDatabaseFunc):
    """Test utils that don't alter the db."""

    __test__ = True

    def test_get_history_db_with_study_type(cls):
        """We should be able to output a subset of history depending on study type."""
        # Note: connects to in-memory db
        # Also note: get_history_db_from_dao returns a list of histories, len==#cpus. Hence the [0]
        test_studies_only = utils.get_history_db_from_dao(cls.dao, study_type="test_study")[0]
        cls.assertCountEqual(list(test_studies_only["history"].keys()), ["successful_request",
                                                                         "unsuccessful_request"])

    def test_get_history_db_with_distance(cls):
        """We should be able to output a subset of history depending on string distance."""
        studies_with_distance = utils.get_history_db_from_dao(cls.dao,
                                                              distance_datum="AntigenSequence",
                                                              distance_val="cat",
                                                              max_distance=1)[0]
        # Expected usage is to reject anything where lengths differ (will not return "at")
        cls.assertCountEqual(list(studies_with_distance["history"].keys()),
                             ["rat"])

    def test_get_history_db_with_structure_hash(cls):
        """We should be able to output a subset of history depending on structure hash."""
        studies_with_hash = utils.get_history_db_from_dao(
            cls.dao,
            structure_hash="mycoolstructurehash")[0]
        # Expected usage is to reject anything where lengths differ (will not return "at")
        cls.assertCountEqual(list(studies_with_hash["history"].keys()),
                             ["successful_request", "unsuccessful_request",
                              "successful_request_chain_a", "successful_request_chain_b"])

    def test_get_history_db_with_structure_hash_and_study_type(cls):
        """We should be able to output a subset of history depending on hash and type at the same time."""
        studies_with_hash_and_type = utils.get_history_db_from_dao(
            cls.dao,
            study_type="study_with_chain",
            structure_hash="mycoolstructurehash")[0]
        # Expected usage is to reject anything where lengths differ (will not return "at")
        cls.assertCountEqual(list(studies_with_hash_and_type["history"].keys()),
                             ["successful_request_chain_a", "successful_request_chain_b"])

    def test_get_history_db_with_agent_id(cls):
        """We should be able to output a subset of history depending on agent id."""
        # Note: connects to in-memory db
        agent_anne_only = utils.get_history_db_from_dao(cls.dao, agent_id="agent_anne")[0]
        cls.assertCountEqual(list(agent_anne_only["history"].keys()), ["successful_request"])
        both_annes = utils.get_history_db_from_dao(cls.dao, agent_id="anne",
                                                   agent_id_form=sina_utils.AgentIdForm.ENDS_WITH)[0]
        cls.assertCountEqual(list(both_annes["history"].keys()), ["successful_request",
                                                                  "unsuccessful_request"])
        success_only = utils.get_history_db_from_dao(
            cls.dao, agent_id="agent",
            agent_id_form=sina_utils.AgentIdForm.STARTS_WITH)[0]
        cls.assertCountEqual(list(success_only["history"].keys()), ["successful_request"])

    def test_get_history_db_using_nrandom(cls):
        """We should be able to combine nrandom and study_type."""
        request = utils.get_history_db_from_dao(cls.dao, study_type="test_study", nrandom=1)[0]
        all_requests = utils.get_history_db_from_dao(cls.dao, study_type="test_study")[0]
        cls.assertEqual(len(request["history"].keys()), 1)
        cls.assertIn(list(request["history"].keys())[0], all_requests["history"].keys())


class testAlteringUtils(TestNonAlteringDatabaseFunc):
    """
    Test utils that DO alter the db.

    Dropping and recreating the db after each test takes time, so only add tests
    here if they modify the database in some way.
    """

    __test__ = True

    def test_insert_paretos(cls):
        """We should be able to update the pareto set with new entries."""
        utils.update_pareto(cls.dao, ["cat"])
        # Make sure the stub is properly inserted.
        stubs = list(cls.dao.recs.data_query(ANTIGEN_SEQUENCE="cat"))
        cls.assertEqual(len(stubs), 1)
        rec = cls.dao.recs.get(stubs[0])
        cls.assertEqual(rec.id[-7:], "_pareto")
        cls.assertEqual(rec.data["study_type"]["value"], "pareto_stub")

    def test_update_paretos(cls):
        """Updating the pareto set should remove removed ones and leave existing ones alone."""
        utils.update_pareto(cls.dao, ["cat", "rat"])
        rec = cls.dao.recs.get(list(cls.dao.recs.data_query(ANTIGEN_SEQUENCE="cat"))[0])
        utils.update_pareto(cls.dao, ["cat", "bat"])
        # Make sure that "rat" was removed.
        cls.assertEqual(0, len(list(cls.dao.recs.data_query(ANTIGEN_SEQUENCE="rat"))))
        # Make sure that "cat" wasn't altered.
        rec2 = cls.dao.recs.get(list(cls.dao.recs.data_query(ANTIGEN_SEQUENCE="cat"))[0])
        cls.assertEqual(rec.id, rec2.id)
        cls.assertEqual(rec.data["timestamp"]["value"], rec2.data["timestamp"]["value"])
        # And finally, make sure that "bat" was inserted.
        stubs = list(cls.dao.recs.data_query(ANTIGEN_SEQUENCE="bat"))
        cls.assertEqual(len(stubs), 1)

    def test_get_pareto_sequences(cls):
        """We should be able to get the pareto set."""
        # NOTE: This could easily be in NonAlteringUtils; pairing it with
        # the "altering" update_pareto just keeps this independent of the
        # format of the pareto stubs, which might be altered for a little (TODO)
        utils.update_pareto(cls.dao, ["cat", "rat"])
        utils.update_pareto(cls.dao, ["bat", "rat", "rabbit", "coolpareto"])
        paretos = utils.get_pareto(cls.dao)
        cls.assertCountEqual(["bat", "rat", "rabbit", "coolpareto"], paretos)
