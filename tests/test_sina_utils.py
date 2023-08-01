#!/usr/bin/env python
"""Run tests for select functions from "utils"."""

from sina.model import Record

import improvwf.db_interface.utils_sina as utils_sina
from tests.shared_db_test_code import TestNonAlteringDatabaseFunc, TestAlteringDatabaseFunc


class testNonAlteringSinaUtils(TestNonAlteringDatabaseFunc):
    """Test utils that don't alter the db."""

    __test__ = True

    # Request data formatted to support a few unit tests
    # Templated off of the "formatted_request" record from shared_db_test_code
    sample_req_data = {"run_descriptors": {
                    "study_parameters": {
                        "ANTIGEN_CHAINS_IN_STRUCT": {"values": ["A", "B"]},
                        "SOME_HASH": {"values": ["eggs", "md5"]},
                        "HASH_IM_NOT": {"values": "not_hashbrowns"},
                        "delist_me": {"values": ["please"]}},
                    "study_type": "very_cool"},
                    "status": "RUNNING"}

    def test_get_random(cls):
        """We should be able to fetch random Record(s) by type."""
        two_random_requests = list(cls.dao.recs.get_random(2, "request", ids_only=True))
        cls.assertEqual(len(two_random_requests), 2)
        cls.assertNotEqual(two_random_requests[0], two_random_requests[1])
        for request in two_random_requests:
            cls.assertEqual(cls.dao.recs.get(request).type, "request")

    # TODO: nullpool_init

    def test_exists(cls):
        """We should be able to tell if an ID is already found in the database."""
        existing_request_id = next(cls.dao.recs.get_random(1, "request", ids_only=True))
        non_existant_request_id = "i_do_not_exist"
        cls.assertTrue(cls.dao.recs.exists(existing_request_id))
        cls.assertFalse(cls.dao.recs.exists(non_existant_request_id))

    def test_sequences_are_similar(cls):
        """We should be able to tell if two amino sequences are similar within some distance."""
        cls.assertTrue(utils_sina.check_sequences_are_similar("rat", "rat", 0))
        cls.assertTrue(utils_sina.check_sequences_are_similar("rat", "rat", 10))
        cls.assertTrue(utils_sina.check_sequences_are_similar("rat", "cat", 1))
        cls.assertFalse(utils_sina.check_sequences_are_similar("rat", "car", 1))
        # Sequences of different lengths are never similar.
        cls.assertFalse(utils_sina.check_sequences_are_similar("rat", "at", 999))

    # TODO: get_from_matrix not currently in use
    # TODO: dao.connect

    def test_get_value(cls):
        """We should be able to get back values from Records."""
        # Note that this also covers get_value_from_record, as it wraps that function.
        cls.assertEqual(cls.dao.get_value("successful_request", "status"), "FINISHED")
        # Testing relisting functionality
        cls.assertEqual(cls.dao.get_value("successful_request", "MASTER_ANTIGEN_FASTA_HASH"),
                        ["mycoolantigenmasterhash"])
        # Raises an error (vs returning None or the like)
        with cls.assertRaises(KeyError) as context:
            cls.dao.get_value("successful_request", "nonexistant")
        cls.assertIn('nonexistant', str(context.exception))

    def test_create_request_from_data(cls):
        """We should be able to create a request record from data."""
        reqid = "formatted_request"
        expected_rec = cls.dao.recs.get(reqid)
        expected_rec.user_defined["run_descriptors"] = cls.sample_req_data["run_descriptors"]
        resulting_rec = cls.dao.create_request_from_data(reqid, cls.sample_req_data)
        cls.assertTrue(isinstance(resulting_rec.data["timestamp"]["value"], float))
        # Sync up the timestamps
        expected_rec.set_data("timestamp", resulting_rec.data["timestamp"]["value"])
        cls.assertEqual(expected_rec.data, resulting_rec.data)
        cls.assertEqual(expected_rec.user_defined, resulting_rec.user_defined)

    def test_create_data_from_request(cls):
        """We should be able to create a set of data from a request."""
        # This also tests reformat_dict
        reqid = "formatted_request"
        resulting_data = cls.dao.create_data_from_request(reqid)
        print(resulting_data)
        print(cls.sample_req_data)
        # Toplevel data preserved?
        cls.assertEqual(resulting_data["status"], cls.sample_req_data["status"])
        # Slightly less toplevel data preserved?
        cls.assertEqual(resulting_data["study_type"],
                        cls.sample_req_data["run_descriptors"]["study_type"])
        # Delisting done?
        cls.assertEqual(resulting_data["SOME_HASH"], "eggs")
        cls.assertEqual(resulting_data["delist_me"], "please")
        # Everything else de-nested?
        for name, entry in cls.sample_req_data["run_descriptors"]["study_parameters"].items():
            if name not in ("SOME_HASH", "delist_me"):
                cls.assertEqual(resulting_data[name], entry["values"])

    # TODO: set_request
    # TODO: get

    def test_get_request_id_by_partial_match(cls):
        """We should be able to get request ids by various partial matches on agent id."""
        two_annes_requests = cls.dao.get_request_id_by_partial_match(
            agent_id="agent_anne", agent_id_form=utils_sina.AgentIdForm.ENDS_WITH)
        cls.assertCountEqual(list(two_annes_requests), ["successful_request",
                                                        "unsuccessful_request"])
        anne_bond_requests = cls.dao.get_request_id_by_partial_match(
            agent_id="agent", agent_id_form=utils_sina.AgentIdForm.STARTS_WITH)
        cls.assertCountEqual(list(anne_bond_requests), ["successful_request"])

    def test_get_studies_by_string_distance(cls):
        """We should be able to find studies by string distance of some scalar from others."""
        # Note that this currently simply wraps get_nearest_studies_by_sequence
        no_distance = cls.dao.get_nearest_studies_by_sequence("rat", max_distance=0)
        cls.assertCountEqual(no_distance, ["rat_result"])
        distance = cls.dao.get_nearest_studies_by_sequence("bab", max_distance=2)
        cls.assertCountEqual(distance, ["rat_result", "tar_result"])
        no_match = cls.dao.get_nearest_studies_by_sequence("tambourine", max_distance=0)
        cls.assertCountEqual(no_match, [])

    def test_add_data(cls):
        """We should be able to add/update data to a record and list/delist appropriately."""
        test_record = Record(id="temp", type="rest_rec")
        # Test basic add
        cls.dao.add_data(test_record, "hello", "world")
        cls.assertEqual(test_record.data["hello"]["value"], "world")
        # Test adding data with kwargs
        cls.dao.add_data(test_record, "add_me", 123, units="abc", tags=["baz", "qux"])
        cls.assertEqual(test_record.data["add_me"],
                        {"value": 123, "units": "abc", "tags": ["baz", "qux"]})
        # Test updating data
        test_record.add_data("update_me", 123, units="abc", tags=["qux"])
        cls.dao.add_data(test_record, "update_me", "eggs", "cm^3", ["quux"])
        cls.assertEqual(test_record.data["update_me"],
                        {"value": "eggs", "units": "cm^3", "tags": ["quux"]})
        # Test delisting data
        cls.dao.add_data(test_record, "delist", [1], tags=["keep_me"])
        cls.assertEqual(test_record.data["delist"],
                        {"value": 1, "tags": ["keep_me", "delisted"]})


    def test_has_been_run(cls):
        """We should be able to tell whether a request has been run or not."""
        # Note: connects to in-memory set up in parent class
        was_run_req = cls.dao.recs.get("successful_request")
        cls.assertTrue(cls.dao.study_has_been_run([was_run_req])[0])
        wasnt_run_req = cls.dao.recs.get("unsuccessful_request")
        cls.assertFalse(cls.dao.study_has_been_run([wasnt_run_req])[0])
        cls.assertListEqual(list(map(bool,cls.dao.study_has_been_run([was_run_req, wasnt_run_req]))),
                            [True, False])

    def test_has_been_run_agentid(cls):
        """We should be able to identify something as a non-rerun by agent_id."""
        was_run_req = cls.dao.recs.get("successful_request")
        cls.assertTrue(cls.dao.study_has_been_run([was_run_req])[0])
        cls.assertFalse(cls.dao.study_has_been_run([was_run_req], "not_my_agent")[0])
        cls.assertTrue(cls.dao.study_has_been_run([was_run_req], "my_agent")[0])
