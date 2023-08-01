#!/usr/bin/env python
"""
Run tests on a select subset of features related to Sina.

Tested code comes from utils_sina, as use_sina seems to be more for forming connections.
"""

import unittest

from tests.shared_db_test_code import TestNonAlteringDatabaseFunc, TestAlteringDatabaseFunc

import improvwf.db_interface.utils_sina as sina_utils


class testSinaNonAlteringOperations(TestNonAlteringDatabaseFunc):
    """
    Test non-db-altering Sina functionality.

    If it doesn't involve insertion or deletion, it can be tested here.
    """

    __test__ = True

    def test_study_has_been_run(self):
        """We should be able to check whether a study's been run."""
        successful = self.dao.recs.get("successful_request")
        self.assertTrue(self.dao.study_has_been_run(successful))
        nonexistant = self.dao.recs.get("successful_request")
        nonexistant.data["ANTIGEN_FASTA_HASH"]["value"] = "some_nonexistant_hash"
        self.assertFalse(self.dao.study_has_been_run(nonexistant))
        unsuccessful = self.dao.recs.get("unsuccessful_request")
        self.assertFalse(self.dao.study_has_been_run(unsuccessful))

    def test_studies_have_been_run(self):
        """We should be able to check whether many studies at a time have been run."""
        successful = self.dao.recs.get("successful_request")
        # We fail fast on absent hashes
        nonexistant = self.dao.recs.get("successful_request")
        nonexistant.data["ANTIGEN_FASTA_HASH"]["value"] = "some_nonexistant_hash"
        nonexistant.id = "some_nonexistant_id"
        # And fail later on other, less selective criteria
        nonexistant_2 = self.dao.recs.get("successful_request")
        nonexistant_2.data["MASTER_ANTIGEN_FASTA_HASH"]["value"] = "some_nonexistant_master_hash"
        nonexistant_2.id = "some_other_nonexistant_id"
        exists = self.dao.study_has_been_run([successful, nonexistant, nonexistant_2])
        self.assertTrue(exists[0])
        self.assertFalse(exists[1])
        self.assertFalse(exists[2])

        # We should also be correctly "chunking" queries.
        large_list = [nonexistant] * 999
        large_list.append(successful)
        large_exists = self.dao.study_has_been_run(large_list)
        self.assertEqual(len(large_exists), 1000)
        self.assertFalse(large_exists[500])
        self.assertTrue(large_exists[-1])

    def test_studies_with_differing_chains_have_been_run(self):
        """
        We should be able to differentiate run and not-run by chain.

        We inherently check the many-many behavior here; that is, we have multiple
        pre-existing studies that match a given study on everything but chain,
        and also have proposed studies are identical on everything but chain.
        """
        nonexistant = self.dao.recs.get("successful_request")
        nonexistant.set_data("ANTIGEN_CHAINS_IN_STRUCT", ["a", "b", "c"])
        nonexistant.id = "proposed_c"
        exists = self.dao.study_has_been_run(nonexistant)
        self.assertFalse(exists)
        proposed_a = self.dao.recs.get("successful_request")
        proposed_a.set_data("ANTIGEN_CHAINS_IN_STRUCT", ["a"])
        proposed_a.set_data("study_type", "study_with_chain")
        proposed_a.id = "proposed_a"
        proposed_b = self.dao.recs.get("successful_request")
        proposed_b.set_data("ANTIGEN_CHAINS_IN_STRUCT", ["b"])
        proposed_b.set_data("study_type", "study_with_chain")
        proposed_b.id = "proposed_b"
        exists = self.dao.study_has_been_run([proposed_a, proposed_b, nonexistant])

        self.assertTrue(exists[0])
        self.assertTrue(exists[1])
        self.assertFalse(exists[2])

    def test_studies_with_agent_ids(self):
        """We should be able to optionally differentiate by agent."""
        duplicate = self.dao.recs.get("successful_request")
        self.assertTrue(self.dao.study_has_been_run(duplicate))
        self.assertFalse(self.dao.study_has_been_run(duplicate, "not_my_agent"))
        self.assertTrue(self.dao.study_has_been_run(duplicate, "my_agent"))

    def test_studies_with_equality_params(self):
        """
        We should be able to override the simple equality parameters.

        The "default" behavior is actually special (tests sequence hash, then fasta
        hash as a fallback). We can override that for ex: two-hash studies.
        """
        duplicate = self.dao.recs.get("successful_request")
        self.assertTrue(self.dao.study_has_been_run(duplicate))
        self.assertTrue(self.dao.study_has_been_run(duplicate, equality_params=["ANTIGEN_FASTA_HASH"]))
        self.assertTrue(self.dao.study_has_been_run(duplicate, equality_params=["favorite_color"]))
        self.assertTrue(self.dao.study_has_been_run(duplicate, equality_params=["ANTIGEN_FASTA_HASH",
                                                                                "favorite_color"]))
        almost_duplicate = self.dao.recs.get("successful_request")
        almost_duplicate.set_data("favorite_color", "blue")
        self.assertFalse(self.dao.study_has_been_run(almost_duplicate, equality_params=["favorite_color"]))
        self.assertFalse(self.dao.study_has_been_run(almost_duplicate, equality_params=["ANTIGEN_FASTA_HASH",
                                                                                        "favorite_color"]))

    def test_has_been_run_quitfast(self):
        """We should be able to quit out early if we get enough studies that haven't been run."""
        successful = self.dao.recs.get("successful_request")
        # We fail fast on absent hashes
        nonexistant = self.dao.recs.get("successful_request")
        nonexistant.id = "some_nonexistant_id"
        nonexistant.data["ANTIGEN_FASTA_HASH"]["value"] = "some_nonexistant_hash"
        nonexistant_too = self.dao.recs.get("successful_request")
        nonexistant_too.id = "another_nonexistant_id"
        nonexistant_too.set_data("ANTIGEN_CHAINS_IN_STRUCT", ["b", "c"])
        exists = self.dao.study_has_been_run([successful, nonexistant, nonexistant_too], quit_at=1)
        # Even though nonexistant_too doesn't exist, we expect a "True" to be returned for it here.
        # That's because we should have performed a quitfast once we'd confirmed "nonexistant"
        # doesn't exist, because we only need 1 study that hasn't been run yet
        self.assertTrue(exists[0])
        self.assertFalse(exists[1])
        self.assertTrue(exists[2])

        # However, if we go back and request more, it should be "False"
        exists = self.dao.study_has_been_run([successful, nonexistant, nonexistant_too], quit_at=2)
        self.assertTrue(exists[0])
        self.assertFalse(exists[1])
        self.assertFalse(exists[2])


    def test_study_exists(self):
        """We should be able to check whether something by some id exists."""
        self.assertTrue(self.dao.recs.exists("successful_request"))
        self.assertFalse(self.dao.recs.exists("nonexistant_request"))

    def test_request_conversion(self):
        """Requests should be converted to a Sina-queryable form."""
        raw_request = {
            "status": "RUNNING",
            "run_descriptors": {"study_parameters": {
                "ANTIGEN_CHAINS_IN_STRUCT": {"label": ["foo.1"], "values": ["A"]},
                "ANTIGEN_FASTA_HASH": {"label": ["bar.1"], "values": [["hash", "MD5"]]},
                "ANTIGEN_FASTA_PATH": {"label": ["baz.1"], "values": ["/path/egg.fasta"]},
                "MASTER_ANTIGEN_FASTA_HASH": {"label": ["qux.1"],
                                              "values": [["anotherhash", "MD5"]]},
                "MASTER_ANTIGEN_FASTA_PATH": {"label": ["quxx.1"], "values": ["/path/egg.fasta"]},
                "STRUCTURE_HASH": {"label": ["quxxx.1"],
                                   "values": [["anotherhash2electricboogaloo", "MD5_ALL"]]},
                "STRUCTURE_PATH": {"label": ["quxxxx.1"], "values": ["/pdb/final.pdb"]}},
                "study_type": "foldx_listed_mutants"}}
        # Not all are present, just standout examples
        expected_values = {"ANTIGEN_CHAINS_IN_STRUCT": {"value": ["A"]},
                           "ANTIGEN_FASTA_PATH": {"value": "/path/egg.fasta", "tags": ["delisted"]},
                           "ANTIGEN_FASTA_HASH": {"value": "hash", "tags": ["delisted", "MD5"]},
                           "study_type": {"value": "foldx_listed_mutants"},
                           "status": {"value": "RUNNING"}}
        transformed_request = sina_utils.DAO.create_request_from_data("blarp", raw_request)
        for key, expected_entry in expected_values.items():
            gotten_entry = transformed_request.data[key]
            self.assertEqual(expected_entry["value"], gotten_entry["value"])
            self.assertEqual(expected_entry.get("tags", []), gotten_entry.get("tags", []))

    def test_request_conversion_multisequence(self):
        """Multi-sequence requests should be converted to a Sina-queryable form."""
        raw_request = {
            "status": "RUNNING",
            "run_descriptors": {"study_parameters": {
                "ANTIGEN_CHAINS_IN_STRUCT": {"label": ["foo.1"], "values": ["A"]},
                "ANTIGEN_FASTA_HASH": {"label": ["bar.1"], "values": [["hash", "MD5"]]},
                "ANTIGEN_FASTA_HASH_2": {"label": ["bar.1"], "values": [["hash2", "MD5"]]},
                "ANTIGEN_FASTA_PATH": {"label": ["baz.1"], "values": ["/path/egg.fasta"]},
                "MASTER_ANTIGEN_FASTA_HASH": {"label": ["qux.1"],
                                              "values": [["anotherhash", "MD5"]]},
                "MASTER_ANTIGEN_FASTA_HASH_2": {"label": ["qux.1"],
                                                "values": [["anotherhash2", "MD5"]]},
                "MASTER_ANTIGEN_FASTA_PATH": {"label": ["quxx.1"], "values": ["/path/egg.fasta"]},
                "STRUCTURE_HASH": {"label": ["quxxx.1"],
                                   "values": [["anotherhash2electricboogaloo", "MD5_ALL"]]},
                "STRUCTURE_HASH_2": {"label": ["quxxx.1"],
                                     "values": [["anotherhash2electricboogaloo2", "MD5_ALL"]]},
                "STRUCTURE_PATH": {"label": ["quxxxx.1"], "values": ["/pdb/final.pdb"]}},
                "study_type": "foldx_listed_mutants"}}
        # Not all are present, just standout examples
        expected_values = {"ANTIGEN_CHAINS_IN_STRUCT": {"value": ["A"]},
                           "ANTIGEN_FASTA_HASH": {"value": "hash", "tags": ["delisted", "MD5"]},
                           "ANTIGEN_FASTA_HASH_2": {"value": "hash2", "tags": ["delisted", "MD5"]},
                           "STRUCTURE_HASH": {"value": "anotherhash2electricboogaloo",
                                              "tags": ["delisted", "MD5_ALL"]},
                           "STRUCTURE_HASH_2": {"value": "anotherhash2electricboogaloo2",
                                                "tags": ["delisted", "MD5_ALL"]},
                           "MASTER_ANTIGEN_FASTA_HASH": {"value": "anotherhash",
                                                         "tags": ["delisted", "MD5"]},
                           "MASTER_ANTIGEN_FASTA_HASH_2": {"value": "anotherhash2",
                                                           "tags": ["delisted", "MD5"]},
                           "study_type": {"value": "foldx_listed_mutants"},
                           "status": {"value": "RUNNING"}}
        transformed_request = sina_utils.DAO.create_request_from_data("blarp", raw_request)
        for key, expected_entry in expected_values.items():
            gotten_entry = transformed_request.data[key]
            self.assertEqual(expected_entry["value"], gotten_entry["value"])
            self.assertEqual(expected_entry.get("tags", []), gotten_entry.get("tags", []))

    def test_get_nearest_studies_by_sequence(self):
        """We should be able to retrieve the ids of results with nearby sequences."""
        test_sequence = "cat"
        distant_matches = self.dao.get_nearest_studies_by_sequence(test_sequence)
        self.assertListEqual(distant_matches, ["rat_result", "tar_result"])
        near_matches = self.dao.get_nearest_studies_by_sequence(test_sequence, max_distance=1)
        self.assertListEqual(near_matches, ["rat_result"])

    def test_get_studies_by_string_distance(self):
        """
        We should be able to retrieve the ids of results with nearby values for some string.

        Right now, this one mostly wraps the above, but it does have a special shortcut case
        to test.
        """
        test_sequence = "rat"
        exact_matches = self.dao.get_studies_by_string_distance("AntigenSequence",
                                                                test_sequence,
                                                                0)
        self.assertListEqual(exact_matches, ["rat_result"])


class testSinaAlteringOperations(TestAlteringDatabaseFunc):
    """
    Test db-altering Sina functionality.

    The database is recreated for every test, so if you need that functionality
    (create, update, delete), the test goes in here.
    """

    __test__ = True
