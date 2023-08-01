#!/usr/bin/env python
"""Run tests for select functions from "utils"."""

from sina.model import Record

import improvwf.db_interface.db_and_yaml as db_and_yaml
from tests.shared_db_test_code import TestAlteringDatabaseFunc


class testAlteringUtils(TestAlteringDatabaseFunc):
    """Tests db_and_yaml functions that alter the db."""

    __test__ = True

    def test_fail_running(self):
        """Test that we can update all RUNNING requests to be FAILED."""
        running_req = Record(id="was_running_request", type="request")
        running_else = Record(id="was_running_nonrequest", type="something")
        running_req.set_data("status", "RUNNING")
        running_else.set_data("status", "RUNNING")
        self.dao.recs.insert(running_req)
        self.dao.recs.insert(running_else)
        db_and_yaml._do_fail_running(self.dao)
        self.assertListEqual(list(self.dao.recs.data_query(status="RUNNING")),
                             ["was_running_nonrequest"])

    def test_empty_db(self):
        """Test that we remove ALL requests and results from the db."""
        self.assertTrue(len(list(self.dao.recs.get_all_of_type("request", ids_only=True))) > 0)
        self.assertTrue(len(list(self.dao.recs.get_all_of_type("request", ids_only=True))) > 0)
        db_and_yaml._do_empty_db(self.dao)
        self.assertEqual(list(self.dao.recs.get_all_of_type("request")), [])
        self.assertEqual(list(self.dao.recs.get_all_of_type("request")), [])

    # TODO: dump_db_to_file
    # TODO: load_file_to_db
    # TODO: setup_argparser
