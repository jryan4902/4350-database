"""
test_kvstore.py - Unit tests for the key-value store.

Tests cover:
  - LinkedListIndex set/get semantics
  - Log persistence and replay
  - CLI command parsing
"""

import os
import sys
import tempfile
import unittest

# Make kvstore importable from this directory
sys.path.insert(0, os.path.dirname(__file__))

from kvstore import (
    LinkedListIndex,
    append_to_log,
    load_from_log,
    parse_and_dispatch,
)


class TestLinkedListIndex(unittest.TestCase):

    def setUp(self):
        self.idx = LinkedListIndex()

    def test_get_missing_key_returns_none(self):
        self.assertIsNone(self.idx.get("missing"))

    def test_set_and_get_single_key(self):
        self.idx.set("foo", "bar")
        self.assertEqual(self.idx.get("foo"), "bar")

    def test_last_write_wins(self):
        self.idx.set("k", "v1")
        self.idx.set("k", "v2")
        self.assertEqual(self.idx.get("k"), "v2")

    def test_multiple_keys(self):
        self.idx.set("a", "1")
        self.idx.set("b", "2")
        self.idx.set("c", "3")
        self.assertEqual(self.idx.get("a"), "1")
        self.assertEqual(self.idx.get("b"), "2")
        self.assertEqual(self.idx.get("c"), "3")

    def test_overwrite_does_not_duplicate(self):
        for i in range(5):
            self.idx.set("x", str(i))
        count = 0
        node = self.idx.head
        while node:
            if node.key == "x":
                count += 1
            node = node.next
        self.assertEqual(count, 1)


class TestPersistence(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.db_path = self.tmp.name
        self.tmp.close()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_append_and_reload(self):
        append_to_log("hello", "world", self.db_path)
        idx = LinkedListIndex()
        load_from_log(idx, self.db_path)
        self.assertEqual(idx.get("hello"), "world")

    def test_last_write_wins_after_reload(self):
        append_to_log("k", "first", self.db_path)
        append_to_log("k", "second", self.db_path)
        idx = LinkedListIndex()
        load_from_log(idx, self.db_path)
        self.assertEqual(idx.get("k"), "second")

    def test_missing_db_file_is_handled(self):
        idx = LinkedListIndex()
        load_from_log(idx, "/tmp/__no_such_file__.db")
        self.assertIsNone(idx.get("anything"))


class TestCommandParsing(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.db_path = self.tmp.name
        self.tmp.close()
        import kvstore
        self._orig = kvstore.DB_FILE
        kvstore.DB_FILE = self.db_path
        self.idx = LinkedListIndex()

    def tearDown(self):
        import kvstore
        kvstore.DB_FILE = self._orig
        os.unlink(self.db_path)

    def test_set_returns_ok(self):
        self.assertEqual(parse_and_dispatch("SET name Alice", self.idx), "OK")

    def test_get_returns_value(self):
        parse_and_dispatch("SET name Alice", self.idx)
        self.assertEqual(parse_and_dispatch("GET name", self.idx), "Alice")

    def test_get_missing_returns_empty(self):
        self.assertEqual(parse_and_dispatch("GET nope", self.idx), "")

    def test_exit_returns_none(self):
        self.assertIsNone(parse_and_dispatch("EXIT", self.idx))

    def test_case_insensitive_commands(self):
        self.assertEqual(parse_and_dispatch("set x 42", self.idx), "OK")
        self.assertEqual(parse_and_dispatch("get x", self.idx), "42")

    def test_value_with_spaces(self):
        parse_and_dispatch("SET greeting hello world", self.idx)
        self.assertEqual(parse_and_dispatch("GET greeting", self.idx), "hello world")

    def test_unknown_command(self):
        resp = parse_and_dispatch("DELETE foo", self.idx)
        self.assertIn("ERROR", resp)

    def test_set_missing_value_errors(self):
        resp = parse_and_dispatch("SET onlykey", self.idx)
        self.assertIn("ERROR", resp)


if __name__ == "__main__":
    unittest.main(verbosity=2)
