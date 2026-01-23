import os
import sys
import unittest
import json
import string
import random

wd = os.getcwd()
sys.path.append(wd+'/../python')

from mochi.margo import Engine
from mochi.yokan.client import Exception
from mochi.yokan.client import Client, Database, Collection
from mochi.yokan.server import Provider

class TestCollection(unittest.TestCase):

    def setUp(self):
        self.engine = Engine('tcp')
        self.addr = self.engine.addr()
        self.provider_id = 42
        self.provider = Provider(engine=self.engine,
                                 provider_id=self.provider_id,
                                 config='{"database":{"type":"map"}}')
        self.client = Client(engine=self.engine)
        self.db = self.client.make_database_handle(
            address=self.addr,
            provider_id=self.provider_id)

    def tearDown(self):
        del self.db
        del self.addr
        del self.client
        del self.provider
        self.engine.finalize()

    def test_create_collection(self):
        """Test collection creation."""
        self.db.create_collection("matthieu")
        with self.assertRaises(Exception):
            self.db.create_collection("matthieu")
        self.db.create_collection("phil")

    def test_collection_exists(self):
        """Test the collection_exists function."""
        self.assertFalse(self.db.collection_exists("matthieu"))
        self.db.create_collection("matthieu")
        self.assertTrue(self.db.collection_exists("matthieu"))

    def test_drop_collection(self):
        """Test the drop_collection function."""
        self.db.create_collection("matthieu")
        self.assertTrue(self.db.collection_exists("matthieu"))
        self.db.drop_collection("matthieu")
        self.assertFalse(self.db.collection_exists("matthieu"))

    def test_getitem(self):
        """Test the __getitem__ method of database objects."""
        self.assertIsInstance(self.db.open_collection("matthieu"), Collection)
        self.db.create_collection("matthieu")
        self.assertIsInstance(self.db.open_collection("matthieu"), Collection)
        self.db.drop_collection("matthieu")
        self.assertIsInstance(self.db.open_collection("matthieu"), Collection)

if __name__ == '__main__':
    unittest.main()
