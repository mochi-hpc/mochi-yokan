import os
import sys
import unittest
import json
import string
import random

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
import pyyokan_common as yokan
from pyyokan_admin import Admin
from pyyokan_client import Client, Database, Collection
from pyyokan_server import Provider

class TestCollection(unittest.TestCase):

    def setUp(self):
        self.engine = Engine('tcp')
        self.mid = self.engine.get_internal_mid()
        self.addr = self.engine.addr()
        self.hg_addr = self.addr.get_internal_hg_addr()
        self.provider_id = 42
        self.provider = Provider(mid=self.mid,
                                 provider_id=self.provider_id)
        admin = Admin(mid=self.mid)
        db_id = admin.open_database(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='', type='map', config='{}')
        self.client = Client(mid=self.mid)
        self.db = self.client.make_database_handle(
            address=self.hg_addr,
            provider_id=self.provider_id,
            database_id=db_id)

    def tearDown(self):
        del self.db
        del self.addr
        del self.hg_addr
        del self.client
        del self.mid
        del self.provider
        self.engine.finalize()

    def test_create_collection(self):
        """Test collection creation."""
        self.db.create_collection("matthieu")
        with self.assertRaises(yokan.Exception):
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
        self.assertIsInstance(self.db["matthieu"], Collection)
        self.db.create_collection("matthieu")
        self.assertIsInstance(self.db["matthieu"], Collection)
        self.db.drop_collection("matthieu")
        self.assertIsInstance(self.db["matthieu"], Collection)

if __name__ == '__main__':
    unittest.main()
