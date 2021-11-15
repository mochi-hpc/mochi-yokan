import os
import sys
import unittest
import json
import random
import string

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
import pyyokan_common as yokan
from pyyokan_admin import Admin
from pyyokan_client import Client
from pyyokan_server import Provider

class TestCollErase(unittest.TestCase):

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
        self.coll = self.db.create_collection("matt")
        self.reference = list()
        letters = string.ascii_letters
        for i in range(0,64):
            doc_len = random.randint(16, 128)
            doc = ''.join(random.choice(letters) for i in range(doc_len))
            self.coll.store(document=doc)
            self.reference.append(doc)


    def tearDown(self):
        del self.coll
        del self.db
        del self.addr
        del self.hg_addr
        del self.client
        del self.mid
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_erase(self):
        """Test that we can erase some documents."""
        for i in range(len(self.reference)):
            if i % 2 == 0:
                self.coll.erase(id=i)
        for i in range(len(self.reference)):
            if i % 2 == 0:
                with self.assertRaises(yokan.Exception):
                    doc = bytearray(128)
                    self.coll.load(id=i, buffer=doc)
            else:
                doc = bytearray(128)
                self.coll.load(id=i, buffer=doc)

    def test_erase_multi(self):
        """Test that we can use erase_multi with string keys."""
        ids = []
        for i in range(len(self.reference)):
            if i % 2 == 0:
                ids.append(i)
        self.coll.erase_multi(ids=ids)
        for i in range(len(self.reference)):
            if i % 2 == 0:
                with self.assertRaises(yokan.Exception):
                    doc = bytearray(128)
                    self.coll.load(id=i, buffer=doc)
            else:
                doc = bytearray(128)
                self.coll.load(id=i, buffer=doc)

if __name__ == '__main__':
    unittest.main()
