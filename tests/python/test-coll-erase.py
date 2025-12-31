import os
import sys
import unittest
import json
import random
import string

wd = os.getcwd()
sys.path.append(wd+'/../python')
print(sys.path)

from mochi.margo import Engine
from mochi.yokan.client import Exception
from mochi.yokan.client import Client
from mochi.yokan.server import Provider

class TestCollErase(unittest.TestCase):

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
        del self.client
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
                with self.assertRaises(Exception):
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
                with self.assertRaises(Exception):
                    doc = bytearray(128)
                    self.coll.load(id=i, buffer=doc)
            else:
                doc = bytearray(128)
                self.coll.load(id=i, buffer=doc)

if __name__ == '__main__':
    unittest.main()
