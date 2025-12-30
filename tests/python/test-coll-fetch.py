import os
import sys
import unittest
import json
import string
import random
from typing import Optional

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
import pyyokan_common as yokan
from pyyokan_client import Client
from pyyokan_server import Provider

class TestStoreFetch(unittest.TestCase):

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
        self.coll = self.db.create_collection(
            name="matt")
        self.reference = list()
        letters = string.ascii_letters
        for i in range(0,64):
            doc_len = random.randint(16, 128)
            doc = ''.join(random.choice(letters) for i in range(doc_len))
            self.reference.append(doc)

    def tearDown(self):
        del self.coll
        del self.db
        del self.addr
        del self.client
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_store_fetch(self):
        """Test that we can store and fetch documents."""
        for doc in self.reference:
            self.coll.store(document=bytearray(doc.encode('ascii')))
        for i, ref in enumerate(self.reference):
            def callback(i: int, id: int, doc: Optional[memoryview]):
                self.assertEqual(bytearray(doc).decode("ascii"), ref)
            self.coll.fetch(id=i, callback=callback)

        def callback(i: int, id: int, doc: Optional[memoryview]):
            self.assertIsNone(doc)
        self.coll.fetch(id=len(self.reference)+4, callback=callback)

    def test_store_fetch_multi(self):
        """Test that we can use store_multi and fetch_multi."""
        self.coll.store_multi(documents=[ doc.encode('ascii') for doc in self.reference])

        ids = []
        for k in range(0, len(self.reference)):
            ids.append(k)
        ids.append(len(self.reference)+4)
        random.shuffle(ids)

        out = {}

        def callback(i: int, id: int, doc: Optional[memoryview]):
            if doc is None:
                out[id] = None
            else:
                out[id] = bytearray(doc).decode('ascii')

        self.coll.fetch_multi(ids=ids, callback=callback)

        for (id, doc) in out.items():
            if id >= len(self.reference):
                self.assertIsNone(doc)
            else:
                self.assertEqual(doc, self.reference[id])


if __name__ == '__main__':
    unittest.main()
