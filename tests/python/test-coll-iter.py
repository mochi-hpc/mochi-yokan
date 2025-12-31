import os
import sys
import unittest
import json
import random
import string
from typing import Optional

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
from mochi.yokan.client import Client
from mochi.yokan.server import Provider

class TestCollIter(unittest.TestCase):

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
            self.coll.store(document=bytearray(doc.encode('ascii')))

    def tearDown(self):
        del self.coll
        del self.db
        del self.addr
        del self.client
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_iter(self):
        out_ids = []
        out_docs = []
        def callback(i: int, id: int, doc: memoryview):
            out_ids.append(id)
            out_docs.append(bytearray(doc).decode('ascii'))
        self.coll.iter(callback=callback)
        for i, doc_ref in enumerate(self.reference):
            self.assertEqual(i, out_ids[i])
            self.assertEqual(doc_ref, out_docs[i])

if __name__ == '__main__':
    unittest.main()
