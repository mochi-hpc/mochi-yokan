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
from pyyokan_client import Client
from pyyokan_server import Provider

class TestCollIter(unittest.TestCase):

    def setUp(self):
        self.engine = Engine('tcp')
        self.mid = self.engine.get_internal_mid()
        self.addr = self.engine.addr()
        self.hg_addr = self.addr.get_internal_hg_addr()
        self.provider_id = 42
        self.provider = Provider(mid=self.mid,
                                 provider_id=self.provider_id,
                                 config='{"database":{"type":"map"}}')
        self.client = Client(mid=self.mid)
        self.db = self.client.make_database_handle(
            address=self.hg_addr,
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
        del self.hg_addr
        del self.client
        del self.mid
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
