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
from pyyokan_client import Client
from pyyokan_server import Provider

class TestDocList(unittest.TestCase):

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
            self.coll.store(doc)

    def tearDown(self):
        del self.coll
        del self.db
        del self.addr
        del self.client
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_list(self):
        """Test that we can list documents."""
        docs = []
        for i in range(10):
            docs.append(bytearray(128))

        i = 0;
        while i < len(self.reference):
            tmp = self.coll.list_docs(start_id=i, buffers=docs,
                    mode=yokan.YOKAN_MODE_INCLUSIVE)
            ids, sizes = zip(*tmp)
            for j in range(len(ids)):
                self.assertEqual(ids[j], j+i)
                self.assertEqual(sizes[j], len(self.reference[j+i]))
                self.assertEqual(docs[j][0:sizes[j]].decode('ascii'), self.reference[j+i])
            i += 10

    def test_list_packed(self):
        """Test that we can list documents."""
        docs = bytearray(128*10)

        i = 0;
        while i < len(self.reference):
            tmp = self.coll.list_docs_packed(start_id=i, buffer=docs, count=10,
                    mode=yokan.YOKAN_MODE_INCLUSIVE)
            ids, sizes = zip(*tmp)
            offset = 0
            for j in range(len(ids)):
                self.assertEqual(ids[j], j+i)
                self.assertEqual(sizes[j], len(self.reference[j+i]))
                doc = docs[offset:offset+sizes[j]]
                self.assertEqual(doc.decode('ascii'), self.reference[j+i])
                offset += sizes[j]
            i += 10


if __name__ == '__main__':
    unittest.main()
