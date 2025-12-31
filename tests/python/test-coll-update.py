import os
import sys
import unittest
import json
import string
import random

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
from mochi.yokan.client import Exception
from mochi.yokan.client import Client
from mochi.yokan.server import Provider

class TestUpdate(unittest.TestCase):

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
            doc1_len = random.randint(16, 128)
            doc2_len = random.randint(16, 128)
            doc1 = ''.join(random.choice(letters) for i in range(doc1_len))
            doc2 = ''.join(random.choice(letters) for i in range(doc2_len))
            self.reference.append(doc1)
            self.coll.store(doc2)

    def tearDown(self):
        del self.coll
        del self.db
        del self.addr
        del self.client
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_update_strings(self):
        """Test that we can update string documents."""
        for i, doc in enumerate(self.reference):
            self.coll.update(id=i, document=doc)
        out_doc = bytearray(128)
        for i, doc in enumerate(self.reference):
            docsize = self.coll.load(id=i, buffer=out_doc)
            self.assertEqual(out_doc[0:docsize].decode("ascii"), doc)

        with self.assertRaises(Exception):
            self.coll.update(id=len(self.reference)+4, document="abcdef")

    def test_update_buffers(self):
        """Test that we can update buffer documents."""
        for i, doc in enumerate(self.reference):
            self.coll.update(id=i, document=bytearray(doc.encode('ascii')))
        out_doc = bytearray(128)
        for i, doc in enumerate(self.reference):
            docsize = self.coll.load(id=i, buffer=out_doc)
            self.assertEqual(out_doc[0:docsize].decode("ascii"), doc)

        with self.assertRaises(Exception):
            doc = b'abcdef'
            self.coll.update(id=len(self.reference)+4, document=doc)

    def test_update_multi_strings(self):
        """Test that we can use update_multi with strings."""
        ids = [ i for i in range(len(self.reference)) ]
        self.coll.update_multi(ids=ids, documents=self.reference)
        out_doc = bytearray(128)
        for i, doc in enumerate(self.reference):
            docsize = self.coll.load(id=i, buffer=out_doc)
            self.assertEqual(out_doc[0:docsize].decode("ascii"), doc)

    def test_update_multi_buffer(self):
        """Test that we can use update_multi with strings."""
        ids = [ i for i in range(len(self.reference)) ]
        docs = [ doc.encode('ascii') for doc in self.reference ]
        self.coll.update_multi(ids=ids, documents=docs)
        out_doc = bytearray(128)
        for i, doc in enumerate(self.reference):
            docsize = self.coll.load(id=i, buffer=out_doc)
            self.assertEqual(out_doc[0:docsize].decode("ascii"), doc)

    def test_update_packed(self):
        """Test that we can use update_packed with."""
        data = bytearray()
        ids = [ i for i in range(len(self.reference)) ]
        doc_lens = []
        for doc in self.reference:
            data += doc.encode('ascii')
            doc_lens.append(len(doc))
        self.coll.update_packed(ids=ids, documents=data, doc_sizes=doc_lens)
        out_doc = bytearray(128)
        for i, doc in enumerate(self.reference):
            docsize = self.coll.load(id=i, buffer=out_doc)
            self.assertEqual(out_doc[0:docsize].decode("ascii"), doc)

if __name__ == '__main__':
    unittest.main()
