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

class TestStoreLoad(unittest.TestCase):

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

    def test_store_load_strings(self):
        """Test that we can store and load string documents."""
        for doc in self.reference:
            self.coll.store(document=doc)
        out_doc = bytearray(128)
        for i, doc in enumerate(self.reference):
            docsize = self.coll.load(id=i, buffer=out_doc)
            self.assertEqual(out_doc[0:docsize].decode("ascii"), doc)

        with self.assertRaises(yokan.Exception):
            self.coll.load(id=len(self.reference)+4, buffer=out_doc)

    def test_store_load_buffers(self):
        """Test that we can store and load buffer documents."""
        for doc in self.reference:
            self.coll.store(document=bytearray(doc.encode('ascii')))
        out_doc = bytearray(128)
        for i, doc in enumerate(self.reference):
            docsize = self.coll.load(id=i, buffer=out_doc)
            self.assertEqual(out_doc[0:docsize].decode("ascii"), doc)

        with self.assertRaises(yokan.Exception):
            self.coll.load(id=len(self.reference)+4, buffer=out_doc)

    def test_store_load_partial(self):
        """Test that we can store/load partial regions of bytearrays."""
        in_doc = b'XXXXXdorierXX'
        id = self.coll.store(document=in_doc[5:11])

        out_doc = bytearray(len('dorier')+2)
        self.coll.load(id=id, buffer=memoryview(out_doc)[1:7])
        self.assertEqual(out_doc[1:7].decode('ascii'),
                in_doc[5:11].decode('ascii'))

    def test_store_load_multi_strings(self):
        """Test that we can use store_multi and load_multi with strings."""
        self.coll.store_multi(documents=self.reference)

        ids = []
        out = []
        for k, doc in enumerate(self.reference):
            ids.append(k)
            out.append(bytearray(128))
        ids.append(len(self.reference)+4)
        out.append(bytearray(128))
        temp = list(zip(ids, out))
        random.shuffle(temp)
        ids, out = zip(*temp)

        docsizes = self.coll.load_multi(ids=ids, buffers=out)
        for i, (id, buf) in enumerate(zip(ids, out)):
            if id >= len(self.reference):
                self.assertIsNone(docsizes[i])
            else:
                self.assertEqual(buf[0:docsizes[i]].decode('ascii'),
                                 self.reference[id])

    def test_store_load_multi_buffers(self):
        """Test that we can use store_multi and load_multi with buffers."""
        self.coll.store_multi(documents=[ doc.encode('ascii') for doc in self.reference])

        ids = []
        out = []
        for k, doc in enumerate(self.reference):
            ids.append(k)
            out.append(bytearray(128))
        ids.append(len(self.reference)+4)
        out.append(bytearray(128))
        temp = list(zip(ids, out))
        random.shuffle(temp)
        ids, out = zip(*temp)

        docsizes = self.coll.load_multi(ids=ids, buffers=out)
        for i, (id, buf) in enumerate(zip(ids, out)):
            if id >= len(self.reference):
                self.assertIsNone(docsizes[i])
            else:
                self.assertEqual(buf[0:docsizes[i]].decode('ascii'),
                                 self.reference[id])

    def test_store_load_packed(self):
        """Test that store_packed and load_packed work."""
        in_docs_buf = bytearray()
        in_doc_sizes = []
        for doc in self.reference:
            in_docs_buf += bytearray(doc.encode('ascii'))
            in_doc_sizes.append(len(doc))

        ids = self.coll.store_packed(documents=in_docs_buf, doc_sizes=in_doc_sizes)

        ids.append(len(self.reference)+4)
        random.shuffle(ids)
        out_docs_buf = bytearray(128*len(ids))

        out_doc_sizes = self.coll.load_packed(ids=ids, buffer=out_docs_buf)
        offset = 0
        for i, id in enumerate(ids):
            if id >= len(self.reference):
                self.assertIsNone(out_doc_sizes[i])
                continue
            docsize = out_doc_sizes[i]
            self.assertEqual(out_docs_buf[offset:offset+docsize].decode('ascii'),
                             self.reference[id])
            offset += docsize

if __name__ == '__main__':
    unittest.main()
