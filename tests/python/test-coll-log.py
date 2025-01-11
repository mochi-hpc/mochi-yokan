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

class TestUpdate(unittest.TestCase):

    def setUp(self):
        self.engine = Engine('tcp')
        self.mid = self.engine.get_internal_mid()
        self.addr = self.engine.addr()
        self.hg_addr = self.addr.get_internal_hg_addr()
        self.provider_id = 42
        config = {
            "database": {
                "type": "log",
                "config": {
                    "path": "/tmp/py-test-coll-log",
                    "chunk_size": 16384,
                    "cache_size": 4
                }
            }
        }
        self.provider = Provider(mid=self.mid,
                                 provider_id=self.provider_id,
                                 config=json.dumps(config))
        self.client = Client(mid=self.mid)
        self.db = self.client.make_database_handle(
            address=self.hg_addr,
            provider_id=self.provider_id)
        self.coll = self.db.create_collection(
            name="matt")

    def tearDown(self):
        self.db.drop_collection(name="matt")
        del self.coll
        del self.db
        del self.addr
        del self.hg_addr
        del self.client
        del self.mid
        del self.provider
        self.engine.finalize()

    def test_log(self):
        """Test that we can update string documents."""
        reference = list()
        letters = string.ascii_letters
        # create an store a bunch of documents
        # (4096 documents of average size 56 characters = 229376 bytes on average,
        # with a chunk size of 16384, we will need 14 chunks on average
        for i in range(0, 4096):
            doc_len = random.randint(16, 128)
            doc = ''.join(random.choice(letters) for i in range(doc_len))
            reference.append(doc)
            self.coll.store(doc)
        # check (in order) that they have been stored correctly
        out_doc = bytearray(128)
        for i, doc in enumerate(reference):
            doc_len = self.coll.load(id=i, buffer=out_doc)
            self.assertEqual(out_doc[0:doc_len].decode("ascii"), doc)
        # check some out of order to exercise the cache
        for j in range(100):
            i = random.randint(0, 4095)
            doc_len = self.coll.load(id=i, buffer=out_doc)
            self.assertEqual(out_doc[0:doc_len].decode("ascii"), reference[i])
        # do some updates. Some of them are bound to create new chunks,
        # some will update in place
        for i in range(0, 256):
            doc_len = random.randint(16, 128)
            doc = ''.join(random.choice(letters) for i in range(doc_len))
            reference[i] = doc
            self.coll.update(id=i, document=doc)
        # check again all the documents against the reference
        for i, doc in enumerate(reference):
            doc_len = self.coll.load(id=i, buffer=out_doc)
            self.assertEqual(out_doc[0:doc_len].decode("ascii"), doc)


if __name__ == '__main__':
    unittest.main()
