import os
import sys
import unittest
import json
import random
import string

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
from mochi.yokan.client import Client
from mochi.yokan.server import Provider

class TestErase(unittest.TestCase):

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
        self.reference = dict()
        letters = string.ascii_letters
        for i in range(0,16):
            key_len = random.randint(8, 64)
            val_len = random.randint(16, 128)
            key = ''.join(random.choice(letters) for i in range(key_len))
            val = ''.join(random.choice(letters) for i in range(val_len))
            self.db.put(key=key, value=val)
            self.reference[key] = val


    def tearDown(self):
        del self.db
        del self.addr
        del self.client
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_erase_string(self):
        """Test that we can erase some string keys."""
        for key in self.reference:
            self.assertTrue(self.db.exists(key))
        for i, key in enumerate(self.reference):
            if i % 2 == 0:
                self.db.erase(key)
        for i, key in enumerate(self.reference):
            if i % 2 == 0:
                self.assertFalse(self.db.exists(key))
            else:
                self.assertTrue(self.db.exists(key))

    def test_erase_buffer(self):
        """Test that we can erase some buffer keys."""
        for key in self.reference:
            self.assertTrue(self.db.exists(key))
        for i, key in enumerate(self.reference):
            if i % 2 == 0:
                key_buf = key.encode('ascii')
                self.db.erase(key_buf)
        for i, key in enumerate(self.reference):
            if i % 2 == 0:
                self.assertFalse(self.db.exists(key))
            else:
                self.assertTrue(self.db.exists(key))

    def test_erase_multi_string(self):
        """Test that we can use erase_multi with string keys."""
        for key in self.reference:
            self.assertTrue(self.db.exists(key))
        keys = []
        for i, key in enumerate(self.reference):
            if i % 2 == 0:
                keys.append(key)
        self.db.erase_multi(keys)
        for i, key in enumerate(self.reference):
            if i % 2 == 0:
                self.assertFalse(self.db.exists(key))
            else:
                self.assertTrue(self.db.exists(key))

    def test_erase_multi_buffer(self):
        """Test that we can use erase_multi with string keys."""
        for key in self.reference:
            self.assertTrue(self.db.exists(key))
        keys = []
        for i, key in enumerate(self.reference):
            if i % 2 == 0:
                keys.append(key.encode('ascii'))
        self.db.erase_multi(keys)
        for i, key in enumerate(self.reference):
            if i % 2 == 0:
                self.assertFalse(self.db.exists(key))
            else:
                self.assertTrue(self.db.exists(key))

    def test_erase_packed(self):
        """Test that we can use erase_multi with string keys."""
        for key in self.reference:
            self.assertTrue(self.db.exists(key))
        keys = bytearray()
        ksizes = []
        for i, key in enumerate(self.reference):
            if i % 2 == 0:
                keys += key.encode('ascii')
                ksizes.append(len(key))
        self.db.erase_packed(keys, ksizes)
        for i, key in enumerate(self.reference):
            if i % 2 == 0:
                self.assertFalse(self.db.exists(key))
            else:
                self.assertTrue(self.db.exists(key))


if __name__ == '__main__':
    unittest.main()
