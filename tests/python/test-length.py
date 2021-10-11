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

class TestLength(unittest.TestCase):

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
        self.reference = dict()
        letters = string.ascii_letters
        for i in range(0,8):
            key_len = random.randint(8, 64)
            val_len = random.randint(16, 128)
            key = ''.join(random.choice(letters) for i in range(key_len))
            val = ''.join(random.choice(letters) for i in range(val_len))
            self.db.put(key=key, value=val)
            self.reference[key] = val


    def tearDown(self):
        del self.db
        del self.addr
        del self.hg_addr
        del self.client
        del self.mid
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_length_string(self):
        """Test that we can check that the keys put do have the correct length."""
        for key, val in self.reference.items():
            self.assertEqual(self.db.length(key), len(val))
        with self.assertRaises(yokan.Exception):
            self.db.length('xxxxx')

    def test_length_buffer(self):
        """Test that we can check that the keys put do have the correct length."""
        for key, val in self.reference.items():
            self.assertEqual(self.db.length(bytearray(key.encode('ascii'))), len(val))
        with self.assertRaises(yokan.Exception):
            self.db.length(bytearray('xxxxx'.encode('ascii')))

    def test_length_multi_string(self):
        """Test that we can use length_multi with string keys."""
        keys = list(self.reference.keys())
        keys.append('xxxxxxxx')
        random.shuffle(keys)
        sizes = self.db.length_multi(keys)
        for i, key in enumerate(keys):
            if key == 'xxxxxxxx':
                self.assertIsNone(sizes[i])
            else:
                self.assertEqual(sizes[i], len(self.reference[key]))

    def test_length_multi_buffer(self):
        """Test that we can use length_multi with buffer keys."""
        keys = list(self.reference.keys())
        keys.append('xxxxxxxx')
        random.shuffle(keys)
        keys_buf = [ bytearray(k.encode('ascii')) for k in keys ]
        sizes = self.db.length_multi(keys_buf)
        for i, key in enumerate(keys):
            if key == 'xxxxxxxx':
                self.assertIsNone(sizes[i])
            else:
                self.assertEqual(sizes[i], len(self.reference[key]))

    def test_length_packed(self):
        """Test that we can use length_packed."""
        keys = list(self.reference.keys())
        keys.append('xxxxxxxx')
        random.shuffle(keys)
        keys_buf = bytearray()
        key_sizes = []
        for key in keys:
            keys_buf += key.encode('ascii')
            key_sizes.append(len(key))
        sizes = self.db.length_packed(keys_buf, key_sizes)
        for i, key in enumerate(keys):
            if key == 'xxxxxxxx':
                self.assertIsNone(sizes[i])
            else:
                self.assertEqual(sizes[i], len(self.reference[key]))

if __name__ == '__main__':
    unittest.main()
