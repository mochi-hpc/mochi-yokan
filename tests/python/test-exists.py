import os
import sys
import unittest
import json
import random
import string

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
from pyyokan_admin import Admin
from pyyokan_client import Client
from pyyokan_server import Provider

class TestExists(unittest.TestCase):

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
        self.reference_true = dict()
        self.reference_false = dict()
        letters = string.ascii_letters
        for i in range(0,8):
            key_len = random.randint(8, 64)
            val_len = random.randint(16, 128)
            key = ''.join(random.choice(letters) for i in range(key_len))
            val = ''.join(random.choice(letters) for i in range(val_len))
            self.db.put(key=key, value=val)
            self.reference_true[key] = val
        for i in range(0,8):
            key_len = random.randint(8, 64)
            val_len = random.randint(16, 128)
            key = ''.join(random.choice(letters) for i in range(key_len))
            val = ''.join(random.choice(letters) for i in range(val_len))
            self.reference_false[key] = val


    def tearDown(self):
        del self.db
        del self.addr
        del self.hg_addr
        del self.client
        del self.mid
        del self.provider
        del self.reference_true
        del self.reference_false
        self.engine.finalize()

    def test_exists_string(self):
        """Test that we can check that the keys put do exist."""
        for key in self.reference_true:
            self.assertTrue(self.db.exists(key))

    def test_no_exists_string(self):
        """Test that we can check that the keys not put do not exist."""
        for key in self.reference_false:
            self.assertFalse(self.db.exists(key))

    def test_exists_buffer(self):
        """Test that we can check that the keys put do exist."""
        for key in self.reference_true:
            key_buf = key.encode('ascii')
            self.assertTrue(self.db.exists(key_buf))

    def test_no_exists_buffer(self):
        """Test that we can check that the keys not put do not exist."""
        for key in self.reference_false:
            key_buf = key.encode('ascii')
            self.assertFalse(self.db.exists(key_buf))

    def test_exists_multi_string(self):
        """Test that we can use exists_multi with string keys."""
        keys = list(self.reference_false.keys()) + list(self.reference_true.keys())
        random.shuffle(keys)
        exists = self.db.exists_multi(keys)
        for i, key in enumerate(keys):
            if key in self.reference_true:
                self.assertTrue(exists[i])
            else:
                self.assertFalse(exists[i])

    def test_exist_multi_buffer(self):
        """Test that we can use exists_multi with buffer keys."""
        keys = list(self.reference_false.keys()) + list(self.reference_true.keys())
        random.shuffle(keys)
        keys_buf = [ bytearray(k.encode('ascii')) for k in keys ]
        exists = self.db.exists_multi(keys_buf)
        for i, key in enumerate(keys):
            if key in self.reference_true:
                self.assertTrue(exists[i])
            else:
                self.assertFalse(exists[i])

    def test_exist_packed(self):
        """Test that we can use exists_packed."""
        keys = list(self.reference_false.keys()) + list(self.reference_true.keys())
        random.shuffle(keys)
        keys_buf = bytearray()
        key_sizes = []
        for k in keys:
            keys_buf += k.encode('ascii')
            key_sizes.append(len(k))
        exists = self.db.exists_packed(keys_buf, key_sizes)
        for i, key in enumerate(keys):
            if key in self.reference_true:
                self.assertTrue(exists[i])
            else:
                self.assertFalse(exists[i])


if __name__ == '__main__':
    unittest.main()
