import os
import sys
import unittest
import json
import random
import string

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
from pyrkv_admin import Admin
from pyrkv_client import Client
from pyrkv_server import Provider

class TestErase(unittest.TestCase):

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
        del self.hg_addr
        del self.client
        del self.mid
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_erase(self):
        """Test that we can erase some keys."""
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

if __name__ == '__main__':
    unittest.main()
