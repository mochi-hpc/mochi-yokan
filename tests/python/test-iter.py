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
from pyyokan_admin import Admin
from pyyokan_client import Client
from pyyokan_server import Provider

class TestIter(unittest.TestCase):

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
        self.prefix = 'matt'
        letters = string.ascii_letters
        for i in range(0,32):
            key_len = random.randint(4, 64)
            val_len = random.randint(16, 128)
            key = ''.join(random.choice(letters) for i in range(key_len))
            val = ''.join(random.choice(letters) for i in range(val_len))
            if i % 2 == 0:
                key = self.prefix + key
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

    def test_iter(self):
        def callback(i: int, key: str, val: Optional[memoryview]):
            out_keys.append(key)
            if val is None:
                out_vals.append(val)
            else:
                out_vals.append(bytearray(val).decode('ascii'))
        for prefix in ['', self.prefix]:
            out_keys = []
            out_vals = []
            self.db.iter(callback=callback, from_key='', filter=prefix)
            i = 0
            for k_ref in sorted(self.reference.keys()):
                if not k_ref.startswith(prefix):
                    continue
                v_ref = self.reference[k_ref]
                self.assertEqual(k_ref, out_keys[i])
                self.assertEqual(v_ref, out_vals[i])
                i += 1


if __name__ == '__main__':
    unittest.main()
