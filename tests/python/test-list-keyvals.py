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

class TestListKeyVals(unittest.TestCase):

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

    def test_list_keyvals(self):
        out_items = []
        for i in range(0, 4):
            out_items.append((bytearray(64+len(self.prefix)+1), bytearray(128)))
        from_key = ''
        for prefix in ['', self.prefix]:
            while True:
                sizes = self.db.list_keyvals(pairs=out_items,
                                             from_key=from_key,
                                             filter=prefix)
                for i in range(0, len(sizes)):
                    ksize, vsize = sizes[i]
                    self.assertNotEqual(ksize, -1)
                    self.assertNotEqual(vsize, -1)
                    key = out_items[i][0][0:ksize].decode('ascii')
                    val = out_items[i][1][0:vsize].decode('ascii')
                    from_key = key
                    self.assertIn(key, self.reference)
                    self.assertTrue(key.startswith(prefix))
                    self.assertEqual(val, self.reference[key])
                if len(sizes) != 4:
                    break

    def test_list_keyvals_packed(self):
        out_keys = bytearray(4*(64+len(self.prefix)))
        out_vals = bytearray(4*128)
        from_key = ''
        for prefix in ['', self.prefix]:
            while True:
                sizes = self.db.list_keyvals_packed(keys=out_keys,
                                                    values=out_vals,
                                                    count=4,
                                                    from_key=from_key,
                                                    filter=prefix)
                koffset = 0
                voffset = 0
                for i in range(0, len(sizes)):
                    ksize, vsize = sizes[i]
                    self.assertNotEqual(ksize, -1)
                    self.assertNotEqual(vsize, -1)
                    key = out_keys[koffset:koffset+ksize].decode('ascii')
                    val = out_vals[voffset:voffset+vsize].decode('ascii')
                    from_key = key
                    self.assertIn(key, self.reference)
                    self.assertTrue(key.startswith(prefix))
                    self.assertEqual(val, self.reference[key])
                    koffset += ksize
                    voffset += vsize
                if len(sizes) != 4:
                    break

if __name__ == '__main__':
    unittest.main()
