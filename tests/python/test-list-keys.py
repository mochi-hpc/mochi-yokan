import os
import sys
import unittest
import json
import random
import string

wd = os.getcwd()
sys.path.append(wd+'/../python')

from mochi.margo import Engine
from mochi.yokan.client import Client
from mochi.yokan.server import Provider

class TestListKeys(unittest.TestCase):

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
        del self.client
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_list_keys(self):
        out_keys = []
        for i in range(0, 4):
            out_keys.append(bytearray(64+len(self.prefix)+1))
        from_key = ''
        for prefix in ['', self.prefix]:
            while True:
                ksizes = self.db.list_keys(keys=out_keys,
                                           from_key=from_key,
                                           filter=prefix)
                for i in range(0, len(ksizes)):
                    self.assertNotEqual(ksizes[i], -1)
                    key = out_keys[i][0:ksizes[i]].decode('ascii')
                    from_key = key
                    self.assertIn(key, self.reference)
                    self.assertTrue(key.startswith(prefix))
                if len(ksizes) != 4:
                    break

    def test_list_keys_packed(self):
        out_keys = bytearray(4*(64+len(self.prefix)))
        from_key = ''
        for prefix in ['', self.prefix]:
            while True:
                ksizes = self.db.list_keys_packed(keys=out_keys,
                                                  count=4,
                                                  from_key=from_key,
                                                  filter=prefix)
                koffset = 0
                for i in range(0, len(ksizes)):
                    self.assertNotEqual(ksizes[i], -1)
                    key = out_keys[koffset:koffset+ksizes[i]].decode('ascii')
                    from_key = key
                    self.assertIn(key, self.reference)
                    self.assertTrue(key.startswith(prefix))
                    koffset += ksizes[i]
                if len(ksizes) != 4:
                    break

if __name__ == '__main__':
    unittest.main()
