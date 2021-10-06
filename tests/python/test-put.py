import os
import sys
import unittest
import json

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
from pyrkv_admin import Admin
from pyrkv_client import Client
from pyrkv_server import Provider

class TestPut(unittest.TestCase):

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
        client = Client(mid=self.mid)
        self.db = client.make_database_handle(
            address=self.hg_addr,
            provider_id=self.provider_id,
            database_id=db_id)

    def tearDown(self):
        del self.db
        del self.addr
        del self.hg_addr
        del self.mid
        del self.provider
        self.engine.finalize()

    def test_put_strings(self):
        k = 'matthieu'
        v = 'dorier'
        self.db.put(key=k, value=v)

    def test_put_bytearray(self):
        k = bytearray(b'matthieu')
        v = bytearray(b'dorier')
        self.db.put(key=k, value=v)
        k = 'phil'
        v = bytearray(b'carns')
        self.db.put(key=k, value=v)

if __name__ == '__main__':
    unittest.main()
