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

class TestPutGet(unittest.TestCase):

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

    def tearDown(self):
        del self.db
        del self.addr
        del self.hg_addr
        del self.client
        del self.mid
        del self.provider
        self.engine.finalize()

    def test_put_get_strings(self):
        """Test that we can put string key/value pairs."""
        key = 'matthieu'
        in_val = 'dorier'
        self.db.put(key=key, value=in_val)

        out_val = bytearray(len(in_val))
        self.db.get(key=key, value=out_val)
        self.assertEquals(out_val.decode("utf-8"), in_val)

    def test_put_get_bytes(self):
        """Test that we can put string key/value pairs."""
        key = b'matthieu'
        in_val = b'dorier'
        self.db.put(key=key, value=in_val)

        out_val = bytearray(len(in_val))
        self.db.get(key=key, value=out_val)
        self.assertEquals(out_val.decode("utf-8"),
                          in_val.decode("utf-8"))

    def test_put_get_partial(self):
        """Test that we can put/get partial regions of bytearrays."""
        in_key = b'XXXmatthieuXXXX'
        in_val = b'XXXXXdorierXX'
        self.db.put(key=in_key[3:11], value=in_val[5:11])

        out_key = b'YYmatthieuYY'
        out_val = bytearray(len('dorier')+2)
        self.db.get(key=out_key[2:10], value=memoryview(out_val)[1:7])
        self.assertEquals(out_val[1:7].decode("utf-8"),
                in_val[5:11].decode("utf-8"))

if __name__ == '__main__':
    unittest.main()
