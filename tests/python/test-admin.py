import os
import sys
import unittest
import json

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
from pyrkv_admin import Admin
from pyrkv_server import Provider

class TestAdmin(unittest.TestCase):

    def setUp(self):
        self.engine = Engine('tcp')
        self.mid = self.engine.get_internal_mid()
        self.addr = self.engine.addr()
        self.hg_addr = self.addr.get_internal_hg_addr()
        self.provider_id = 42
        self.provider = Provider(mid=self.mid,
                                 provider_id=self.provider_id)

    def tearDown(self):
        del self.addr
        del self.hg_addr
        del self.mid
        del self.provider
        self.engine.finalize()

    def test_init_admin(self):
        admin = Admin(mid=self.mid)

    def test_open_database(self):
        admin = Admin(mid=self.mid)
        admin.open_database(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='', type='map', config='{}')


if __name__ == '__main__':
    unittest.main()
