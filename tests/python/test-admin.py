import os
import sys
import unittest
import json

wd = os.getcwd()
sys.path.append(wd+'/../python')

from pymargo.core import Engine
from pyyokan_admin import Admin
from pyyokan_server import Provider

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
        """Test that we can create an Admin object."""
        admin = Admin(mid=self.mid)

    def test_open_database(self):
        """Test that we can open a database on the provider."""
        admin = Admin(mid=self.mid)
        admin.open_database(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='', type='map', config='{}')

    def test_list_databases(self):
        """Test that we can list databases."""
        admin = Admin(mid=self.mid)
        db_ids = []
        for i in range(0,10):
            db_id = admin.open_database(
                address=self.hg_addr,
                provider_id=self.provider_id,
                token='', type='map', config='{}')
            db_ids.append(db_id)
        listed_ids = admin.list_databases(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='')
        self.assertEqual(set(db_ids), set(listed_ids))

    def test_close_database(self):
        """Test that we can close a database."""
        admin = Admin(mid=self.mid)
        db_id1 = admin.open_database(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='', type='map', config='{}')
        db_id2 = admin.open_database(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='', type='map', config='{}')
        listed_ids = admin.list_databases(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='')
        self.assertIn(db_id1, listed_ids)
        self.assertIn(db_id2, listed_ids)
        admin.close_database(
            address=self.hg_addr,
            provider_id=self.provider_id,
            database_id=db_id1,
            token='')
        listed_ids = admin.list_databases(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='')
        self.assertNotIn(db_id1, listed_ids)
        self.assertIn(db_id2, listed_ids)

    def test_destroy_database(self):
        """Test that we can destroy a database."""
        admin = Admin(mid=self.mid)
        db_id1 = admin.open_database(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='', type='map', config='{}')
        db_id2 = admin.open_database(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='', type='map', config='{}')
        listed_ids = admin.list_databases(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='')
        self.assertIn(db_id1, listed_ids)
        self.assertIn(db_id2, listed_ids)
        admin.destroy_database(
            address=self.hg_addr,
            provider_id=self.provider_id,
            database_id=db_id1,
            token='')
        listed_ids = admin.list_databases(
            address=self.hg_addr,
            provider_id=self.provider_id,
            token='')
        self.assertNotIn(db_id1, listed_ids)
        self.assertIn(db_id2, listed_ids)


if __name__ == '__main__':
    unittest.main()
