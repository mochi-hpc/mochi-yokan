import os
import sys
import unittest
import random
import string

wd = os.getcwd()
sys.path.append(wd+'/../python')

from mochi.margo import Engine
from mochi.yokan.client import Client
from mochi.yokan.server import Provider


class TestEraseRange(unittest.TestCase):

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
        letters = string.ascii_letters
        self.reference = dict()
        for i in range(0, 32):
            prefix = 'AAA-' if i % 2 == 0 else 'BBB-'
            key_body = ''.join(random.choice(letters) for _ in range(random.randint(8, 32)))
            key = prefix + key_body
            val = ''.join(random.choice(letters) for _ in range(random.randint(16, 64)))
            self.db.put(key=key, value=val)
            self.reference[key] = val

    def tearDown(self):
        del self.db
        del self.addr
        del self.client
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_erase_range_string_prefix(self):
        """erase_range with a string prefix removes only keys with that prefix."""
        for key in self.reference:
            self.assertTrue(self.db.exists(key))
        self.db.erase_range('AAA-')
        for key in self.reference:
            if key.startswith('AAA-'):
                self.assertFalse(self.db.exists(key))
            else:
                self.assertTrue(self.db.exists(key))

    def test_erase_range_buffer_prefix(self):
        """erase_range accepts a bytes/buffer prefix."""
        for key in self.reference:
            self.assertTrue(self.db.exists(key))
        self.db.erase_range(b'BBB-')
        for key in self.reference:
            if key.startswith('BBB-'):
                self.assertFalse(self.db.exists(key))
            else:
                self.assertTrue(self.db.exists(key))

    def test_erase_range_empty_prefix(self):
        """An empty prefix clears the entire database."""
        for key in self.reference:
            self.assertTrue(self.db.exists(key))
        self.db.erase_range('')
        for key in self.reference:
            self.assertFalse(self.db.exists(key))

    def test_erase_range_no_match(self):
        """A prefix matching nothing leaves the database untouched."""
        self.db.erase_range('ZZZ-')
        for key in self.reference:
            self.assertTrue(self.db.exists(key))


if __name__ == '__main__':
    unittest.main()
