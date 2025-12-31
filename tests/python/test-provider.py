import os
import sys
import unittest

wd = os.getcwd()
sys.path.append(wd+'/../python')

from mochi.margo import Engine
from mochi.yokan.server import Exception
from mochi.yokan.server import Provider

class TestInitProvider(unittest.TestCase):

    def test_init_provider_with_config(self):
        """Tests that we can initialize a provider
        with a config."""
        engine = Engine('tcp')
        self.assertIsInstance(engine, Engine)
        provider = Provider(
            engine=engine,
            provider_id=42,
            config='{"database":{"type":"map"}}')
        engine.finalize()

    def test_init_two_providers(self):
        """Tests that we can initialize two providers."""
        engine = Engine('tcp')
        self.assertIsInstance(engine, Engine)
        provider1 = Provider(
            engine=engine,
            provider_id=42,
            config='{"database":{"type":"map"}}')
        provider2 = Provider(
            engine=engine,
            provider_id=35,
            config='{"database":{"type":"map"}}')
        engine.finalize()

    def test_init_two_providers_same_id(self):
        """Tests that we can't initialize two providers
        with the same provider id."""
        engine = Engine('tcp')
        self.assertIsInstance(engine, Engine)
        provider1 = Provider(
            engine=engine,
            provider_id=42,
            config='{"database":{"type":"map"}}')
        with self.assertRaises(Exception):
            provider2 = Provider(
                engine=engine,
                provider_id=42,
                config='{"database":{"type":"map"}}')
        engine.finalize()

if __name__ == '__main__':
    unittest.main()
