import os
import sys
import unittest

wd = os.getcwd()
sys.path.append(wd+'/../python')

import pyyokan_common as yokan
from pyyokan_server import Provider
from pymargo.core import Engine

class TestInitProvider(unittest.TestCase):

    def test_init_provider(self):
        """Tests that we can initialize a provider
        without any token or configuration."""
        engine = Engine('tcp')
        self.assertIsInstance(engine, Engine)
        provider = Provider(
            mid=engine.get_internal_mid(),
            provider_id=42)
        engine.finalize()

    def test_init_provider_with_token(self):
        """Tests that we can initialize a provider
        with a token."""
        engine = Engine('tcp')
        self.assertIsInstance(engine, Engine)
        provider = Provider(
            mid=engine.get_internal_mid(),
            provider_id=42,
            token='abc')
        engine.finalize()

    def test_init_provider_with_config(self):
        """Tests that we can initialize a provider
        with a config."""
        engine = Engine('tcp')
        self.assertIsInstance(engine, Engine)
        provider = Provider(
            mid=engine.get_internal_mid(),
            provider_id=42,
            config='{}')
        engine.finalize()

    def test_init_two_providers(self):
        """Tests that we can initialize two providers."""
        engine = Engine('tcp')
        self.assertIsInstance(engine, Engine)
        provider1 = Provider(
            mid=engine.get_internal_mid(),
            provider_id=42,
            config='{}')
        provider2 = Provider(
            mid=engine.get_internal_mid(),
            provider_id=35,
            config='{}')
        engine.finalize()

    def test_init_two_providers_same_id(self):
        """Tests that we can't initialize two providers
        with the same provider id."""
        engine = Engine('tcp')
        self.assertIsInstance(engine, Engine)
        provider1 = Provider(
            mid=engine.get_internal_mid(),
            provider_id=42,
            config='{}')
        with self.assertRaises(yokan.Exception):
            provider2 = Provider(
                mid=engine.get_internal_mid(),
                provider_id=42,
                config='{}')
        engine.finalize()

if __name__ == '__main__':
    unittest.main()
