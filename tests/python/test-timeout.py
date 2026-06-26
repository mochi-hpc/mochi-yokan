import os
import sys
import unittest

wd = os.getcwd()
sys.path.append(wd + '/../python')

from mochi.margo import Engine
from mochi.yokan.client import Client
from mochi.yokan.server import Provider


class TestTimeout(unittest.TestCase):
    """Smoke test for the per-call ``timeout_ms`` kwarg.

    Default ``timeout_ms=0.0`` must reproduce the pre-extras wire format and
    semantics, and a positive ``timeout_ms`` must round-trip cleanly. We use
    a long timeout (1 s) against the in-process map backend so the call
    completes well within budget — the assertion is that the kwarg is accepted
    and the operation succeeds, not that the timeout fires.
    """

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

    def tearDown(self):
        del self.db
        del self.addr
        del self.client
        del self.provider
        self.engine.finalize()

    def test_default_timeout_unchanged(self):
        self.db.put(key='k1', value='v1')
        self.assertTrue(self.db.exists('k1'))
        self.assertEqual(self.db.length('k1'), 2)

    def test_put_get_with_timeout(self):
        self.db.put(key='k2', value='v2', timeout_ms=1000.0)
        buf = bytearray(16)
        vsize = self.db.get(key='k2', value=buf, timeout_ms=1000.0)
        self.assertEqual(bytes(buf[:vsize]), b'v2')
        self.assertTrue(self.db.exists('k2', timeout_ms=1000.0))
        self.assertEqual(self.db.length('k2', timeout_ms=1000.0), 2)

    def test_timeout_zero_is_blocking(self):
        # timeout_ms=0.0 is the explicit "no timeout" sentinel; behavior
        # must match the default-omitted call.
        self.db.put(key='k3', value='v3', timeout_ms=0.0)
        self.assertTrue(self.db.exists('k3', timeout_ms=0.0))

    def test_erase_with_timeout(self):
        self.db.put(key='k4', value='v4')
        self.db.erase('k4', timeout_ms=1000.0)
        self.assertFalse(self.db.exists('k4'))

    def test_collection_with_timeout(self):
        coll = self.db.create_collection('docs', timeout_ms=1000.0)
        self.assertTrue(self.db.collection_exists('docs', timeout_ms=1000.0))
        doc = b'{"k":1}'
        doc_id = coll.store(doc, timeout_ms=1000.0)
        buf = bytearray(64)
        size = coll.load(doc_id, buf, timeout_ms=1000.0)
        self.assertEqual(bytes(buf[:size]), doc)
        self.assertEqual(coll.size(timeout_ms=1000.0), 1)
        coll.erase(doc_id, timeout_ms=1000.0)
        self.db.drop_collection('docs', timeout_ms=1000.0)


if __name__ == '__main__':
    unittest.main()
