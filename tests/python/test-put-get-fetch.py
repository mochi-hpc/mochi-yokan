import os
import sys
import unittest
import json
import string
import random
from typing import Optional

wd = os.getcwd()
sys.path.append(wd+'/../python')

from mochi.margo import Engine
from mochi.yokan.exception import Exception, YOKAN_ERR_KEY_NOT_FOUND
from mochi.yokan.client import Client
from mochi.yokan.server import Provider

class TestPutGetFetch(unittest.TestCase):

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
        letters = string.ascii_letters
        for i in range(0,8):
            key_len = random.randint(8, 64)
            val_len = random.randint(16, 128)
            key = ''.join(random.choice(letters) for i in range(key_len))
            val = ''.join(random.choice(letters) for i in range(val_len))
            self.reference[key] = val

    def tearDown(self):
        del self.db
        del self.addr
        del self.client
        del self.provider
        del self.reference
        self.engine.finalize()

    def test_put_get_strings(self):
        """Test that we can put and get string key/value pairs."""
        for k, v in self.reference.items():
            self.db.put(key=k, value=v)
        out_val = bytearray(128)
        for k, v in self.reference.items():
            vsize = self.db.get(key=k, value=out_val)
            self.assertEqual(out_val[0:vsize].decode("ascii"), v)

        with self.assertRaises(Exception) as ctx:
            self.db.get(key='xxxxx', value=out_val)
        self.assertEqual(ctx.exception.code, YOKAN_ERR_KEY_NOT_FOUND)

    def test_put_get_buffers(self):
        """Test that we can put and get buffer key/value pairs."""
        for k, v in self.reference.items():
            self.db.put(key=bytearray(k.encode('ascii')),
                        value=bytearray(v.encode('ascii')))
        out_val = bytearray(128)
        for k, v in self.reference.items():
            vsize = self.db.get(key=bytearray(k.encode('ascii')),
                                value=out_val)
            self.assertEqual(out_val[0:vsize].decode("ascii"), v)

        with self.assertRaises(Exception) as ctx:
            self.db.get(key=bytearray(b'xxxxx'), value=out_val)
        self.assertEqual(ctx.exception.code, YOKAN_ERR_KEY_NOT_FOUND)

    def test_put_fetch_strings(self):
        """Test that we can fetch key/value pairs."""
        for k, v in self.reference.items():
            self.db.put(key=k, value=v)
        for k_ref, v_ref in self.reference.items():
            def compare(i: int, k: str, v: Optional[memoryview]):
                self.assertEqual(k, k_ref)
                self.assertEqual(v, memoryview(bytearray(v_ref.encode('ascii'))))
            self.db.fetch(key=k_ref, callback=compare)

        def check_not_found(i: int, k: str, v: Optional[memoryview]):
            if v is None:
                raise Exception("Key not found", YOKAN_ERR_KEY_NOT_FOUND)
        with self.assertRaises(Exception) as ctx:
            self.db.fetch(key='xxxxx', callback=check_not_found)
        self.assertEqual(ctx.exception.code, YOKAN_ERR_KEY_NOT_FOUND)

    def test_put_fetch_buffers(self):
        """Test that we can fetch key/value pairs."""
        for k, v in self.reference.items():
            self.db.put(key=bytearray(k.encode('ascii')),
                        value=bytearray(v.encode('ascii')))
        for k_ref, v_ref in self.reference.items():
            def compare(i: int, k: memoryview, v: Optional[memoryview]):
                self.assertEqual(k, memoryview(bytearray(k_ref.encode('ascii'))))
                self.assertEqual(v, memoryview(bytearray(v_ref.encode('ascii'))))
            self.db.fetch(key=bytearray(k_ref.encode('ascii')), callback=compare)

        def check_not_found(i: int, k: memoryview, v: Optional[memoryview]):
            if v is None:
                raise Exception("Key not found", YOKAN_ERR_KEY_NOT_FOUND)
        with self.assertRaises(Exception) as ctx:
            self.db.fetch(key=bytearray(b'xxxxx'), callback=check_not_found)
        self.assertEqual(ctx.exception.code, YOKAN_ERR_KEY_NOT_FOUND)

    def test_put_get_partial(self):
        """Test that we can put/get partial regions of bytearrays."""
        in_key = b'XXXmatthieuXXXX'
        in_val = b'XXXXXdorierXX'
        self.db.put(key=in_key[3:11], value=in_val[5:11])

        out_key = b'YYmatthieuYY'
        out_val = bytearray(len('dorier')+2)
        self.db.get(key=out_key[2:10], value=memoryview(out_val)[1:7])
        self.assertEqual(out_val[1:7].decode('ascii'),
                in_val[5:11].decode('ascii'))

    def test_put_get_multi_strings(self):
        """Test that we can use put_multi and get_multi with strings."""
        pairs = list(self.reference.items())
        self.db.put_multi(pairs=pairs)

        out = []
        for k in self.reference:
            out.append((k, bytearray(128)))
        out.append(('xxxxxxxx', bytearray(128)))
        random.shuffle(out)

        vsizes = self.db.get_multi(pairs=out)
        for i, (k, v) in enumerate(out):
            if k == 'xxxxxxxx':
                self.assertIsNone(vsizes[i])
            else:
                self.assertEqual(v[0:vsizes[i]].decode('ascii'),
                                  self.reference[k])

    def test_put_get_multi_buffers(self):
        """Test that we can use put_multi and get_multi with buffers."""
        pairs = [ (bytearray(k.encode('ascii')), bytearray(v.encode('ascii'))) for k, v in self.reference.items() ]
        self.db.put_multi(pairs=pairs)

        out = []
        for k in self.reference:
            out.append((bytearray(k.encode('ascii')), bytearray(128)))
        out.append((bytearray(b'xxxxxxxx'), bytearray(128)))
        random.shuffle(out)

        vsizes = self.db.get_multi(pairs=out)
        for i, (k, v) in enumerate(out):
            if k.decode('ascii') == 'xxxxxxxx':
                self.assertIsNone(vsizes[i])
            else:
                self.assertEqual(v[0:vsizes[i]].decode('ascii'),
                                  self.reference[k.decode('ascii')])

    def test_put_fetch_multi_strings(self):
        """Test that we can use put_multi and fetch_multi with strings."""
        pairs = list(self.reference.items())
        self.db.put_multi(pairs=pairs)

        keys = list(self.reference.keys())
        keys.append('xxxxxxxx')
        random.shuffle(keys)

        def compare(i: int, k: str, v: Optional[memoryview]):
            if k == 'xxxxxxxx':
                self.assertIsNone(v)
            else:
                self.assertEqual(k, keys[i])
                v_ref = self.reference[keys[i]]
                self.assertEqual(v, memoryview(bytearray(v_ref.encode('ascii'))))

        self.db.fetch_multi(keys=keys, callback=compare)

    def test_put_fetch_multi_buffers(self):
        """Test that we can use put_multi and fetch_multi with buffer."""
        pairs = [ (bytearray(k.encode('ascii')), bytearray(v.encode('ascii'))) for k, v in self.reference.items() ]
        self.db.put_multi(pairs=pairs)

        keys = [ p[0] for p in pairs ]
        keys.append(b'xxxxxxxx')
        random.shuffle(keys)

        def compare(i: int, k: memoryview, v: Optional[memoryview]):
            if k == b'xxxxxxxx':
                self.assertIsNone(v)
            else:
                self.assertEqual(bytearray(k), keys[i])
                v_ref = self.reference[keys[i].decode()]
                self.assertEqual(v, memoryview(bytearray(v_ref.encode('ascii'))))

        self.db.fetch_multi(keys=keys, callback=compare)

    def test_put_get_packed(self):
        """Test that we can use put_packed and get_packed."""
        in_keys_buf = bytearray()
        in_vals_buf = bytearray()
        in_key_sizes = []
        in_val_sizes = []
        for k, v in self.reference.items():
            in_keys_buf += bytearray(k.encode('ascii'))
            in_key_sizes.append(len(k))
            in_vals_buf += bytearray(v.encode('ascii'))
            in_val_sizes.append(len(v))

        self.db.put_packed(keys=in_keys_buf, key_sizes=in_key_sizes,
                           values=in_vals_buf, value_sizes=in_val_sizes)

        out_keys = list(self.reference.keys())
        out_keys.append('xxxxxxxx')
        random.shuffle(out_keys)
        out_keys_buf = bytearray()
        out_key_sizes = []
        for k in out_keys:
            out_keys_buf += k.encode('ascii')
            out_key_sizes.append(len(k))
        out_vals_buf = bytearray(128*len(out_key_sizes))

        out_val_sizes = self.db.get_packed(keys=out_keys_buf, key_sizes=out_key_sizes,
                                           values=out_vals_buf)
        voffset = 0
        for i, k in enumerate(out_keys):
            if k == 'xxxxxxxx':
                self.assertIsNone(out_val_sizes[i])
                continue
            vsize = out_val_sizes[i]
            self.assertEqual(out_vals_buf[voffset:voffset+vsize].decode('ascii'),
                             self.reference[k])
            voffset += vsize

    def test_put_fetch_packed(self):
        """Test that we can use put_packed and fetch_packed."""
        in_keys_buf = bytearray()
        in_vals_buf = bytearray()
        in_key_sizes = []
        in_val_sizes = []
        for k, v in self.reference.items():
            in_keys_buf += bytearray(k.encode('ascii'))
            in_key_sizes.append(len(k))
            in_vals_buf += bytearray(v.encode('ascii'))
            in_val_sizes.append(len(v))

        self.db.put_packed(keys=in_keys_buf, key_sizes=in_key_sizes,
                           values=in_vals_buf, value_sizes=in_val_sizes)

        out_keys = list(self.reference.keys())
        out_keys.append('xxxxxxxx')
        random.shuffle(out_keys)
        out_keys_buf = bytearray()
        out_key_sizes = []
        for k in out_keys:
            out_keys_buf += k.encode('ascii')
            out_key_sizes.append(len(k))

        def compare(i: int, k: memoryview, v: Optional[memoryview]):
            if k == b'xxxxxxxx':
                self.assertIsNone(v)
            else:
                self.assertEqual(bytearray(k), bytearray(out_keys[i].encode('ascii')))
                v_ref = self.reference[out_keys[i]]
                self.assertEqual(v, memoryview(bytearray(v_ref.encode('ascii'))))

        self.db.fetch_packed(
            keys=out_keys_buf, key_sizes=out_key_sizes,
            callback=compare)


if __name__ == '__main__':
    unittest.main()
