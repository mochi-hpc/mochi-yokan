# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


import unittest
from .spec import YokanProviderSpec
from mochi.bedrock.spec import PoolSpec

class TestConfigSpace(unittest.TestCase):

    def test_yokan_config_space(self):
        from ConfigSpace import ConfigurationSpace

        pools = [PoolSpec(name='p1'), PoolSpec(name='p2')]

        ycs = YokanProviderSpec.space(
            max_num_pools=4,
            paths=["/tmp", "/scratch"],
            need_values=False, need_persistence=False,
            tags=['my_tag'])
        print(ycs)
        cs = ConfigurationSpace()
        cs.add_configuration_space(prefix='abc', delimiter='.',
                                   configuration_space=ycs)
        config = cs.sample_configuration()
        print(config)
        spec = YokanProviderSpec.from_config(
            name='my_provider', provider_id=42, pools=pools,
            config=config, prefix='abc.')
        print(spec)


if __name__ == "__main__":
    unittest.main()
