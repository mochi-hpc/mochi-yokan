# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


import unittest
from .spec import YokanProviderSpec
from mochi.bedrock.spec import ProcSpec

class TestConfigSpace(unittest.TestCase):

    def test_yokan_config_space(self):

        max_num_pools = 3

        ycs = YokanProviderSpec.space(
            max_num_pools=max_num_pools,
            paths=["/tmp", "/scratch"],
            need_values=False, need_persistence=False,
            tags=['my_tag'])

        provider_space_factories = [
            {
                "family": "databases",
                "space": ycs,
                "count": (1,3)
            }
        ]

        space = ProcSpec.space(num_pools=(1, max_num_pools), num_xstreams=(2, 5),
                               provider_space_factories=provider_space_factories).freeze()
        print(space)
        config = space.sample_configuration()
        print(config)
        spec = ProcSpec.from_config(address='na+sm', config=config)
        print(spec.to_json(indent=4))


if __name__ == "__main__":
    unittest.main()
