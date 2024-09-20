# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


import unittest
from .config_space import YokanSpaceBuilder
from mochi.bedrock.spec import ProcSpec


class TestConfigSpace(unittest.TestCase):

    def test_yokan_provider_space(self):
        yokan_space_builder = YokanSpaceBuilder()
        provider_space_factories = [
            {
                "family": "databases",
                "builder": yokan_space_builder,
                "count": (1,3)
            }
        ]
        space = ProcSpec.space(num_pools=(1,3), num_xstreams=(2,5),
                               provider_space_factories=provider_space_factories).freeze()
        #print(space)
        config = space.sample_configuration()
        #print(config)
        spec = ProcSpec.from_config(address='na+sm', config=config)
        #print(spec.to_json(indent=4))

    def test_yokan_provider_space_with_paths(self):
        yokan_space_builder = YokanSpaceBuilder(paths=["/aaa", "/bbb"])
        provider_space_factories = [
            {
                "family": "databases",
                "builder": yokan_space_builder,
                "count": (1,3)
            }
        ]
        space = ProcSpec.space(num_pools=(1,3), num_xstreams=(2,5),
                               provider_space_factories=provider_space_factories).freeze()
        #print(space)
        config = space.sample_configuration()
        #print(config)
        spec = ProcSpec.from_config(address='na+sm', config=config)
        #print(spec.to_json(indent=4))



if __name__ == "__main__":
    unittest.main()
