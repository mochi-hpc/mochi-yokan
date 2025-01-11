# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


"""
.. module:: config_space
   :synopsis: This package provides the configuration for a Yokan provider
   and the corresponding ConfigurationSpace.

.. moduleauthor:: Matthieu Dorier <mdorier@anl.gov>


"""


from dataclasses import dataclass
from typing import Optional
from dataclasses import dataclass
from mochi.bedrock.spec import (
        ProviderConfigSpaceBuilder,
        CS,
        Config,
        ProviderSpec)


@dataclass(frozen=True)
class BackendType:

    name: str
    is_sorted: bool = True
    has_values: bool = True
    is_persistent: bool = True
    has_collections: bool = True
    has_keyval: bool = True


class YokanSpaceBuilder(ProviderConfigSpaceBuilder):

    _backends = [
        BackendType(name='map',
                    is_persistent=False, has_collections=True),
        BackendType(name='unordered_map',
                    is_sorted=False, is_persistent=False, has_collections=True),
        BackendType(name='set',
                    is_persistent=False, has_values=False, has_collections=False),
        BackendType(name='unordered_set',
                    is_sorted=False, is_persistent=False, has_values=False, has_collections=False),
        BackendType(name='rocksdb'),
        BackendType(name='leveldb'),
        BackendType(name='berkeleydb'),
        BackendType(name='lmdb'),
        BackendType(name='tkrzw'),
        BackendType(name='unqlite'),
        BackendType(name='gdbm',
                    is_sorted=False),
        BackendType(name='array',
                    is_persistent=False, has_collections=True, has_keyval=False),
        BackendType(name='log',
                    has_collections=True, has_keyval=False),
    ]

    def __init__(self, *,
                 types: list[str] = ['map', 'unordered_map', 'set', 'unordered_set', 'rocksdb',
                                    'leveldb', 'berkeleydb', 'lmdb', 'tkrzw', 'unqlite', 'gdbm',
                                     'array', 'log'],
                 paths: list[str] = [],
                 need_sorted_db: bool = True,
                 need_values: bool = True,
                 need_persistence: bool = False,
                 need_collections: bool = True,
                 need_keyval: bool = True,
                 tags: list[str] = []):
        from .backends import available_backends
        if need_persistence and len(paths) == 0:
            raise ValueError("Paths should be provided if database needs persistence")
        self.types = [b for b in YokanSpaceBuilder._backends if \
            (b.name in available_backends) and \
            ((not need_sorted_db) or b.is_sorted) and \
            ((not need_values) or b.has_values) and \
            ((not need_persistence) or b.is_persistent) and \
            ((not need_collections) or b.has_collections) and \
            ((not need_keyval) or b.has_keyval) and \
            b.name in types]
        self.paths = paths
        self.tags = tags

    def set_provider_hyperparameters(self, configuration_space: CS) -> None:
        from mochi.bedrock.config_space import (
            CategoricalChoice,
            InCondition,
            Categorical,
            CategoricalOrConst)

        # add a pool dependency
        num_pools = configuration_space["margo.argobots.num_pools"]
        configuration_space.add(CategoricalChoice("pool", num_options=num_pools))
        # add backend type
        hp_type = Categorical('type', self.types)
        configuration_space.add(hp_type)
        # add path
        if len(self.paths) != 0:
            hp_path = CategoricalOrConst("path", self.paths)
            configuration_space.add(hp_path)
            path_if_persistent = InCondition(hp_path, hp_type, [t for t in self.types if t.is_persistent])
            configuration_space.add(path_if_persistent)

    def resolve_to_provider_spec(self, name: str, provider_id: int,
                                 config: Config, prefix: str) -> ProviderSpec:
        type_key = prefix + "type"
        path_key = prefix + "path"
        db_type = config[prefix + "type"]
        cfg = {
            "database": {
                "type" : db_type.name
            }
        }
        if path_key in config:
            cfg["database"]["config"] = {
                "path": config[path_key]
            }
        dep = {
            "pool" : int(config[prefix + "pool"])
        }
        return ProviderSpec(name=name, type="yokan", provider_id=provider_id,
                            tags=self.tags, config=cfg, dependencies=dep)
