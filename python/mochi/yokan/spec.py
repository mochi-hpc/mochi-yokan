# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


"""
.. module:: spec
   :synopsis: This package provides the configuration for a Yokan provider
   and the corresponding ConfigurationSpace.

.. moduleauthor:: Matthieu Dorier <mdorier@anl.gov>


"""


from dataclasses import dataclass
from typing import Optional
from dataclasses import dataclass
from mochi.bedrock.spec import ProviderSpec


@dataclass(frozen=True)
class BackendType:

    name: str
    is_sorted: bool = True
    has_values: bool = True
    is_persistent: bool = True


class YokanProviderSpec(ProviderSpec):

    _backends = [
        BackendType(name='map', is_persistent=False),
        BackendType(name='unordered_map', is_sorted=False, is_persistent=False),
        BackendType(name='set', is_persistent=False, has_values=False),
        BackendType(name='unordered_set', is_sorted=False, is_persistent=False, has_values=False),
        BackendType(name='rocksdb'),
        BackendType(name='leveldb'),
        BackendType(name='berkeleydb'),
        BackendType(name='lmdb'),
        BackendType(name='tkrzw'),
        BackendType(name='unqlite'),
        BackendType(name='gdbm', is_sorted=False),
    ]

    @staticmethod
    def space(types: list[str] = ['map', 'unordered_map', 'set', 'unordered_set', 'rocksdb',
                                  'leveldb', 'berkeleydb', 'lmdb', 'tkrzw', 'unqlite', 'gdbm'],
              paths: list[str] = [''],
              need_sorted_db: bool = True,
              need_values: bool = True,
              need_persistence: bool = True,
              **kwargs):
        from mochi.bedrock.config_space import ConfigurationSpace, InCondition, Categorical
        from .backends import available_backends
        db_backends = [b for b in YokanProviderSpec._backends if \
            (b.name in available_backends) and \
            ((not need_sorted_db) or b.is_sorted) and \
            ((not need_values) or b.has_values) and \
            ((not need_persistence) or b.is_persistent) and \
            b.name in types]
        db_types = [b.name for b in db_backends]
        config_cs = ConfigurationSpace()
        config_cs.add(Categorical('type', db_types))
        for b in db_backends:
            if not b.is_persistent:
                continue
            db_cs = ConfigurationSpace()
            db_cs.add(Categorical('path', paths))
            config_cs.add_configuration_space(
                prefix=b.name, delimiter='.',
                configuration_space=db_cs,
                parent_hyperparameter={'parent': config_cs['type'], 'value': b.name})

        def provider_config_resolver(config: 'Configuration', prefix: str) -> dict:
            result = {}
            result['type'] = config[f'{prefix}type']
            t = result['type']
            if f'{prefix}{t}.path' in config:
                result['path'] = config[f'{prefix}{t}.path']
            return result

        kwargs['provider_config_space'] = config_cs
        kwargs['provider_config_resolver'] = provider_config_resolver
        kwargs['dependency_config_space'] = None
        kwargs['dependency_resolver'] = None

        provider_cs = ProviderSpec.space(type='yokan', **kwargs)
        return provider_cs
