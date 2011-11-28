#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
from vesper.utils import package_import

__all__ = ['get_factory']

_proto_map = { # XXX don't hardcode this
    'tyrant':'vesper.data.store.tyrant.TransactionTyrantStore',
    'file':'vesper.data.store.basic.FileStore',
    'mem':'vesper.data.store.basic.MemStore',
    'bdb':'vesper.data.store.bdb.TransactionBdbStore',
    'sqlite':'vesper.data.store.sqlite.TransactionSqliteStore'
}
    
def get_factory(proto):
    full_model = _proto_map[proto].split('.')
    model_pkg = '.'.join(full_model[:-1]) # e.g. 'vesper.store.tyrant'
    model_class = full_model[-1] # e.g. 'TransactionTyrantStore'

    mod = package_import(model_pkg)
    fac = getattr(mod, model_class)
    return fac
