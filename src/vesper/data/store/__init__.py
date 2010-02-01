__all__ = ['get_factory']

_proto_map = { # XXX don't hardcode this
    'tyrant':'vesper.data.store.RxPathModelTyrant.TransactionTyrantModel',
    'rdf':'vesper.data.RxPath.FileModel',
    'mem':'vesper.data.RxPathModel.MemModel',
    'bdb':'vesper.data.store.RxPathModelBdb.TransactionBdbModel'
}
# get a reference to the module object
# workaround an inconvenient behavior with __import__ on
# multilevel imports
#
# Can't find the ref in the python docs anymore, but these discuss the issue:
# http://stackoverflow.com/questions/211100/pythons-import-doesnt-work-as-expected
# http://stackoverflow.com/questions/547829/how-to-dynamically-load-a-python-class
def _my_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod
    
def get_factory(proto):
    full_model = _proto_map[proto].split('.')
    model_pkg = '.'.join(full_model[:-1]) # e.g. 'vesper.store.RxPathModelTyrant'
    model_class = full_model[-1] # e.g. 'TransactionTyrantModel'

    mod = _my_import(model_pkg)
    fac = getattr(mod, model_class)
    return fac
