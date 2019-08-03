from .. import export, schema, lookup, persist, lookup_or_persist
from ..schema import object_store as t_ds
import json
from abc import abstractmethod, ABCMeta
import urlparse

__all__ = []

@export
class ABCObjectStore(object):
    __metaclass__ = ABCMeta
    uri_schemes = {}

    def __init__(self, ds):
        self.__dict__.update(dict(
            uri=ds.uri,
            name=ds.name,
            idobject_store=ds.idobject_store
        ))

    def __setattr__(self, name, value):
        raise TypeError("ABCObjectStore instances are immutable")

    @abstractmethod
    def initialize(self):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")

    @abstractmethod
    def exists(self, uuid):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")

    @abstractmethod
    def get(self, uuid):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")

    @abstractmethod
    def post(self, uuid, content):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")

    @abstractmethod
    def put(self, uuid, content):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")

    @abstractmethod
    def delete(self, uuid):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")

    @staticmethod
    def create(session, name, uri, **kwargs):
        scheme = urlparse.urlparse(uri).scheme
        klass = ABCObjectStore.uri_schemes[scheme]
        ds = lookup_or_persist(session, t_ds, name=name, uri=uri, kwargs=json.dumps(kwargs))
        ret = klass(ds, **kwargs)
        ret.initialize()
        return ret

    @staticmethod
    def open(session, name):
        ds = lookup(session, t_ds, name=name)
        u = urlparse.urlparse(ds.uri)
        klass = ABCObjectStore.uri_schemes[u.scheme]
        return klass(ds, **json.loads(ds.kwargs))

    @staticmethod
    def register(uri_scheme):
        def decorator_register(klass):
            ABCObjectStore.uri_schemes[uri_scheme] = klass
            return klass

        return decorator_register
