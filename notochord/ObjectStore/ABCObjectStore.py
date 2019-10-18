from .. import export, schema, lookup, persist, lookup_or_persist
from ..schema import object_store as t_os
import json
from abc import abstractmethod, ABCMeta
import urlparse

__all__ = []

@export
class ABCObjectCollection(object):
    def __init__(self, object_store, name):
        self.__dict__.update(dict(
            store=object_store,
            name=name
        ))

    def __setattr__(self, name, value):
        raise TypeError("ABCObjectCollection instances are immutable")

    @abstractmethod
    def exists(self, uuid):
        raise NotImplementedError("Not Implemented: ABCObjectCollection is an Abstract Base Class")

    @abstractmethod
    def get(self, uuid):
        raise NotImplementedError("Not Implemented: ABCObjectCollection is an Abstract Base Class")

    @abstractmethod
    def post(self, uuid, content):
        raise NotImplementedError("Not Implemented: ABCObjectCollection is an Abstract Base Class")

    @abstractmethod
    def put(self, uuid, content):
        raise NotImplementedError("Not Implemented: ABCObjectCollection is an Abstract Base Class")

    @abstractmethod
    def delete(self, uuid):
        raise NotImplementedError("Not Implemented: ABCObjectCollection is an Abstract Base Class")

@export
class ABCObjectStore(object):
    __metaclass__ = ABCMeta
    uri_schemes = {}

    def __init__(self, osinst):
        self.__dict__.update(dict(
            uri=osinst.uri,
            name=osinst.name,
            idobject_store=osinst.idobject_store
        ))

    def __setattr__(self, name, value):
        raise TypeError("ABCObjectStore instances are immutable")

    @abstractmethod
    def initialize(self):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")

    @staticmethod
    def create(session, name, uri, **kwargs):
        scheme = urlparse.urlparse(uri).scheme
        klass = ABCObjectStore.uri_schemes[scheme]
        ds = lookup_or_persist(session, t_os, name=name, uri=uri, kwargs=json.dumps(kwargs))
        ret = klass(ds, **kwargs)
        ret.initialize()
        return ret

    @staticmethod
    def open_by_name(session, name):
        osinst = lookup(session, t_os, name=name)
        if osinst is None:
            raise ValueError("Object store does not exist: '{}'".format(name))
        u = urlparse.urlparse(osinst.uri)
        klass = ABCObjectStore.uri_schemes[u.scheme]
        return klass(osinst, **json.loads(osinst.kwargs))

    @staticmethod
    def open_by_id(session, idobject_store):
        osinst = lookup(session, t_os, idobject_store=idobject_store)
        if osinst is None:
            raise ValueError("Object store does not exist: '{}'".format(idobject_store))
        u = urlparse.urlparse(osinst.uri)
        klass = ABCObjectStore.uri_schemes[u.scheme]
        return klass(osinst, **json.loads(osinst.kwargs))

    @staticmethod
    def register(uri_scheme):
        def decorator_register(klass):
            ABCObjectStore.uri_schemes[uri_scheme] = klass
            return klass

        return decorator_register

    @abstractmethod
    def create_collection(self, name):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")

    @abstractmethod
    def delete_collection(self, name):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")

    @abstractmethod
    def has_collection(self, name):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")

    @abstractmethod
    def get_collection(self, name):
        raise NotImplementedError("Not Implemented: ABCObjectStore is an Abstract Base Class")
