from .. import export, schema, lookup, persist, lookup_or_persist
from ..schema import coherence_store as t_cs
import json
from abc import abstractmethod, ABCMeta
import urlparse

__all__ = []

@export
class ABCCoherenceCollection(object):
    def __init__(self, coherence_store, name):
        self.__dict__.update(dict(
            store=coherence_store,
            name=name
        ))

    def __setattr__(self, name, value):
        raise TypeError("ABCCoherenceCollection instances are immutable")

    @abstractmethod
    def exists(self, name):
        raise NotImplementedError("Not Implemented: ABCCoherenceCollection is an Abstract Base Class")

    @abstractmethod
    def get(self, name):
        raise NotImplementedError("Not Implemented: ABCCoherenceCollection is an Abstract Base Class")

    @abstractmethod
    def set(self, name, ident):
        raise NotImplementedError("Not Implemented: ABCCoherenceCollection is an Abstract Base Class")

    @abstractmethod
    def delete(self, name):
        raise NotImplementedError("Not Implemented: ABCCoherenceCollection is an Abstract Base Class")

    @abstractmethod
    def current(self):
        raise NotImplementedError("Not Implemented: ABCCoherenceCollection is an Abstract Base Class")

@export
class ABCCoherenceStore(object):
    __metaclass__ = ABCMeta
    uri_schemes = {}

    def __init__(self, csinst):
        self.__dict__.update(dict(
            uri=csinst.uri,
            name=csinst.name,
            idcoherence_store=csinst.idcoherence_store
        ))

    def __setattr__(self, name, value):
        raise TypeError("ABCCoherenceStore instances are immutable")

    @abstractmethod
    def initialize(self):
        raise NotImplementedError("Not Implemented: ABCCoherenceStore is an Abstract Base Class")

    @staticmethod
    def create(session, name, uri, **kwargs):
        scheme = urlparse.urlparse(uri).scheme
        klass = ABCCoherenceStore.uri_schemes[scheme]
        cs = lookup_or_persist(session, t_cs, name=name, uri=uri, kwargs=json.dumps(kwargs))
        ret = klass(cs, **kwargs)
        ret.initialize()
        return ret

    @staticmethod
    def open_by_name(session, name):
        csinst = lookup(session, t_cs, name=name)
        if csinst is None:
            raise ValueError("Coherence store does not exist: '{}'".format(name))
        u = urlparse.urlparse(csinst.uri)
        klass = ABCCoherenceStore.uri_schemes[u.scheme]
        return klass(csinst, **json.loads(csinst.kwargs))

    @staticmethod
    def open_by_id(session, idcoherence_store):
        csinst = lookup(session, t_cs, idcoherence_store=idcoherence_store)
        if csinst is None:
            raise ValueError("Coherence store does not exist: '{}'".format(idcoherence_store))
        u = urlparse.urlparse(csinst.uri)
        klass = ABCCoherenceStore.uri_schemes[u.scheme]
        return klass(csinst, **json.loads(csinst.kwargs))

    @staticmethod
    def register(uri_scheme):
        def decorator_register(klass):
            ABCCoherenceStore.uri_schemes[uri_scheme] = klass
            return klass

        return decorator_register

    @abstractmethod
    def create_collection(self, name):
        raise NotImplementedError("Not Implemented: ABCCoherenceStore is an Abstract Base Class")

    @abstractmethod
    def delete_collection(self, name):
        raise NotImplementedError("Not Implemented: ABCCoherenceStore is an Abstract Base Class")

    @abstractmethod
    def has_collection(self, name):
        raise NotImplementedError("Not Implemented: ABCCoherenceStore is an Abstract Base Class")

    @abstractmethod
    def get_collection(self, name):
        raise NotImplementedError("Not Implemented: ABCCoherenceStore is an Abstract Base Class")
