from .. import schema
from .. import export, tag_widget, lookup_or_persist, widget_label, persist, lookup
from ..ObjectStore import ABCObjectStore
from ..CoherenceStore import ABCCoherenceStore
from sqlalchemy import exc as sa_exc
import uuid

__all__ = []

@export
class Feature(object):
    def __init__(self, dbinst, context):
        self.__dict__.update(dict(
            _dbinst=dbinst,
            _context=context
        ))

    def __setattr__(self, name, value):
        raise TypeError("Feature instances are immutable")

    def __getattr__(self, name):
        return getattr(self._dbinst, name)

    @property
    def context(self):
        return self._context

    def add_tag(self, label):
        tag_feature(self._context.session, self._dbinst, label)

@export
class FeatureSet(object):
    def __init__(self, dbinst, collection, context):
        self.__dict__.update(dict(
            _dbinst=dbinst,
            _collection=collection,
            _context=context
        ))

    @property
    def name(self):
        return self._dbinst.name

    @property
    def idfeature_set(self):
        return self._dbinst.idfeature_set

    @property
    def idcoherence_store(self):
        return self._dbinst.idcoherence_store

    @property
    def context(self):
        return self._context

    def __setattr__(self, name, value):
        raise TypeError("FeatureSet instances are immutable")

    def create_feature(self, uuid=None, labels=[], content=None):
        if uuid is None:
            uuid = uuid.uuid4().hex

        session = self.context.session

        try:
            with session.begin_nested(), warnings.catch_warnings():
                warnings.simplefilter("ignore", category=sa_exc.SAWarning)
                n_feature = persist(session, schema.feature(
                        idfeature_set = self.idfeature_set,
                        uuid = uuid
                    ))
        except:
            n_feature = None
        else:
            tag_feature(session, n_feature, feature_label(session, 'feature_set', self.name))
            self._collection.put(n_feature.uuid, content)

        return n_feature

@export
class Widget(object):
    def __init__(self, dbinst, context):
        self.__dict__.update(dict(
            _dbinst=dbinst,
            _context=context
        ))

    def __setattr__(self, name, value):
        raise TypeError("Widget instances are immutable")

    def __getattr__(self, name):
        return getattr(self._dbinst, name)

    @property
    def context(self):
        return self._context

    def add_tag(self, label):
        tag_widget(self._context.session, self._dbinst, label)

@export
class WidgetSet(object):
    def __init__(self, dbinst, collection, context):
        self.__dict__.update(dict(
            _dbinst=dbinst,
            _collection=collection,
            _context=context
        ))

    @property
    def name(self):
        return self._dbinst.name

    @property
    def idwidget_set(self):
        return self._dbinst.idwidget_set

    @property
    def idobject_store(self):
        return self._dbinst.idobject_store

    @property
    def context(self):
        return self._context

    def __setattr__(self, name, value):
        raise TypeError("WidgetSet instances are immutable")

    def create_widget(self, uuid=None, labels=[], content=None):
        if uuid is None:
            uuid = uuid.uuid4().hex

        session = self.context.session

        try:
            with session.begin_nested(), warnings.catch_warnings():
                warnings.simplefilter("ignore", category=sa_exc.SAWarning)
                n_widget = persist(session, schema.widget(
                        idcontext_created = self.context.idcontext,
                        idwidget_set = self.idwidget_set,
                        uuid = uuid
                    ))
        except:
            n_widget = None
        else:
            tag_widget(session, n_widget, widget_label(session, 'widget_set', self.name))
            self._collection.put(n_widget.uuid, content)

        return n_widget

@export
class Context(object):
    def __init__(self, app, session):
        n_uuid = uuid.uuid4().hex

        self.__dict__.update(dict(
            _session = session,
            _app=app,
            _context=schema.context(uuid=n_uuid)
        ))

        session.add(self._context)
        session.flush()

    @property
    def app(self):
        return self._app

    @property
    def log(self):
        return self._app.log

    @property
    def session(self):
        return self._session

    @property
    def uuid(self):
        return self._context.uuid

    @property
    def idcontext(self):
        return self._context.idcontext
    
    def build_model(self, __klass__, **kwargs):
        from . import Model
        return Model.new(self, __klass__, params=kwargs)

    def __getattr__(self, name):
        return getattr(self._session, name)

    def __setattr__(self, name, value):
        raise TypeError("Model contexts are immutable")

    def create_widget_set(self, name, object_store):
        if (not isinstance(name, str) and not isinstance(name, unicode)) or len(name) > 511:
            raise TypeError("name must be str with len < 512")

        if isinstance(object_store, schema.object_store):
            object_store = ABCObjectStore.open(self._session, object_store.name)
        elif isinstance(object_store, str) or isinstance(object_store, unicode):
            object_store = ABCObjectStore.open(self._session, object_store)
        elif isinstance(object_store, ABCObjectStore):
            pass
        else:
            raise TypeError("object_store must be schema.object_store or str")

        dbinst = persist(self._session, schema.widget_set(
            name = name,
            idobject_store = object_store.idobject_store
        ))

        collection = object_store.create_collection(name)

        ret = WidgetSet(dbinst=dbinst, collection=collection, context=self)

        return ret

    def load_widget_set(self, widget_set_name):
        dbinst = lookup(self._session, schema.widget_set, name=widget_set_name)
        if dbinst is None:
            raise ValueError("Widget set does not exist: '{}'".format(widget_set_name))
        object_store = ABCObjectStore.open_by_id(self._session, dbinst.idobject_store)
        collection = object_store.get_collection(widget_set_name)
        return WidgetSet(dbinst, collection, context=self)

    def create_feature_set(self, name, coherence_store):
        if (not isinstance(name, str) and not isinstance(name, unicode)) or len(name) > 511:
            raise TypeError("name must be str with len < 512")

        if isinstance(coherence_store, schema.coherence_store):
            coherence_store = ABCCoherenceStore.open(self._session, coherence_store.name)
        elif isinstance(coherence_store, str) or isinstance(coherence_store, unicode):
            coherence_store = ABCCoherenceStore.open(self._session, coherence_store)
        elif isinstance(coherence_store, ABCCoherenceStore):
            pass
        else:
            raise TypeError("coherence_store must be schema.coherence_store or str")

        dbinst = persist(self._session, schema.feature_set(
            name = name,
            idcoherence_store = coherence_store.idcoherence_store
        ))

        collection = coherence_store.create_collection(name)

        ret = FeatureSet(dbinst=dbinst, collection=collection, context=self)

        return ret

    def load_feature_set(self, feature_set_name):
        dbinst = lookup(self._session, schema.feature_set, name=feature_set_name)
        if dbinst is None:
            raise ValueError("Feature set does not exist: '{}'".format(feature_set_name))
        coherence_store = ABCCoherenceStore.open_by_id(self._session, dbinst.idcoherence_store)
        collection = coherence_store.get_collection(feature_set_name)
        return FeatureSet(dbinst, collection, context=self)

