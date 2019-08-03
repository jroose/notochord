import logging
import collections
import lru
import itertools
import inspect
import iso8601
import sys
import datetime
import sqlalchemy
from contextlib import contextmanager

__all__ = ["null_log", "export"]

null_log = logging.getLogger("null")
null_log.addHandler(logging.NullHandler())

def export(obj):
    inspect.getmodule(obj).__all__.append(obj.__name__)
    return obj

@export
def insert_ignore(t, dialect=None):
    if dialect in (None, "mysql", "sqlite"):
        return t.__table__.insert() \
            .prefix_with("IGNORE", dialect="mysql") \
            .prefix_with("OR IGNORE", dialect="sqlite")
    elif dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
        return insert(t.__table__).on_conflict_do_nothing()
    else:
        raise NotImplementedError("Unsupported SQL dialect: {}".format(dialect))

@export
def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.izip_longest(*args, fillvalue=fillvalue)

#TODO: Switch to redis for cache
@export
class QueryCache(object):
    def __init__(self, count=65536):
        self.caches = collections.defaultdict(lambda: lru.LRU(count))

    def __call__(self, session, table, idcol, **kwargs):
        uid = kwargs[idcol]
        cache = self.caches[table.__tablename__]
        ret = cache.get(uid)

        if ret is None:
            n_entry = session.query(table).filter(getattr(table, idcol) == uid).one_or_none()
            if n_entry is None:
                n_entry = table(**kwargs)
                session.add(n_entry)
                session.flush()
            ret = getattr(n_entry, "id{tbl}".format(tbl=table.__name__))
            cache[uid] = ret

        return ret

@export
class FeatureCache(object):
    def __init__(self, count=65536, log=None):
        self.cache = lru.LRU(count)
        self.log = log or null_log

    def __call__(self, session, idfeature_set, names):
        from .schema import feature as t_f

        self.log.info("Replacing feature names with idfeature and identifying missing features")
        missing = collections.defaultdict(lambda: [])
        ret = []
        for it, name in enumerate(names):
            key = self.cache.get(name)
            ret.append(key)
            if key is None:
                missing[name].append(it)

        dialect = session.bind.dialect.name

        if dialect == 'sqlite':
            for batch in grouper(missing.iteritems(), 445):
                session.execute(insert_ignore(t_f, dialect).values(
                    [dict(name=b[0], idfeature_set=idfeature_set) for b in batch if b is not None]
                ))

                q_f = session.query(t_f) \
                    .filter(t_f.name.in_([b[0] for b in batch if b is not None])) \
                    .filter(t_f.idfeature_set == idfeature_set)

                for f in q_f:
                    name = f.name
                    key = f.idfeature
                    self.cache[name] = key
                    for it in missing[name]:
                        ret[it] = key
        else:
            self.log.info("Inserting missing features")
            session.execute(insert_ignore(t_f, dialect).values(
                [dict(name=b[0], idfeature_set=idfeature_set) for b in missing.iteritems()]
            ))

            self.log.info("Querying and replacing missing features")
            q_f = session.query(t_f) \
                .filter(t_f.name.in_(missing.keys())) \
                .filter(t_f.idfeature_set == idfeature_set)

            for f in q_f:
                name = f.name
                key = f.idfeature
                self.cache[name] = key
                for it in missing[name]:
                    ret[it] = key
            self.log.info("Done")

        return ret

@export
def get_utcnow():
    return datetime.datetime.now(tz=iso8601.UTC)

@export
def lookup(session, table, **kwargs):
    return session.query(table).filter_by(**kwargs).one_or_none()

@export
def persist(session, inst, **kwargs):
    session.add(inst)
    session.flush()
    return inst

@export
def lookup_or_persist(session, table, **kwargs):
    return lookup(session, table, **kwargs) or persist(session, table(**kwargs))

@export
def pk(t):
    return getattr(t, "id{}".format(t.__tablename__))

@export
def batcher(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

@export
def populate(session, t, values, batch_size=100):
    if not isinstance(t, sqlalchemy.schema.Table): t = t.__table__

    for group in grouper(values, batch_size):
        session.execute(
            t.insert(),
            [x for x in group if x is not None]
        )

@export
def temporary_table_like(name, t):
    from sqlalchemy import Column, Table
    columns = [Column(desc.name, desc.type) for desc in t.__table__.columns]
    return Table(name, t.metadata, *columns, prefixes=["TEMPORARY"])

@export
@contextmanager
def temptable_scope(session, klass):
    from sqlalchemy import Table
    
    if isinstance(klass, Table):
        tbl = klass
    else:
        tbl = klass.__table__

    try:
        tbl.create(bind=session.bind)
    except:
        tbl.drop(bind=session.bind)
        tbl.create(bind=session.bind)

    try:
        yield klass
    except:
        raise
    else:
        tbl.drop(bind=session.bind)

@export
def build_log(name, level=logging.INFO, nostderr=False, logfile=None):
    fmtstr = str(name) + "|%(levelname)s|%(asctime)-15s|%(message)s"
    fmt = logging.Formatter(fmtstr, datefmt="%Y-%m-%dT%H:%M:%S")
    def configure_handler(handler):
        handler.setFormatter(fmt)
        handler.setLevel(level)
        return handler

    log = logging.getLogger(str(name))
    log.setLevel(level)
    if not nostderr:
        log.addHandler(configure_handler(logging.StreamHandler(sys.stderr)))
    if logfile is not None:
        log.addHandler(configure_handler(logging.FileHandler(logfile)))
    if nostderr and (logfile is None):
        log.addHandler(logging.NullHandler())

    return log

@export
def filter_widgets(wq, min_idwidget=None, max_idwidget=None, datasources=[]):
    from .schema import widget as t_w
    from .schema import datasource as t_ds

    wq = wq.join(t_ds, t_ds.iddatasource == t_w.iddatasource)

    if min_idwidget is not None:
        wq = wq.filter(t_w.idwidget >= min_idwidget)
    if max_idwidget is not None:
        wq = wq.filter(t_w.idwidget < max_idwidget)
    if datasources is not None and len(datasources) > 0:
        wq = wq.filter(t_ds.name.in_(datasources))

    return wq

@export
def upload_widget_features(session, widget_features):
    from .schema import widget_feature as t_wf
    from .schema import widget as t_w
    from .schema import datasource as t_ds
    from .schema import feature as t_f
    from .schema import feature_set as t_fs

    from . import schema

    from sqlalchemy.sql.expression import func, literal
    from sqlalchemy import Column, tuple_

    if session.bind.dialect.name.startswith("mysql"):
        session.execute("SET @@foreign_key_checks=0;")

    class tmp_upload(schema.TableBase):
        idtmp_upload = Column(t_f.idfeature.type, nullable=False, primary_key=True)
        widget = Column(t_w.uuid.type, nullable=False)
        idfeature = Column(t_f.idfeature.type, nullable=False)
        idfeature_set = Column(t_fs.idfeature_set.type, nullable=False)
        iddatasource = Column(t_w.iddatasource.type, nullable=False)
        __table_args__ = ({'prefixes':["TEMPORARY"], 'keep_existing':True},)
        __tablename__ = "tmp_upload"

    dialect = session.bind.dialect.name
    
    with temptable_scope(session, tmp_upload):
        session.bulk_insert_mappings(tmp_upload, widget_features)

#        session.execute(
#            t_wf.__table__.delete().where(
#                tuple_(t_wf.idwidget, t_wf.idfeature).in_(
#                    session.query(t_wf.idwidget, t_wf.idfeature) \
#                        .select_from(tmp_upload) \
#                        .join(t_w, t_w.uuid == tmp_upload.widget) \
#                        .join(t_wf, t_wf.idwidget == t_w.idwidget) \
#                        .join(t_f, t_f.idfeature == t_wf.idfeature) \
#                        .filter(t_f.idfeature_set == tmp_upload.idfeature_set) \
#                        .filter(t_w.iddatasource == tmp_upload.iddatasource)
#        )))
        session.execute(
            insert_ignore(t_w, dialect).from_select(
                [t_w.uuid, t_w.iddatasource],
                session.query(tmp_upload.widget, tmp_upload.iddatasource)
        ))
        session.execute(
            insert_ignore(t_wf, dialect).from_select(
                [t_wf.idwidget, t_wf.idfeature],
                session.query(t_w.idwidget, tmp_upload.idfeature) \
                    .select_from(tmp_upload) \
                    .join(t_w, t_w.uuid == tmp_upload.widget) \
                    .filter(t_w.iddatasource == tmp_upload.iddatasource)
        ))
    tmp_upload.metadata.remove(tmp_upload.__table__)
    if session.bind.dialect.name.startswith("mysql"):
        session.execute("SET @@foreign_key_checks=1;")
