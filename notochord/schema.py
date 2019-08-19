from sqlalchemy import Column, Integer, String, Float, ForeignKey, UnicodeText, Unicode, LargeBinary, Boolean, Index
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy_utc import UtcDateTime, utcnow
from sqlalchemy.orm.session import object_session
from .util import export

STRING_LENGTH=512
BLOB_LENGTH=1024*1024

__all__ = []

################################################################################
# TableBase
################################################################################

@as_declarative()
@export
class TableBase(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()



################################################################################
# Object Store
################################################################################

@export
class object_store(TableBase):
    idobject_store = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(Unicode(STRING_LENGTH), nullable=False)
    uri = Column(String(STRING_LENGTH), nullable=True)
    kwargs = Column(LargeBinary(BLOB_LENGTH), nullable=True)
    __table_args__ = (UniqueConstraint('name'),)



################################################################################
# Context
################################################################################

@export
class context(TableBase):
    idcontext = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    uuid = Column(String(34), nullable=False)
    start_time = Column(UtcDateTime(timezone=True), default=utcnow(), nullable=False)
    end_time = Column(UtcDateTime(timezone=True), nullable=True)



################################################################################
# Widget
################################################################################

@export
class wtag_set(TableBase):
    idwtag_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'), Index('idxwtag_set_name', 'name'))

@export
class wtag(TableBase):
    idwtag = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idwtag_set = Column(Integer, ForeignKey('wtag_set.idwtag_set', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('idwtag_set', 'name'), Index('idxwtag_idwtag_set_name', 'idwtag_set', 'name'))

@export
class widget_wtag(TableBase):
    idwidget = Column(Integer, ForeignKey('widget.idwidget', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)
    idwtag = Column(Integer, ForeignKey('wtag.idwtag', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)

@export
class widget_set(TableBase):
    idwidget_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(Unicode(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'),)

@export
class widget(TableBase):
    idwidget = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idcontext_created = Column(Integer, ForeignKey('context.idcontext', onupdate='RESTRICT', ondelete='RESTRICT'), nullable=False)
    idwidget_set = Column(Integer, ForeignKey('widget_set.idwidget_set', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    insert_time = Column(UtcDateTime(timezone=True), default=utcnow(), nullable=False)
    uuid = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('idwidget_set', 'uuid'), Index('idxwidget_iwidget_set_uuid', 'idwidget_set', 'uuid'), Index('idxwidget_idwidget_set', 'idwidget_set'))



################################################################################
# Feature
################################################################################

@export
class ftag_set(TableBase):
    idftag_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'), Index('idxftag_set_name', 'name'))

@export
class ftag(TableBase):
    idftag = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idftag_set = Column(Integer, ForeignKey('ftag_set.idftag_set', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('idftag_set', 'name'), Index('idxftag_idftag_set_name', 'idftag_set', 'name'))

@export
class feature_ftag(TableBase):
    idfeature = Column(Integer, ForeignKey('feature.idfeature', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)
    idftag = Column(Integer, ForeignKey('ftag.idftag', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)

@export
class feature_set(TableBase):
    idfeature_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=True)
    idobject_store = Column(Integer, ForeignKey('object_store.idobject_store', onupdate='RESTRICT', ondelete='CASCADE'), nullable=True)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('idmodel', 'name'),)

@export
class feature(TableBase):
    idfeature = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idfeature_set = Column(Integer, ForeignKey('feature_set.idfeature_set', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    name = Column(Unicode(STRING_LENGTH), nullable=False)
    __table_args__ = (
        UniqueConstraint('idfeature_set', 'name'),
        Index('idxfeature_idfeatureset_name', 'idfeature_set', 'name')
    )



################################################################################
# Model
################################################################################

@export
class mtag_set(TableBase):
    idmtag_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'), Index('idxmtag_set_name', 'name'))

@export
class mtag(TableBase):
    idmtag = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idmtag_set = Column(Integer, ForeignKey('mtag_set.idmtag_set', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('idmtag_set', 'name'), Index('idxmtag_idmtag_set_name', 'idmtag_set', 'name'))

@export
class model_mtag(TableBase):
    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)
    idmtag = Column(Integer, ForeignKey('mtag.idmtag', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)

@export
class model(TableBase):
    idmodel = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    uuid = Column(String(34), nullable=False)
    idcontext_created = Column(Integer, ForeignKey('context.idcontext', onupdate='RESTRICT', ondelete='RESTRICT'), nullable=False)
    idcontext_trained = Column(Integer, ForeignKey('context.idcontext', onupdate='RESTRICT', ondelete='RESTRICT'), nullable=False)

    algorithm_name = Column(String(STRING_LENGTH), nullable=True)
    hyperparameters = Column(LargeBinary(BLOB_LENGTH), nullable=False)

    insert_time = Column(UtcDateTime(timezone=True), default=utcnow(), nullable=False)
    trained_time = Column(UtcDateTime(timezone=True), nullable=True)

    trained_package = Column(LargeBinary(BLOB_LENGTH), nullable=True)
    __table_args__ = (UniqueConstraint('uuid'),Index('idxmodel_uuid', 'uuid'))



################################################################################
# Metric
################################################################################

@export
class metric(TableBase):
    idmetric = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    name = Column(String(STRING_LENGTH), nullable=False)

    __table_args__ = (UniqueConstraint('name'),)



################################################################################
# Widget X Feature
################################################################################

@export
class widget_feature(TableBase):
    idwidget = Column(Integer, ForeignKey('widget.idwidget', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)
    idfeature = Column(Integer, ForeignKey('feature.idfeature', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)

    value = Column(Float, nullable=True)



################################################################################
# Model X Feature
################################################################################

@export
class model_feature_type(TableBase):
    idmodel_feature_type = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    name = Column(String(STRING_LENGTH), nullable=False)

    __table_args__ = (UniqueConstraint('name'),)

@export
class model_feature(TableBase):
    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)
    idfeature = Column(Integer, ForeignKey('feature.idfeature', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)
    idmodel_feature_type = Column(Integer, ForeignKey('model_feature_type.idmodel_feature_type', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)



################################################################################
# Model X Widget
################################################################################

@export
class model_widget_type(TableBase):
    idmodel_widget_type = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    name = Column(String(STRING_LENGTH), nullable=False)

    __table_args__ = (UniqueConstraint('name'),)

@export
class model_widget(TableBase):
    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)
    idwidget = Column(Integer, ForeignKey('widget.idwidget', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)
    idmodel_widget_type = Column(Integer, ForeignKey('model_widget_type.idmodel_widget_type', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)

    idcontext = Column(Integer, ForeignKey('context.idcontext', onupdate='RESTRICT', ondelete='RESTRICT'), nullable=False)
    insert_time = Column(UtcDateTime(timezone=True), default=utcnow(), nullable=False)



################################################################################
# Model X Context X Metric
################################################################################

@export
class model_context_metric(TableBase):
    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)
    idmetric = Column(Integer, ForeignKey('metric.idmetric', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)
    idcontext = Column(Integer, ForeignKey('context.idcontext', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)

    value = Column(Float, nullable=True)
    insert_time = Column(UtcDateTime(timezone=True), default=utcnow(), nullable=False)























































