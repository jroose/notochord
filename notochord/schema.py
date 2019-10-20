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
# Coherence Store
################################################################################

@export
class coherence_store(TableBase):
    idcoherence_store = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(Unicode(STRING_LENGTH), nullable=False)
    uri = Column(String(STRING_LENGTH), nullable=True)
    kwargs = Column(LargeBinary(BLOB_LENGTH), nullable=True)
    __table_args__ = (UniqueConstraint('name'),)



################################################################################
# App, Execution, Context
################################################################################

@export
class app(TableBase):
    idapp = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(Unicode(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'),)

@export
class execution(TableBase):
    idexecution = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idapp = Column(Integer, ForeignKey('app.idapp', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    uuid = Column(String(34), nullable=False)
    config = Column(LargeBinary(BLOB_LENGTH), nullable=True)
    start_time = Column(UtcDateTime(timezone=True), default=utcnow(), nullable=False)
    end_time = Column(UtcDateTime(timezone=True), nullable=True)
    __table_args__ = (UniqueConstraint('uuid'),)

@export
class context(TableBase):
    idcontext = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idexecution = Column(Integer, ForeignKey('execution.idexecution', onupdate='RESTRICT', ondelete='CASCADE'), nullable=True)
    uuid = Column(String(34), nullable=False)
    start_time = Column(UtcDateTime(timezone=True), default=utcnow(), nullable=False)
    end_time = Column(UtcDateTime(timezone=True), nullable=True)



################################################################################
# Widget
################################################################################

@export
class widget_label_set(TableBase):
    idwidget_label_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'), Index('idxwidget_label_set_name', 'name'))

@export
class widget_label(TableBase):
    idwidget_label = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idwidget_label_set = Column(Integer, ForeignKey('widget_label_set.idwidget_label_set', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('idwidget_label_set', 'name'), Index('idxwidget_label_idwidget_label_set_name', 'idwidget_label_set', 'name'))

@export
class widget_tag(TableBase):
    idwidget = Column(Integer, ForeignKey('widget.idwidget', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)
    idwidget_label = Column(Integer, ForeignKey('widget_label.idwidget_label', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)

@export
class widget_set(TableBase):
    idwidget_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idobject_store = Column(Integer, ForeignKey('object_store.idobject_store', onupdate='RESTRICT', ondelete='CASCADE'), nullable=True)
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
class feature_label_set(TableBase):
    idfeature_label_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'), Index('idxfeature_label_set_name', 'name'))

@export
class feature_label(TableBase):
    idfeature_label = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idfeature_label_set = Column(Integer, ForeignKey('feature_label_set.idfeature_label_set', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('idfeature_label_set', 'name'), Index('idxfeature_label_idfeature_label_set_name', 'idfeature_label_set', 'name'))

@export
class feature_tag(TableBase):
    idfeature = Column(Integer, ForeignKey('feature.idfeature', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)
    idfeature_label = Column(Integer, ForeignKey('feature_label.idfeature_label', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)

@export
class feature_set(TableBase):
    idfeature_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=True)
    idcoherence_store = Column(Integer, ForeignKey('coherence_store.idcoherence_store', onupdate='RESTRICT', ondelete='CASCADE'), nullable=True)
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
class model_label_set(TableBase):
    idmodel_label_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'), Index('idxmodel_label_set_name', 'name'))

@export
class model_label(TableBase):
    idmodel_label = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    idmodel_label_set = Column(Integer, ForeignKey('model_label_set.idmodel_label_set', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('idmodel_label_set', 'name'), Index('idxmodel_label_idmodel_label_set_name', 'idmodel_label_set', 'name'))

@export
class model_tag(TableBase):
    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)
    idmodel_label = Column(Integer, ForeignKey('model_label.idmodel_label', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False, primary_key=True)

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























































