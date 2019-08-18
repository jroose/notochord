from sqlalchemy import Column, Integer, String, Float, ForeignKey, UnicodeText, Unicode, LargeBinary, Boolean, Index
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy_utc import UtcDateTime, utcnow
from sqlalchemy.orm.session import object_session
from .util import export

STRING_LENGTH=512

__all__ = []

@as_declarative()
@export
class TableBase(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

@export
class datasource(TableBase):
    iddatasource = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(Unicode(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'),)

@export
class widget(TableBase):
    idwidget = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    iddatasource = Column(Integer, ForeignKey('datasource.iddatasource', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    insert_time = Column(UtcDateTime(timezone=True), default=utcnow(), nullable=False)
    uuid = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('iddatasource', 'uuid'), Index('idxwidget_idatasource_uuid', 'iddatasource', 'uuid'), Index('idxwidget_iddatasource', 'iddatasource'))

@export
class object_store(TableBase):
    idobject_store = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(Unicode(STRING_LENGTH), nullable=False)
    uri = Column(String(STRING_LENGTH), nullable=True)
    kwargs = Column(String(STRING_LENGTH), nullable=True)
    __table_args__ = (UniqueConstraint('name'),)

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

@export
class widget_feature(TableBase):
    idwidget = Column(Integer, ForeignKey('widget.idwidget', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)
    idfeature = Column(Integer, ForeignKey('feature.idfeature', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)
    value = Column(Float, nullable=True)
    __table_args__ = ( \
#        Index('idxwidget_feature_idwidget', 'idwidget'), \
#        Index('idxwidget_feature_idfeature','idfeature'), \
    )

#@export
#class algorithm(TableBase):
#    idalgorithm = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
#    name = Column(String(STRING_LENGTH), nullable=False)
#    __table_args__ = (UniqueConstraint('name'),)
#
#@export
#class model_status(TableBase):
#    idmodel_status = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
#    name = Column(String(STRING_LENGTH), nullable=False)
#    __table_args__ = (UniqueConstraint('name'),)
#
#@export
#class model_set(TableBase):
#    idmodel_set = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
#    name = Column(String(STRING_LENGTH), nullable=False)
#    __table_args__ = (UniqueConstraint('name'),)

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
#    idmodel_set = Column(Integer, ForeignKey('model_set.idmodel_set', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    idalgorithm = Column(Integer, ForeignKey('algorithm.idalgorithm', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    idmodel_status = Column(Integer, ForeignKey('model_status.idmodel_status', onupdate='RESTRICT', ondelete='RESTRICT'), nullable=False)
    insert_time = Column(UtcDateTime(timezone=True), default=utcnow(), nullable=False)
    uuid = Column(String(34), nullable=False)
    hyperparameters = Column(LargeBinary(STRING_LENGTH), nullable=False)
    trained_time = Column(UtcDateTime(timezone=True), nullable=True)
    trained_package = Column(LargeBinary(STRING_LENGTH), nullable=True)
    __table_args__ = (UniqueConstraint('uuid'),Index('idxmodel_uuid', 'uuid'))

@export
class model_feature_type(TableBase):
    idmodel_feature_type = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'),)

@export
class model_feature(TableBase):
    idmodel_feature = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False)
    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    idfeature = Column(Integer, ForeignKey('feature.idfeature', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    idmodel_feature_type = Column(Integer, ForeignKey('model_feature_type.idmodel_feature_type', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)

@export
class model_widget_type(TableBase):
    idmodel_widget_type = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'),)

@export
class model_widget(TableBase):
    idmodel_widget = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False)
    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    idwidget = Column(Integer, ForeignKey('widget.idwidget', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
    idmodel_widget_type = Column(Integer, ForeignKey('model_widget_type.idmodel_widget_type', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)

#@export
#class model_trained_on_widget(TableBase):
#    #Primary key allows us to know the order of the widgets
#    idmodel_trained_on_widget = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
#    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    idwidget = Column(Integer, ForeignKey('widget.idwidget', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    __table_args__ = (UniqueConstraint('idmodel', 'idwidget'),Index('idxmodel_trained_on_widget_idmodel', 'idmodel'))
#
#@export
#class model_predicts_widget(TableBase):
#    #Primary key allows us to know the order of the widgets
#    idmodel_predicts_widget = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
#    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    idwidget = Column(Integer, ForeignKey('widget.idwidget', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    __table_args__ = (UniqueConstraint('idmodel', 'idwidget'),Index('idxmodel_predicts_widget_idmodel', 'idmodel'))
#
#@export
#class model_validated_on_widget(TableBase):
#    #Primary key allows us to know the order of the widgets
#    idmodel_validated_on_widget = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
#    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    idwidget = Column(Integer, ForeignKey('widget.idwidget', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    __table_args__ = (UniqueConstraint('idmodel', 'idwidget'),Index('idxmodel_validated_on_widget_idmodel', 'idmodel'))
#
#@export
#class model_trained_on_input_feature(TableBase):
#    #Primary key allows us to know the order of the features
#    idmodel_trained_on_input_feature = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
#    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    idfeature = Column(Integer, ForeignKey('feature.idfeature', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    __table_args__ = (UniqueConstraint('idmodel', 'idfeature'),Index('idxmodel_trained_on_input_feature_idmodel', 'idmodel'))
#
#@export
#class model_trained_on_output_feature(TableBase):
#    #Primary key allows us to know the order of the features
#    idmodel_trained_on_output_feature = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
#    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    idfeature = Column(Integer, ForeignKey('feature.idfeature', onupdate='RESTRICT', ondelete='CASCADE'), nullable=False)
#    __table_args__ = (UniqueConstraint('idmodel', 'idfeature'),Index('idxmodel_trained_on_output_feature_idmodel', 'idmodel'))

@export
class metric(TableBase):
    idmodel = Column(Integer, ForeignKey('model.idmodel', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)
    idmetric_type = Column(Integer, ForeignKey('metric_type.idmetric_type', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)
    value = Column(Float, nullable=True)

@export
class metric_type(TableBase):
    idmetric_type = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    name = Column(String(STRING_LENGTH), nullable=False)
    __table_args__ = (UniqueConstraint('name'),)
