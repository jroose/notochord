#!/usr/bin/env python
from . import export, lookup, persist, schema, grouper, insert_ignore, pk, get_utcnow, temporary_table_like, populate, lookup_or_persist, null_log, temptable_scope
import base64
import cPickle as pickle
import json
import uuid
import datetime
import numpy as np
import scipy as sp
import scipy.sparse
import sqlalchemy
import itertools

__all__ = []

@export
class CrossvalidationQuery(object):
    def __init__(self, session, widget_query, num_splits=10, table_name="tmp_widget_random"):
        from sqlalchemy import Column
        from sqlalchemy.sql.expression import func, select

        t_w = schema.widget

        class tmp_widget_random(schema.TableBase):
            idfold = Column(t_w.idwidget.type, nullable=False, primary_key=True)
            idwidget = Column(t_w.idwidget.type, nullable=False, primary_key=True)
            __table_args__ = ({'prefixes':["TEMPORARY"], 'keep_existing':True},)
            __tablename__ = table_name

        self.num_splits = int(num_splits)
        self.session = session
        self.tmp_wr = tmp_widget_random

        self.tmp_wr.__table__.create(bind=self.session.bind)

        if self.session.bind.dialect.name.lower() == "mysql":
            randfunc = func.rand()
        else:
            randfunc = func.random()

        num_rows = widget_query.count()
        row_number_column = func.row_number().over(order_by=randfunc).label('row_number')
        self.session.execute(
            self.tmp_wr.__table__.insert().from_select(
                [self.tmp_wr.idfold, self.tmp_wr.idwidget],
                select([((row_number_column-1) * int(self.num_splits)) / int(num_rows), t_w.idwidget]) \
                    .where(t_w.idwidget.in_(widget_query)) \
        ))


    def __iter__(self):
        from sqlalchemy.sql.expression import func, select

        for it in xrange(self.num_splits):
            train_query = self.session.query(self.tmp_wr.idwidget).filter(self.tmp_wr.idfold != it)
            predict_query = self.session.query(self.tmp_wr.idwidget).filter(self.tmp_wr.idfold == it)
            yield train_query, predict_query

    def __enter__(self):
        return self

    def __exit__(self, etype, value, tb):
        self.tmp_wr.__table__.drop(bind=self.session.bind)
        schema.TableBase.metadata.remove(self.tmp_wr.__table__)
        self.session = None

@export
class Model(object):
    def __init__(self, session, dbinst, log=None):
        self.__dict__.update(dict(
            _dbinst=dbinst,
            session=session,
            log=log or null_log
        ))

    def __getattr__(self, name):
        return getattr(self._dbinst, name)

    def __setattr__(self, name, value):
        raise TypeError("Model instances are immutable")

    def _get_ordered_data(self, t_mw, sparse_inputs=False, batch_size=None, get_labels=True):
        from sqlalchemy.sql.expression import and_

        t_w = schema.widget
        t_wf = schema.widget_feature
        t_mtif = schema.model_trained_on_input_feature
        t_mtof = schema.model_trained_on_output_feature

        if not sparse_inputs:
            def build_matrix(shape, rows, cols, vals):
                arr = np.zeros(shape=shape, dtype=np.float32)
                arr[rows, cols] = vals
                return arr
        else:
            def build_matrix(shape, rows, cols, vals):
                return sp.sparse.csr_matrix((vals, (rows, cols)), dtype=np.float32, shape=shape)

        def build_encoder(iterable):
            encoder = {}
            for it, f in enumerate(iterable):
                encoder[f] = it
            return encoder

        def triples_to_matrix(iterable, shape):
            self.log.info("shape: {}".format(shape))
            rows, cols, vals = [], [], []
            for r, c, v in iterable:
                if len(rows) == 0:
                    self.log.info("rcv: {} {} {}".format(r,c,v))
                rows.append(r)
                cols.append(c)
                vals.append(v)

            return build_matrix(shape, rows, cols, vals)

        if batch_size == 0:
            return

        def next_or_none(iterable, num):
            try:
                return next(iterable)
            except StopIteration:
                return [None] * num

        def pull_many(iterable, n):
            ret = []
            try:
                it = 0
                while n is None or it < n:
                    ret.append(next(iterable))
                    it += 1
            except StopIteration:
                pass
            return ret
                
        input_col_encoder = build_encoder(x.idfeature for x in self.query_input_features())
        num_input_features = len(input_col_encoder)

        widgets = self.session.query(pk(t_mw), t_mw.idwidget) \
            .filter(t_mw.idmodel==self.idmodel) \
            .order_by(pk(t_mw).asc())
        it_widget = iter(widgets)

        while 1:
            widget_chunk = pull_many((x for x in it_widget), batch_size)
            if len(widget_chunk) == 0:
                break
            widget_encoder = dict(((x.idwidget,i) for i, x in enumerate(widget_chunk)))

            q_inputs = self.session.query(t_wf.idwidget, t_wf.idfeature, t_wf.value) \
                .select_from(t_mw) \
                .filter(t_mw.idmodel == self.idmodel) \
                .filter(pk(t_mw) >= widget_chunk[0][0]) \
                .filter(pk(t_mw) <= widget_chunk[-1][0]) \
                .join(t_wf, t_mw.idwidget == t_wf.idwidget) \
                .join(t_mtif, and_(t_mtif.idfeature == t_wf.idfeature, t_mtif.idmodel == self.idmodel)) \
                .filter(t_mtif.idmodel == self.idmodel)

            in_chunk = triples_to_matrix(((widget_encoder[i[0]], input_col_encoder[i[1]], i[2]) for i in q_inputs), (len(widget_chunk), num_input_features))

            if not get_labels:
                yield [x.idwidget for x in widget_chunk], in_chunk
            else:
                raise NotImplementedError("Supervised data not yet implemented")
                #arr_out = build_matrix((count, num_output_features), out_rows, out_cols, out_vals)
                #yield idwidgets, arr_in, arr_out


#        if get_labels:
#            outputs = self.session.query(t_wf.idwidget, t_wf.idfeature, t_wf.value) \
#                .select_from(t_wf) \
#                .join(t_mtof, and_(t_mtof.idfeature == t_wf.idfeature, t_mtof.idmodel == self.idmodel)) \
#                .join(t_mw, and_(t_mw.idwidget == t_wf.idwidget, t_mw.idmodel == self.idmodel)) \
#                .order_by(pk(t_mw).asc())
#            output_col_encoder = build_encoder(x.idfeature for x in self.query_output_features())
#            num_output_features = len(output_col_encoder)


    def get_training_data(self, widgets, sparse_inputs=False, supervised=True, batch_size=None):
        self.select_training_widgets(widgets)
        t_mw = schema.model_trained_on_widget
        num_widgets = self.count_training_widgets()

        if num_widgets > 0:
            for ret in self._get_ordered_data(t_mw, sparse_inputs=sparse_inputs, get_labels=supervised, batch_size=batch_size):
                yield ret

    def get_predict_data(self, widgets, sparse_inputs=False, supervised=True, batch_size=None):
        self.select_predicts_widgets(widgets)
        t_mw = schema.model_predicts_widget
        num_widgets = self.count_predicts_widgets()
        if num_widgets > 0:
            for ret in self._get_ordered_data(t_mw, sparse_inputs=sparse_inputs, get_labels=supervised, batch_size=batch_size):
                yield ret

    def get_validation_data(self, widgets, sparse_inputs=False, supervised=True, batch_size=None):
        self.select_validation_widgets(widgets)
        t_mw = schema.model_validated_on_widget
        num_widgets = self.count_validation_widgets()

        if num_widgets > 0:
            for ret in self._get_ordered_data(t_mw, sparse_inputs=sparse_inputs, get_labels=supervised, batch_size=batch_size):
                yield ret

#    def get_predict_data(self, widgets, sparse_inputs=False):
#        for ret in self._get_data(widgets, sparse_inputs=sparse_inputs, get_labels=False):
#            yield ret

    @classmethod
    def new_from_model_set(klass, algorithm, input_features, output_features=None, params=None, log=None):
        t_m = schema.model
        t_ms = schema.model_status
        t_alg = schema.algorithm

        idalg = lookup_or_persist(model_set.session, t_alg, name=type(algorithm).__name__).idalgorithm

        dbinst = persist(model_set.session, t_m(
                idmodel_set=model_set.idmodel_set, idmodel_status=idms_new, 
                idalgorithm=idalg, uuid=str(uuid.uuid4()),
                hyperparameters=base64.b64encode(pickle.dumps(params or algorithm.get_params()))
            ))

        ret = klass(model_set.session, dbinst, log=log)
        ret.select_input_features(input_features)
        if output_features is not None:
            ret.select_output_features(output_features)
            ret.select_predicts_features(output_features)

        return ret

    def set_trained(self, package):
        t_ms = schema.model_status
        ms_trained = lookup(self.session, t_ms, name="trained")
        self._dbinst.trained_time = get_utcnow()
        self._dbinst.trained_package = base64.b64encode(pickle.dumps(package))
        self._dbinst.idmodel_status = ms_trained.idmodel_status
        persist(self.session, self._dbinst)

    def set_metric(self, metric_type, value):
        t_metric = schema.metric
        t_mt = schema.metric_type
        
        mt = lookup(self.session, t_mt, name=metric_type) \
            or persist(self.session, t_mt(name=metric_type))
        idmt = mt.idmetric_type
        idme = self.idmodel

        metric = lookup(self.session, t_metric, idmodel=idme, idmetric_type=idmt)
        if metric is not None:
            metric.value = value
            persist(self.session, metric)
        else:
            persist(self.session, t_metric(idmodel=idme, idmetric_type=idmt, value=value))

    def get_metric(self, metric):
        t_metric = schema.metric
        t_mt = schema.metric_type

        mt = lookup(self.session, t_mt, name=metric)
        if mt is None:
            raise ValueError("Unknown metric type: '{}'".format(metric))

        return lookup(
                self.session, t_metric, 
                idmodel=self.idmodel, 
                idmetric_type=mt.idmetric_type
            )

    def select_widgets(self, subquery, usage_type):
        from sqlalchemy.sql.expression import select, literal

        t_mw = schema.model_widget
        tt_mw = t_mw.__table__
        t_w = schema.widget
        t_mwt = schema.model_widget_type

        idtype = lookup(self.session, t_mwt, name=usage_type)

        type_of_id = tt_mw.c.idmodel.type
        sq = subquery.subquery('t')
        self.session.execute(tt_mw.insert().from_select(
            [tt_mw.c.idmodel, tt_mw.c.idwidget, tt_mw.c.idmodel_widget_type],
            select([literal(self.idmodel, type_=type_of_id), sq.c.idwidget, literal(idtype, type_=type_of_id)])
        ))

    def select_features(self, subquery, usage_type):
        from sqlalchemy.sql.expression import select, literal

        t_mf = schema.model_feature
        tt_mf = t_mf.__table__
        t_w = schema.feature
        t_mft = schema.model_feature_type

        idtype = lookup(self.session, t_mft, name=usage_type)

        type_of_id = tt_mf.c.idmodel.type
        sq = subquery.subquery('t')
        self.session.execute(tt_mf.insert().from_select(
            [tt_mf.c.idmodel, tt_mf.c.idfeature, tt_mf.c.idmodel_feature_type],
            select([literal(self.idmodel, type_=type_of_id), sq.c.idfeature, literal(idtype, type_=type_of_id)])
        ))

    def select_training_widgets(self, subquery):
        from sqlalchemy.sql.expression import select, literal

        t_mtw = schema.model_trained_on_widget
        tt_mtw = t_mtw.__table__
        t_w = schema.widget

        idtype = tt_mtw.c.idmodel.type
        sq = subquery.subquery('t')
        self.session.execute(tt_mtw.insert().from_select(
            [tt_mtw.c.idmodel, tt_mtw.c.idwidget],
            select([literal(self.idmodel, type_=idtype), sq.c.idwidget])
        ))

    def select_predicts_widgets(self, subquery):
        from sqlalchemy.sql.expression import select, literal

        t_mpw = schema.model_predicts_widget
        tt_mpw = t_mpw.__table__
        t_w = schema.widget

        idtype = tt_mpw.c.idmodel.type
        sq = subquery.subquery('t')
        self.session.execute(tt_mpw.insert().from_select(
            [tt_mpw.c.idmodel, tt_mpw.c.idwidget],
            select([literal(self.idmodel, type_=idtype), sq.c.idwidget])
        ))

    def select_validation_widgets(self, subquery):
        from sqlalchemy.sql.expression import select, literal

        t_mvw = schema.model_validated_on_widget
        tt_mvw = t_mvw.__table__
        t_w = schema.widget

        idtype = tt_mvw.c.idmodel.type
        sq = subquery.subquery('t')
        self.session.execute(tt_mvw.insert().from_select(
            [tt_mvw.c.idmodel, tt_mvw.c.idwidget],
            select([literal(self.idmodel, type_=idtype), sq.c.idwidget])
        ))

    def select_input_features(self, subquery):
        from sqlalchemy.sql.expression import select, literal

        t_mtif = schema.model_trained_on_input_feature
        tt_mtif = t_mtif.__table__
        t_f = schema.feature

        idtype = tt_mtif.c.idmodel.type
        sq = subquery.subquery('t')
        self.session.execute(
            insert_ignore(t_mtif).from_select(
                [t_mtif.idmodel, t_mtif.idfeature],
                select([literal(self.idmodel, type_=idtype), sq.c.idfeature])
        ))

    def select_output_features(self, subquery):
        from sqlalchemy.sql.expression import select, literal

        t_mtof = schema.model_trained_on_output_feature
        tt_mtof = t_mtof.__table__
        t_f = schema.feature

        idtype = tt_mtof.c.idmodel.type
        sq = subquery.subquery('t')
        self.session.execute(
            insert_ignore(t_mtof).from_select(
                [t_mtof.idmodel, t_mtof.idfeature],
                select([literal(self.idmodel, type_=idtype), sq.c.idfeature])
        ))

    def select_predicts_features(self, subquery):
        from sqlalchemy.sql.expression import select, literal, text, and_
        from sqlalchemy.orm import aliased

        t_f = schema.feature
        t_fs = schema.feature_set

        idtype = t_fs.idmodel.type
        idmodel = literal(self.idmodel, type_=idtype)

        old_fs = self.session.query(idmodel, t_fs.name) \
                .join(t_f, t_f.idfeature_set == t_fs.idfeature_set) \
                .filter(t_f.idfeature.in_(subquery.subquery('t'))) \
                .distinct()

        self.session.execute(
            insert_ignore(t_fs).from_select(
                [t_fs.idmodel, t_fs.name],
                old_fs.subquery()
        ))

        nt_f = schema.feature
        nt_fs = schema.feature_set
        t_f = aliased(schema.feature)
        t_fs = aliased(schema.feature_set)

        q = self.session.query(nt_fs.idfeature_set, t_f.name) \
                    .select_from(t_f) \
                    .filter(t_f.idfeature.in_(subquery.subquery('t'))) \
                    .join(t_fs, t_fs.idfeature_set == t_f.idfeature_set) \
                    .join(nt_fs, and_(t_fs.name == nt_fs.name, nt_fs.idmodel == idmodel))

        self.session.execute(
            insert_ignore(nt_f).from_select(
                [nt_f.__table__.c.idfeature_set, nt_f.__table__.c.name],
                q
        ))

#        self.session.execute(
#            insert_ignore(t_mpf).from_select(
#                [t_mpf.idmodel, t_mpf.idfeature],
#                select([idmodel, t_f.idfeature])
#                    .select_from(t_f)
#                    .join(t_fs, t_fs.idfeature_set == t_fidfeature_set)
#                    .filter(t_fs.idmodel == t_f.idmodel)
#        ))

    def query_training_widgets(self):
        t_mtw = schema.model_trained_on_widget

        return self.session.query(t_mtw) \
            .filter_by(idmodel=self.idmodel) \
            .order_by(pk(t_mtw).asc())

    def query_predicts_widgets(self):
        t_mpw = schema.model_predicts_widget

        return self.session.query(t_mpw) \
            .filter_by(idmodel=self.idmodel) \
            .order_by(pk(t_mpw).asc())

    def query_validation_widgets(self):
        t_mvw = schema.model_validated_on_widget

        return self.session.query(t_mvw) \
            .filter_by(idmodel=self.idmodel) \
            .order_by(pk(t_mvw).asc())

    def query_input_features(self):
        t_mtif = schema.model_trained_on_input_feature

        return self.session.query(t_mtif) \
            .filter_by(idmodel=self.idmodel) \
            .order_by(pk(t_mtif).asc())

    def query_output_features(self):
        t_mtof = schema.model_trained_on_output_feature

        return self.session.query(t_mtof) \
            .filter_by(idmodel=self.idmodel) \
            .order_by(pk(t_mtof).asc())

    def query_predicts_features(self):
        t_f = schema.feature
        t_fs = schema.feature_set
        return self.session.query(t_f.idfeature) \
            .join(t_fs, t_fs.idfeature_set == t_f.idfeature_set) \
            .filter(t_fs.idmodel == self.idmodel) \
            .order_by(t_f.idfeature.asc())

    def count_training_widgets(self):
        from sqlalchemy.sql.expression import func, distinct
        t_mtw = schema.model_trained_on_widget
        return self.session.query(func.count(distinct(t_mtw.idwidget))).filter_by(idmodel=self.idmodel).scalar()

    def count_predicts_widgets(self):
        from sqlalchemy.sql.expression import func, distinct
        t_mpw = schema.model_trained_on_widget
        return self.session.query(func.count(distinct(t_mpw.idwidget))).filter_by(idmodel=self.idmodel).scalar()

    def count_validation_widgets(self):
        from sqlalchemy.sql.expression import func, distinct
        t_mvw = schema.model_validated_on_widget
        return self.session.query(func.count(distinct(t_mvw.idwidget))).filter_by(idmodel=self.idmodel).scalar()

    def count_input_features(self):
        from sqlalchemy.sql.expression import func, distinct
        t_mtif = schema.model_trained_on_input_feature
        return self.session.query(func.count(distinct(t_mtif.idfeature))).filter_by(idmodel=self.idmodel).scalar()

    def count_output_features(self):
        from sqlalchemy.sql.expression import func, distinct
        t_mtof = schema.model_trained_on_output_feature
        return self.session.query(func.count(distinct(t_mtof.idfeature))).filter_by(idmodel=self.idmodel).scalar()

    def count_predicts_features(self):
        from sqlalchemy.sql.expression import func, distinct
        t_f = schema.feature
        t_fs = schema.feature_set
        return self.session.query(func.count(distinct(t_f.idfeature))) \
            .join(t_fs, t_fs.idfeature_set == t_f.idfeature_set) \
            .filter(t_fs.idmodel == self.idmodel)

    def add_training_widgets(self, widgets):
        t_mtw = schema.model_trained_on_widget
        for batch in grouper(widgets, 490):
            self.session.execute(
                insert_ignore(t_mtw),
                [dict(idmodel=self.idmodel, idwidget=x) for x in batch if x is not None]
            )

    def add_predicts_widgets(self, widgets):
        t_mpw = schema.model_trained_on_widget
        for batch in grouper(widgets, 490):
            self.session.execute(
                insert_ignore(t_mpw),
                [dict(idmodel=self.idmodel, idwidget=x) for x in batch if x is not None]
            )

    def add_validation_widgets(self, widgets):
        t_mvw = schema.model_validated_on_widget
        for batch in grouper(widgets, 490):
            self.session.execute(
                insert_ignore(t_mvw),
                [dict(idmodel=self.idmodel, idwidget=x) for x in batch if x is not None]
            )

    def add_input_features(self, features):
        t_mtif = schema.model_trained_on_input_feature
        for batch in grouper(features, 490):
            self.session.execute(
                insert_ignore(t_mtif),
                [dict(idmodel=self.idmodel, idfeature=x) for x in batch if x is not None]
            )

    def add_output_features(self, features):
        t_mtof = schema.model_trained_on_output_feature
        for batch in grouper(features, 490):
            self.session.execute(
                insert_ignore(t_mtof),
                [dict(idmodel=self.idmodel, idfeature=x) for x in batch if x is not None]
            )

    def add_predicts_features(self, features):
        t_mpf = schema.model_predictes_feature
        for batch in grouper(features, 490):
            self.session.execute(
                insert_ignore(t_mpf),
                [dict(idmodel=self.idmodel, idfeature=x) for x in batch if x is not None]
            )

    def add_input_features_from_feature_set(self, fs_name):
        t_f = schema.feature
        t_fs = schema.feature_set

        self.select_input_features(
            select([t_f.idfeature])
                .join(t_fs, t_fs.idfeature == t_f.idfeature)
                .where(t_f.name == feature_set)
        )

    def add_output_features_from_feature_set(self, fs_name):
        t_f = schema.feature
        t_fs = schema.feature_set

        self.select_output_features(
            select([t_f.idfeature])
                .join(t_fs, t_fs.idfeature == t_f.idfeature)
                .where(t_f.name == feature_set)
        )

    def add_predicts_features_from_feature_set(self, fs_name):
        t_f = schema.feature
        t_fs = schema.feature_set



        self.select_predicts_features(
            select([t_f.idfeature])
                .join(t_fs, t_fs.idfeature == t_f.idfeature)
                .where(t_f.name == feature_set)
                .where(t_fs.idmodel == self.idmodel)
        )

    def update_predictions(self, widgets, values):
        tuple_ = sqlalchemy.sql.expression.tuple_

        t_wf = schema.widget_feature
        tt_wf = t_wf.__table__

        def rowdicts_generator(widgets, features, values):
            if isinstance(values, np.ndarray):
                if values.ndim == 1:
                    values = values.reshape((values.size, 1))
                self.log.info("rowdicts_generator {} {}".format(values.shape, (len(widgets), len(features))))
                assert tuple(values.shape) == (len(widgets), len(features))
                if values.ndim == 1:
                    #1 dimensional numpy array
                    for it_widget in xrange(len(widgets)):
                        yield dict(
                            idwidget=widgets[it_widget], idfeature=features[0], 
                            value=float(values[it_widget]),
                            idmodel=self.idmodel
                        )
                elif values.ndim == 2:
                    #2 dimensional numpy array
                    for it_widget, it_feature in itertools.product(xrange(len(widgets)), xrange(len(features))):
                        yield dict(
                            idwidget=widgets[it_widget], idfeature=features[it_feature], 
                            value=float(values[it_widget, it_feature]),
                            idmodel=self.idmodel
                        )
            elif isinstance(values, sp.sparse.spmatrix):
                #2 dimensional scipy matrix
                values = values.tocoo()
                if (widgets is None) and (features is None):
                    for it_nnz in xrange(values.nnz):
                        yield dict(
                            idwidget=values.row[it_nnz], 
                            idfeature=values.col[it_nnz],
                            value=float(values.data[it_nnz]),
                            idmodel=self.idmodel
                        )
                else:
                    for it_nnz in xrange(values.nnz):
                        yield dict(
                            idwidget=widgets[values.row[it_nnz]], 
                            idfeature=features[values.col[it_nnz]],
                            value=float(values.data[it_nnz]),
                            idmodel=self.idmodel
                        )
            else:
                #List of lists
                for it_widget, widget_values in enumerate(values):
                    for it_feature, value in enumerate(widget_values):
                        yield dict(
                            idwidget=widgets[it_widget], idfeature=features[it_feature], 
                            value=float(value), 
                            idmodel=self.idmodel
                        )

        tmp_wf = temporary_table_like("tmp_widget_feature", t_wf)
        with temptable_scope(self.session, tmp_wf):
            predicts_features = [x.idfeature for x in self.query_predicts_features()]
            populate(self.session, tmp_wf, rowdicts_generator(widgets, predicts_features, values), batch_size=200)

            t_wf_keys = tuple_(*tt_wf.primary_key)
            tmp_wf_keys = [getattr(tmp_wf.c, k.name) for k in tt_wf.primary_key]

            from sqlalchemy.sql.expression import select
            self.session.execute(tt_wf.delete().where(t_wf_keys == select(tmp_wf_keys)))
            self.session.execute(tt_wf.insert().from_select(tt_wf.columns, select(tmp_wf.columns)))

        #tmp_wf.drop(bind=self.session.bind)
        t_wf.metadata.remove(tmp_wf)

    def query_predictions(self):
        t_wf = schema.widget_feature
        
        return self.session.query(t_wf) \
            .filter_by(idmodel=self.idmodel) \
            .order_by(t_wf.idwidget.asc(), t_wf.idfeature.asc())

    def get_predictions(self, features):
        t_wf = schema.widget_feature
        t_fs = schema.feature_set
        t_f = schema.feature

        q = self.session.query(t_wf) \
            .select_from(t_fs) \
            .join(t_f, t_f.idfeature_set == t_fs.idfeature_set) \
            .join(t_wf, t_wf.idfeature == t_f.idfeature) \
            .filter(t_fs.idmodel==self.idmodel) \
            .filter(t_wf.idfeature.in_(features)) \
            .order_by(t_wf.idwidget.asc(), t_wf.idfeature.asc())
        
        feature_index = dict(zip(features, range(len(features))))
        prev_widget = None
        widget_vector = [None for x in xrange(len(features))]
        for p in q:
            if (prev_widget is None):
                prev_widget = p.idwidget
            elif(prev_widget != p.idwidget):
                yield prev_widget, widget_vector
                prev_widget = p.idwidget

            widget_vector[feature_index[p.idfeature]] = p.value

        if prev_widget is not None:
            yield prev_widget, widget_vector

    @property
    def status(self):
        t_ms = schema.model_status
        return self.session.query(t_ms.name).filter_by(idmodel_status=self.idmodel_status).one()[0]

    @property
    def algorithm(self):
        t_alg = schema.algorithm
        return self.session.query(t_alg.name).filter_by(idalgorithm=self.idalgorithm).one()[0]

    @property
    def package(self):
        return pickle.loads(base64.b64decode(self._dbinst.trained_package))

    @property
    def hyperparameters(self):
        return pickle.loads(base64.b64decode(self._dbinst.hyperparameters))

