from ..Model import ModelSet, Model

from . import FeaturePopulatedTestCase, datadir_session
from .. import export, schema, grouper, get_utcnow, persist, lookup
import unittest
import time
import random
import json
import datetime
import iso8601
import itertools
import numpy as np

__all__ = []

class FakeAlg(object):
    def __init__(self, a, b, c):
        self.params = dict(a=a, b=b, c=c)
        self.weights = None

    def fit(self, weights):
        self.weights = weights

    def get_params(self):
        return self.params

    def set_params(self, **kwargs):
        self.params.update(kwargs)

@export
class TestModel(FeaturePopulatedTestCase):
    def setUp(self):
        super(TestModel, self).setUp()
        self.session = datadir_session(self.result_dir, singleton=True)
        self.fake_hyper = json.loads(json.dumps(dict(a=5, b=10, c=20)))
        self.fake_weights = [2,4,6,8]
        self.fake_algorithm = FakeAlg
        self.test_metric = "TEST_METRIC"
        self.test_model_set_name = "test_model"

        #Generate training widgets
        t_w = schema.widget
        self.training_widgets = self.session.query(t_w.idwidget).filter(t_w.idwidget % 2 == 0).order_by((t_w.idwidget + 5) % 11, t_w.idwidget)
        self.expected_training_widgets = [x[0] for x in self.training_widgets]
        assert(len(self.expected_training_widgets) > 0)

        #Generate validation widgets
        t_w = schema.widget
        self.validation_widgets = self.session.query(t_w.idwidget).filter(t_w.idwidget % 2 == 1).order_by((t_w.idwidget + 7) % 3, t_w.idwidget)
        self.expected_validation_widgets = [x[0] for x in self.validation_widgets]
        assert(len(self.expected_validation_widgets) > 0)

        t_f = schema.feature
        t_fs = schema.feature_set
        idfs_words = lookup(self.session, t_fs, name="title_bag_of_words").idfeature_set
        self.input_features = self.session.query(t_f.idfeature).filter_by(idfeature_set=idfs_words).order_by((t_f.idfeature + 3) % 7, t_f.idfeature)
        self.expected_input_features = [x[0] for x in self.input_features]
        assert(len(self.expected_input_features) > 0)

        num_predicted_features = 5
        fs_predict = persist(self.session, t_fs(name='TestModel_predicted_features'))
        self.expected_output_features = []
        for it in xrange(num_predicted_features):
            self.expected_output_features.append(persist(self.session, t_f(name=u"output_feature#{}".format(it), idfeature_set=fs_predict.idfeature_set)).idfeature)

        self.output_features = self.session.query(t_f.idfeature).filter_by(idfeature_set = fs_predict.idfeature_set).order_by((t_f.idfeature+7) % 13, t_f.idfeature)

        self.prediction_widgets = self.session.query(t_w.idwidget).filter(t_w.idwidget % 2 == 0).order_by((t_w.idwidget + 17) % 23, t_w.idwidget)
        self.predictions = [(idw[0], [random.random() for it_pf in xrange(num_predicted_features)]) for idw in self.prediction_widgets]

    def runTest(self):
        t_alg = schema.algorithm

        self.log.info("Creating a new model_set")
        model_set = ModelSet(self.session, self.test_model_set_name)
        expected_model_id = model_set.idmodel_set
        del model_set

        self.log.info("Looking up created model_set")
        model_set = ModelSet(self.session, self.test_model_set_name)
        self.assertEqual(model_set.idmodel_set, expected_model_id)

        self.log.info("Creating a new model_set edition")
        fake_alg = FakeAlg(**self.fake_hyper)
        m = Model.new_from_model_set(model_set, fake_alg, self.input_features, self.output_features)
        expected_me_id = m.idmodel
        self.assertEqual(m.hyperparameters, self.fake_hyper)
        self.assertEqual(m.status, "new")
        self.assertEqual(m.algorithm, FakeAlg.__name__)

        self.log.info("Adding training widgets")
        m.select_training_widgets(self.training_widgets)
        result = [x.idwidget for x in m.query_training_widgets()]
        self.assertEqual(result, self.expected_training_widgets)
        self.assertEqual(m.count_training_widgets(), len(self.expected_training_widgets))

        self.log.info("Adding validation widgets")
        m.select_validation_widgets(self.validation_widgets)
        result = [x.idwidget for x in m.query_validation_widgets()]
        self.assertEqual(result, self.expected_validation_widgets)
        self.assertEqual(m.count_validation_widgets(), len(self.expected_validation_widgets))

        self.log.info("Adding training features")
        result = [x.idfeature for x in m.query_input_features()]
        self.assertEqual(result, self.expected_input_features)
        self.assertEqual(m.count_input_features(), len(self.expected_input_features))

        self.log.info("Adding predicts features")
        result = [x.idfeature for x in m.query_output_features()]
        self.assertEqual(result, self.expected_output_features)
        self.assertEqual(m.count_output_features(), len(self.expected_output_features))

        self.log.info("Checking model_set.latest is None")
        self.assertEqual(model_set.latest_model(), None)

        self.log.info("Setting model_set edition as trained")
        start_time = get_utcnow()
        time.sleep(1)
        m.set_trained(fake_alg)
        self.session.flush()
        time.sleep(1)
        end_time = get_utcnow()

        self.log.info("Looking up and checking last trained model_set edition")
        del m
        m = model_set.latest_model()
        self.assertEqual(expected_me_id, m.idmodel_set)
        self.assertEqual(m.hyperparameters, self.fake_hyper)
        self.assertEqual(m.algorithm, FakeAlg.__name__)
        self.assertEqual(m.package.__dict__, fake_alg.__dict__)
        self.assertEqual(m.status, "trained")
        self.assertLess(start_time, m.trained_time)
        self.assertLess(m.trained_time, end_time)
    
        self.log.info("Settinga nd checking metric")
        m.set_metric(self.test_metric, 16384)
        self.assertEqual(float(m.get_metric(self.test_metric).value), float(16384))

        predictions_copy = [x[1] for x in self.predictions]
        #random.shuffle(predictions_copy)
        m.update_predictions([x[0] for x in self.prediction_widgets], predictions_copy)

        for a,b in itertools.izip(m.get_predictions(self.expected_output_features), sorted(self.predictions, key=lambda x:x[0])):
            self.assertEqual(a[0], b[0])
            np.testing.assert_almost_equal(a[1], b[1])




