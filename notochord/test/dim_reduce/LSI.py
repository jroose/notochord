from ...dim_reduce.LSI import *
from .. import FeaturePopulatedTestCase, default_test_config
from ... import export
import unittest

__all__ = []

config = default_test_config['LSITrain']

@export
class TestLSITrainConstructDestroy(FeaturePopulatedTestCase):
    def runTest(self):
        app = LSITrain.from_args([
            "--datadir={}".format(self.result_dir),
#            "--input-feature-set={}".format(config['input_feature_set']),
#            "--output-feature-set={}".format(config['output_feature_set']),
#            "--n-components={}".format(config['hyperparameters']['n_components'])
        ])
        del app

@export
class TestLSITrainConstructDestroyExplicit(FeaturePopulatedTestCase):
    def runTest(self):
        app = LSITrain.from_args([
            "--datadir={}".format(self.result_dir),
            "--input-feature-set={}".format(config['input_feature_set']),
            "--output-feature-set={}".format(config['output_feature_set']),
            "--n-components={}".format(config['hyperparameters']['n_components'])
        ])
        del app

@export
class TestLSITrain_Run(FeaturePopulatedTestCase):
    def runTest(self):
        app = LSITrain(self.result_dir, log=self.log)
        app.main()
