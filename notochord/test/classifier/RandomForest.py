from ...classifier.RandomForest import *
from .. import LSIPopulatedTestCase
from ... import export
import unittest

__all__ = []

@export
class TestRandomForestTrainConstructDestroy(LSIPopulatedTestCase):
    def runTest(self):
        app = RandomForestTrain.from_args(["--datadir={}".format(self.result_dir), "RandomForestUnitTest"])
        del app

@export
class TestRandomForestTrainConstructDestroyExplicit(LSIPopulatedTestCase):
    def runTest(self):
        app = RandomForestTrain(self.result_dir)
        del app

@export
class TestRandomForestTrain_Run(LSIPopulatedTestCase):
    def runTest(self):
        app = RandomForestTrain(self.result_dir, log=self.log)
        app.main()
        session = app.get_session()
