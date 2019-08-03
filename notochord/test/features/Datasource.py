from ...features.Datasource import Datasource
from .. import PopulatedTestCase
from ... import export
import unittest

__all__ = []

@export
class TestDatasourceConstructDestroy(PopulatedTestCase):
    def runTest(self):
        app = Datasource.from_args(["--datadir={}".format(self.result_dir)])
        del app

@export
class TestDatasourceConstructDestroyExplicit(PopulatedTestCase):
    def runTest(self):
        app = Datasource(self.result_dir)
        del app

@export
class TestDatasource_Run(PopulatedTestCase):
    def runTest(self):
        from ...schema import feature, widget_feature, feature_set

        app = Datasource(self.result_dir)
        app.run()
        s = app.get_session()
        self.assertEqual(1, s.query(feature)
            .join(feature_set, feature.idfeature_set == feature_set.idfeature_set)
            .filter(feature_set.name == "datasource").count()
        )
        self.assertEqual(25, s.query(widget_feature)
            .join(feature, widget_feature.idfeature == feature.idfeature)
            .join(feature_set, feature.idfeature_set == feature_set.idfeature_set)
            .filter(feature_set.name == "datasource").count()
        )
