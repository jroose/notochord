from ...ingest.IngestRSS import IngestRSS
from .. import FileStoreTestCase, test_data_dir, default_test_config
from ... import QueryCache, export, schema
import unittest
import os.path
import sqlalchemy

config = default_test_config['IngestRSS']

__all__ = []

@export
class TestIngestRSSConstructDestroyFromArgs(FileStoreTestCase):
    def runTest(self):
        app = IngestRSS.from_args(["--datadir", self.result_dir, "--datasource", "test_data"])
        del app

@export
class TestIngestRSSConstructDestroyExplicit(FileStoreTestCase):
    def runTest(self):
        app = IngestRSS(self.result_dir, u"TestFileStore", "test_data")
        del app

@export
class TestIngestRSSConstructDestroyExplicitWithConfig(FileStoreTestCase):
    def runTest(self):
        app = IngestRSS(self.result_dir, config=config)
        del app

@export
class TestIngestRSS_add_feeds(FileStoreTestCase):
    def runTest(self):
        t_ds = schema.object_store

        app = IngestRSS(self.result_dir, log=self.log, config=config)

        session = app.get_session()
        num_feeds = session.query(sqlalchemy.func.count(t_ds.idobject_store)).scalar()

        #self.assertEqual(num_feeds, len(app.config['feeds']))
        

@export
class TestIngestRSS_run(FileStoreTestCase):
    def runTest(self):
        app = IngestRSS(self.result_dir, log=self.log)
        app.run()
        self.check_results(app.get_session())

    def check_results(self, session, expected_widgets=25, expected_authors=21, expected_sources=1):
        t_w = schema.widget
        t_f = schema.feature
        t_fs = schema.feature_set
        t_wf = schema.widget_feature

        num_widgets = session.query(sqlalchemy.func.count(t_w.idwidget)).one()[0]
        self.log.info("Num Articles: {}".format(num_widgets))
        self.assertEqual(num_widgets, expected_widgets)

        num_features = session.query(sqlalchemy.func.count(t_f.idfeature)).filter(
            t_f.name.in_((u"author", u"uuid", u"id", u"title", u"subreddit"))
        ).scalar()
        self.assertEqual(num_features, 5)

#        num_authors = session.query(sqlalchemy.func.count(t_f.idfeature))\
#            .join(t_fs, t_fs.idfeature_set == t_f.idfeature_set)\
#            .filter(t_fs.name == "author")\
#            .one()[0]
#        self.log.info("Num Authors: {}".format(num_authors))
#        self.assertEqual(num_authors, expected_authors)
#
#        num_object_stores = session.query(sqlalchemy.func.count(schema.object_store.idobject_store)).one()[0]
#        self.log.info("Num Feeds: {}".format(num_object_stores))
#        self.assertEqual(num_object_stores, expected_sources)
