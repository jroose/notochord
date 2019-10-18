import shutil
import tempfile
import unittest
import inspect
import os.path
import sqlalchemy
import logging
import json

from .. import schema, export, build_log

__all__ = ["test_data_dir", "default_test_config"]

script_path = os.path.abspath(inspect.getfile(inspect.currentframe()))
script_dir = os.path.dirname(script_path)
test_data_dir = os.path.join(script_dir, "data")

default_test_config = json.loads(json.dumps({
    "Initialize": {
        "prepopulate":[
            {
                "table":"feature_set",
                "values":[
                    {"name": u"author"},
                    {"name": u"words"}
                ]
#            },{
#                "table":"content_type",
#                "values":[
#                    {"name": "title"}
#                ]
#            },{
#                "table":"widget_type",
#                "values":[
#                    {"name": "article"},
#                    {"name": "comment"}
#                ],
            },{
                "table":"metric_type",
                "values":[
                ]
            },{
                "table":"model_feature_type",
                "values":[
                    {"name":u"input"},
                    {"name":u"output"},
                    {"name":u"predict"},
                ]
            },{
                "table":"model_widget_type",
                "values":[
                    {"name":u"train"},
                    {"name":u"predict"},
                    {"name":u"validate"},
                ]
            },{
                "table":"mlabel_set",
                "values":[
                    {"name":u"status", "idmlabel_set":1},
                ]
            },{
                "table":"mlabel",
                "values":[
                    {"name":u"new", "idmlabel_set":1},
                    {"name":u"ready", "idmlabel_set":1},
                    {"name":u"trained", "idmlabel_set":1},
                    {"name":u"production", "idmlabel_set":1}
                ]
            }
        ],
        "object_stores":[
            {"name":"file_store", "uri":"file://{datadir}/file_store", "kwargs":{"compression":"gzip"}}
        ]
    },
    "IngestRSS": {
        "refresh_rate": 10,
        "continue_on_error":False,
        "run_count": 1,
        "datasource":u"test_data",
        "object_store":u"file_store",
        "feeds":[
            {"name":u"test_feed", "uri":"file://" + os.path.join(test_data_dir, "test_feed.xml")}
        ]
    },
    "BagOfWords": {
        "input_feature_set":u"ArticleParts",
        "input_feature": u"title",
        "output_feature_set":u"title_bag_of_words",
        "datasources": [u"test_data"],
        "chunk_size": 16384
    },
    "LSITrain": {
        "model_name":u"LSIUnitTest",
        "input_feature_set":u"title_bag_of_words",
        "output_feature_set":u"LSI(title_bag_of_words)",
        "datasources": [u"test_data"],
        "hyperparameters":{
            "n_components":10
        }
    },
    "RandomForestTrain": {
        "model_name":"RandomForestTrain",
        "hyperparameters":{
            "n_estimators":100
        }
    }
}))

@export
def make_config_dir():
    ret = tempfile.mkdtemp()
    for name, config in default_test_config.items():
        with open(os.path.join(ret, "{}.json".format(name)), 'wb') as fout:
            fout.write(json.dumps(config, indent=4))
    return ret

@export
def datadir_session(datadir, singleton=True, **kwargs):
    dbstring = "sqlite:///" + str(os.path.join(datadir, "sqlite.db"))

    if singleton:
        engine = sqlalchemy.create_engine(dbstring, poolclass=sqlalchemy.pool.SingletonThreadPool, **kwargs)
    else:
        engine = sqlalchemy.create_engine(dbstring, **kwargs)

    Session = sqlalchemy.orm.sessionmaker(bind=engine)

    return Session()

@export
class DataDirTestCase(unittest.TestCase):
    def setUp(self):
        self.log = build_log(type(self).__name__, nostderr=True, logfile="./test.log", level=logging.DEBUG)
        self.result_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.result_dir)

@export
class InitializedTestCase(DataDirTestCase):
    def setUp(self, no_engine=False):
        from ..initialize import Initialize
        super(InitializedTestCase, self).setUp()
        self.confdir = make_config_dir()

        config_path = os.path.join(self.confdir, "Initialize.json")
        init = Initialize(self.result_dir, log=self.log, config=config_path, source_confdir=self.confdir)
        init.run()

        self.dbstring = init.dbstring
        if not no_engine:
            self.eng = sqlalchemy.create_engine(self.dbstring, poolclass=sqlalchemy.pool.SingletonThreadPool)
            self.Session = sqlalchemy.orm.sessionmaker(bind=self.eng)
        else:
            self.eng = None
            self.Session = None

    def tearDown(self):
        if self.eng is not None:
            self.eng.dispose()
        shutil.rmtree(self.confdir)
        super(InitializedTestCase, self).tearDown()

@export
class FileStoreTestCase(InitializedTestCase):
    def setUp(self, no_engine=False):
        from ..ObjectStore import FileStore
        super(FileStoreTestCase, self).setUp(no_engine=no_engine)
        self.session = self.Session()
        self.filestore = FileStore.create(self.session, u"TestFileStore", "file://" + os.path.join(self.result_dir, "filestore"))
        self.session.commit()
        self.session.close()
        self.session = None


@export
class PopulatedTestCase(InitializedTestCase):
    def setUp(self, no_engine=False):
        from ..ingest.IngestRSS import IngestRSS
        
        super(PopulatedTestCase, self).setUp(no_engine=no_engine)
        app = IngestRSS(self.result_dir, log=self.log)
        app.run()

@export
class FeaturePopulatedTestCase(PopulatedTestCase):
    def setUp(self, no_engine=False):
        from ..features.BagOfWords import BagOfWords
        from ..features.Datasource import Datasource

        super(FeaturePopulatedTestCase, self).setUp(no_engine=no_engine)
        app = BagOfWords(self.result_dir, log=self.log)
        app.run()
        del app
        app = Datasource(self.result_dir, log=self.log)
        app.run()
        del app

@export
class LSIPopulatedTestCase(FeaturePopulatedTestCase):
    def setUp(self, no_engine=False):
        from ..dim_reduce.LSI import LSITrain

        super(LSIPopulatedTestCase, self).setUp(no_engine=no_engine)
        app = LSITrain(self.result_dir, log=self.log)
        app.run()

