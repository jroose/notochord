from ..initialize import Initialize
from . import DataDirTestCase, make_config_dir
from .. import export
import unittest
import os.path
import shutil

__all__ = []

@export
class TestInitializeConstructDestroyFromArgs(DataDirTestCase):
    def runTest(self):
        app = Initialize.from_args(["--datadir", self.result_dir], log=self.log)
        del app

@export
class TestInitializeConstructDestroyExplicit(DataDirTestCase):
    def runTest(self):
        app = Initialize(self.result_dir, log=self.log)
        del app

@export
class TestInitializeConstructDestroyFromArgsNoExist(DataDirTestCase):
    def runTest(self):
        datadir = os.path.join(self.result_dir, "noexist")
        app = Initialize.from_args(["--datadir", datadir], log=self.log)
        del app

@export
class TestInitializeConstructDestroyExplicitNoExist(DataDirTestCase):
    def runTest(self):
        datadir = os.path.join(self.result_dir, "noexist")
        app = Initialize(datadir, log=self.log)
        del app

@export
class TestInitializeRun(DataDirTestCase):
    def runTest(self):
        Initialize(self.result_dir).run()

@export
class TestInitializeRunNoExist(DataDirTestCase):
    def runTest(self):
        datadir = os.path.join(self.result_dir, "noexist")
        Initialize(datadir).run()

@export
class TestInitializeRunConfig(DataDirTestCase):
    def setUp(self):
        super(TestInitializeRunConfig, self).setUp()
        self.confdir = make_config_dir()

    def tearDown(self):
        shutil.rmtree(self.confdir)
        super(TestInitializeRunConfig, self).tearDown()

    def runTest(self):
        Initialize(self.result_dir, source_confdir=self.confdir).run()
