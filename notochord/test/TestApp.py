from .. import App
import sys
import unittest
import os
import shutil

__all__ = ["TestAppNoConfig"]

class UselessApp(App):
    pass

class TestAppNoConfig(unittest.TestCase):
    result_dir="./test_results"

    def setUp(self):
        if os.path.exists(self.result_dir):
            shutil.rmtree(self.result_dir)
        os.mkdir(self.result_dir)

    def tearDown(self):
        shutil.rmtree(self.result_dir)

    def test_construct_destroy(self):
        app = UselessApp.from_args(["--datadir=./test_results"])
        del app

    def test_run(self):
        app = UselessApp.from_args(["--datadir=./test_results"])
        app.run()
        del app
