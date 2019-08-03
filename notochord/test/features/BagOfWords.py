from ...features.BagOfWords import BagOfWords
from .. import PopulatedTestCase
from ... import export
import unittest

__all__ = []

@export
class TestBagOfWordsConstructDestroy(PopulatedTestCase):
    def runTest(self):
        app = BagOfWords.from_args(["--datadir={}".format(self.result_dir)])
        del app

@export
class TestBagOfWordsConstructDestroyExplicit(PopulatedTestCase):
    def runTest(self):
        app = BagOfWords(self.result_dir)
        del app

@export
class TestBagOfWords_Run(PopulatedTestCase):
    def runTest(self):
        app = BagOfWords(self.result_dir)
        app.run()
        session = app.get_session()
