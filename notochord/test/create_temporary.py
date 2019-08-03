from ..Model import ModelSet, Model

from . import PopulatedTestCase, datadir_session
from .. import export, schema, grouper, get_utcnow, temporary_table_like, temptable_scope
import unittest
import time
import random
import json
import datetime
import iso8601
import itertools
import numpy as np

__all__ = []

@export
class TestModel(PopulatedTestCase):
    def setUp(self):
        t_w = schema.widget

        super(TestModel, self).setUp()
        self.session = datadir_session(self.result_dir, singleton=True)
        expected = [x.uuid for x in self.session.query(t_w)]
        self.assertGreater(len(expected), 0)

    def runTest(self):
        t_w = schema.widget

        self.log.info("Creating temporary table")
        with temptable_scope(self.session, temporary_table_like("tmp_widget", t_w)) as tmp_w:
            self.log.info("Populating temporary table")
            self.session.execute(tmp_w.insert().from_select(tmp_w.columns, t_w.__table__.select()))

            self.log.info("Copying")
            expected = [x.uuid for x in self.session.query(t_w)]
            observed = [x.uuid for x in self.session.query(tmp_w)]

            self.log.info("len(expected)={} len(observed)={}".format(len(expected), len(observed)))

            self.assertEqual(expected, observed)

        self.session.bind.dispose()
        del self.session

        self.session = datadir_session(self.result_dir)


            

