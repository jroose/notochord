#!/usr/bin/env python

from __future__ import print_function
import feedparser
import json
import time
import logging
import sys
import sqlalchemy
from .. import schema, App, ABCArgumentGroup, lookup_or_persist, temptable_scope, insert_ignore, upload_widget_features
from ..ObjectStore import ABCObjectStore
import uuid
import sys

class IngestRSSArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("--object-store", type=unicode, action="store", metavar="OS_NAME", default=None, nargs='?', help="Name of the object store")
        group.add_argument("--datasource", type=unicode, action="store", metavar="DS_NAME", default=None, nargs='?', help="Name for this datasource")

class IngestRSS(App):
    @staticmethod
    def build_parser_groups():
        return [IngestRSSArgs()] + App.build_parser_groups()

    def __init__(self, datadir, object_store=None, datasource=None, **kwargs):
        super(IngestRSS, self).__init__(datadir, **kwargs)
        self.config['object_store'] = object_store or self.config['object_store']
        self.config['datasource' ] = datasource or self.config['datasource']

    def main(self):
        t_w = schema.widget
        t_fs = schema.feature_set
        t_f = schema.feature
        t_wf = schema.widget_feature
        t_ds = schema.datasource

        datasource_name = self.config['datasource']
        feeds = self.config['feeds']
        refresh_rate = self.config['refresh_rate']
        object_store_name = self.config['object_store']
        continue_on_error = self.config.get('continue_on_error', True)
        max_run_count = self.config.get("run_count", None)

        with self.session_scope() as session:
            self.log.info("Preparing the object store")
            self.object_store = ABCObjectStore.open(session, object_store_name)
            idobject_store = self.object_store.idobject_store
            iddatasource = lookup_or_persist(session, t_ds, name=datasource_name).iddatasource

            self.log.info("Preparing the feature store")
            idfeature_set = lookup_or_persist(session, t_fs, name="ArticleParts", idobject_store=idobject_store).idfeature_set
            self.idfeature = {}
            for k in (u"uuid", u"author", u"title", u"id", u"subreddit"):
                self.idfeature[k] = lookup_or_persist(
                    session, t_f, 
                    name=k,
                    idfeature_set=idfeature_set
                ).idfeature
            
#        from sqlalchemy import Column, tuple_
#        class tmp_upload(schema.TableBase):
#            idtmp_upload = Column(t_f.idfeature.type, nullable=False, primary_key=True)
#            widget = Column(t_w.uuid.type, nullable=False)
#            idfeature = Column(t_f.idfeature.type, nullable=False)
#            __table_args__ = ({'prefixes':["TEMPORARY"], 'keep_existing':True},)
#            __tablename__ = "tmp_upload"

        self.log.info(
            "Running IngestRSS App with refresh rate: {refresh_rate} num_feeds: {num_feeds}".format(
                refresh_rate=refresh_rate,
                num_feeds=len(feeds)
        ))
        time_init = time.time()
        count = 0
        try:
            while (max_run_count is None) or (count < max_run_count):
                total_wait_time = 0.0
                widget_features = []
                for it, object_store in enumerate(feeds):
                    #Wait until we need to refresh.  All feeds are pulled with a
                    #period of config['refresh_period'], but are staggered according
                    #to their position in config['feeds']
                    start_time = time_init + refresh_rate * float(it) / len(feeds)
                    wait_time = start_time - time.time()
                    if wait_time > 0:
                        time.sleep(wait_time)
                        total_wait_time += wait_time

                    #Pull the RSS object_store and add its widgets to the database
                    feed = feedparser.parse(object_store['uri'])

                    for entry in feed.entries:
                        try:
                            w_uuid=uuid.uuid5(uuid.NAMESPACE_URL, entry.id.encode('utf-8'))
                            self.object_store.put(w_uuid.hex, {
                                u"uuid":str(w_uuid),
                                u"author":hasattr(entry, "author") and entry.author or None,
                                u"title":entry.title,
                                u"id":entry.id,
                                u"subreddit":object_store['name']
                            })
                            if hasattr(entry, "author") and entry.author is not None:
                                widget_features.extend([dict(widget=str(w_uuid), idfeature=self.idfeature[k], iddatasource=iddatasource, idfeature_set=idfeature_set) for k in (u"author", u"title", u"id", u"uuid",u"subreddit")])
                            else:
                                widget_features.extend([dict(widget=str(w_uuid), idfeature=self.idfeature[k], iddatasource=iddatasource, idfeature_set=idfeature_set) for k in (u"title", u"id", u"uuid",u"subreddit",)])
                        except Exception, e:
                            self.log.info("Exception occurred from: {entry}".format(entry=str(entry)))
                            self.log.exception(e)
                            if self.continue_on_error:
                                continue
                            else:
                                raise

                self.log.info("Uploading chunk of widget features")
                with self.session_scope() as session:
                    upload_widget_features(session, widget_features)

                count += 1
        except KeyboardInterrupt:
            pass

    @property
    def continue_on_error(self):
        return self.config.get('continue_on_error', True)

if __name__ == "__main__":
    App = IngestRSS.from_args(sys.argv[1:])
    App.run()
