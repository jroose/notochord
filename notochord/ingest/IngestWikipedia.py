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
import urllib
#import xml.etree.cElementTree as etree
from lxml import etree
import gc
import collections

def strip_tag_name(t):
    idx = k = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t

def parse_wikipedia(fpath):
    for event, elem in etree.iterparse(fpath, events=('start','end')):
        tname = strip_tag_name(elem.tag)
        #tname = elem.tag
        if event == 'start':
            if tname == 'page':
                title = ''
                id = -1
                redirect = ''
                inrevision = False
                ns = 0
                article_text = None
            elif tname == 'revision':
                inrevision = True
            else:
                if tname == 'title':
                    title = elem.text
                elif tname == 'id' and not inrevision:
                    if elem.text is not None:
                        id = int(elem.text)
                elif tname == 'redirect':
                    redirect = elem.attrib['title']
                elif tname == 'ns':
                    if elem.text is not None:
                        ns = int(elem.text)
                elif tname == 'text':
                    article_text = elem.text
        else:
            if tname == 'page':
                if ns == 10:
                    pass
                elif len(redirect) > 0:
                    pass
                elif article_text is not None:
                    yield title, id, article_text

            elem.clear()
            #elem.getparent().remove(elem)

class IngestWikipediaArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("--object-store", type=unicode, action="store", metavar="OS_NAME", default=None, nargs='?', help="Name of the object store")
        group.add_argument("--datasource", type=unicode, action="store", metavar="DS_NAME", default=None, nargs='?', help="Name for this datasource")
        group.add_argument("--max-count", type=int, action="store", metavar="INT", default=None, help="Maximum number of articles")
        group.add_argument("wikifile", type=unicode, action="store", metavar="PATH", default=None, help="Path to the wiki XML file")

class IngestWikipedia(App):
    @staticmethod
    def build_parser_groups():
        return [IngestWikipediaArgs()] + App.build_parser_groups()

    def __init__(self, datadir, wikifile, max_count=None, object_store=None, datasource=None, **kwargs):
        super(IngestWikipedia, self).__init__(datadir, **kwargs)
        self.config['object_store'] = object_store or self.config['object_store']
        self.config['datasource' ] = datasource or self.config['datasource']
        self.config['max_count' ] = max_count or self.config.get('max_count')
        self.config['wikifile' ] = wikifile

    def main(self):
        t_w = schema.widget
        t_fs = schema.feature_set
        t_f = schema.feature
        t_wf = schema.widget_feature
        t_ds = schema.datasource

        datasource_name = self.config['datasource']
        wikifile = self.config['wikifile']
        object_store_name = self.config['object_store']
        continue_on_error = self.config.get('continue_on_error', True)

        with self.session_scope() as session:

            self.log.info("Preparing the object store")
            self.object_store = ABCObjectStore.open(session, object_store_name)
            idobject_store = self.object_store.idobject_store
            iddatasource = lookup_or_persist(session, t_ds, name=datasource_name).iddatasource

            self.log.info("Preparing the feature store")
            self.idfeature_set = lookup_or_persist(session, t_fs, name="ArticleParts", idobject_store=idobject_store).idfeature_set
            self.idfeature = {}
            for k in (u"title",u"text",u"id","uuid"):
                self.idfeature[k] = lookup_or_persist(
                    session, t_f, 
                    name=k,
                    idfeature_set=self.idfeature_set
                ).idfeature
            
        self.log.info("Running IngestWikipedia App")
        time_init = time.time()
        count = 0

        try:
            total_wait_time = 0.0
            sum_load_time = 0.0
            load_count = 0
            widget_features = []
            #gc_items = collections.Counter(type(x) for x in gc.get_objects())
            for it, article in enumerate(parse_wikipedia(wikifile)):
                article_title, article_id, article_text = article
                #print(article_title)
                try:
                    w_uuid=uuid.uuid5(uuid.NAMESPACE_URL, str(article_id))
                    self.object_store.put(w_uuid.hex, {
                        'uuid':w_uuid.hex,
                        'title':article_title,
                        'id':article_id,
                        'text':article_text
                    })

                    for k in (u"title",u"text",u"id","uuid"):
                        widget_features.append({
                            "widget": w_uuid.hex,
                            "idfeature": self.idfeature[k],
                            'idfeature_set': self.idfeature_set,
                            'iddatasource': iddatasource
                        })

                except Exception, e:
                    self.log.info("Exception occurred from: {}".format(article))
                    self.log.exception(e)
                    if self.continue_on_error:
                        continue
                    else:
                        raise

                if (it+1) % 5000 == 0:
                    load_start_time = time.time()
                    with self.session_scope() as session:
                        upload_widget_features(session, widget_features)
                    widget_features = []
                    sum_load_time += time.time() - load_start_time
                    avg_total_duration = (time.time() - time_init) / (it + 1)
                    avg_load_duration = sum_load_time / (it+1)
                    self.log.info("Load time: {} Total time: {} Count: {}".format(avg_load_duration, avg_total_duration, it+1))
                    
                    gc.collect()
                    if self.config.get('max_count') is not None:
                        if it >= self.config.get('max_count'):
                            break
        except KeyboardInterrupt:
            pass


    @property
    def continue_on_error(self):
        return self.config.get('continue_on_error', True)

if __name__ == "__main__":
    App = IngestWikipedia.from_args(sys.argv[1:])
    App.run()
