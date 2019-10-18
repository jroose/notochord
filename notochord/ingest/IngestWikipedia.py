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
        group.add_argument("--widget_set", type=unicode, action="store", metavar="DS_NAME", default=None, nargs='?', help="Name for this widget_set")
        group.add_argument("--max-count", type=int, action="store", metavar="INT", default=None, help="Maximum number of articles")
        group.add_argument("wikifile", type=unicode, action="store", metavar="PATH", default=None, help="Path to the wiki XML file")

class IngestWikipedia(App):
    @staticmethod
    def build_parser_groups():
        return [IngestWikipediaArgs()] + App.build_parser_groups()

    def __init__(self, datadir, wikifile, max_count=None, object_store=None, widget_set=None, **kwargs):
        super(IngestWikipedia, self).__init__(datadir, **kwargs)
        self.config['object_store'] = object_store or self.config['object_store']
        self.config['widget_set' ] = widget_set or self.config['widget_set']
        self.config['max_count' ] = max_count or self.config.get('max_count')
        self.config['wikifile' ] = wikifile

    def main(self):
        widget_set_name = self.config['widget_set']
        wikifile = self.config['wikifile']
        object_store_name = self.config['object_store']
        max_count = self.config.get('max_count', None)
        continue_on_error = self.config.get('continue_on_error', True)

        with self.context_scope() as C:
            widget_set = C.load_widget_set(widget_set_name)
            with self.rate_timer(print_frequency=1000) as timer:
                for article in parse_wikipedia(wikifile):
                    article_title, article_id, article_text = article
                    w_uuid = uuid.uuid5(uuid.NAMESPACE_URL, str(article_id))
                    try:
                        widget_set.create_widget(w_uuid.hex, content={
                            'uuid':w_uuid.hex,
                            'title':article_title,
                            'id':article_id,
                            'text':article_text
                        })

                    except:
                        if continue_on_error:
                            self.log.exception("Problem ingesting article: {}".format(timer.count))
                        else:
                            raise
                    finally:
                        timer.inc()

                    if max_count and (timer.count >= max_count):
                        break

if __name__ == "__main__":
    App = IngestWikipedia.from_args(sys.argv[1:])
    App.run()
