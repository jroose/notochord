from .. import schema, App, QueryCache, batcher, grouper, insert_ignore, export, lookup, persist, lookup_or_persist, ABCArgumentGroup, WorkOrderArgs, filter_widgets, temptable_scope, FeatureCache
from ..ObjectStore import ABCObjectStore
from sqlalchemy import Column, Integer, String, Float, ForeignKey, UnicodeText, Unicode, LargeBinary, Boolean, Index
import collections
import csv
import os
import re
import sqlalchemy
import sys
import tempfile
import time
import stat
from sklearn.feature_extraction.text import CountVectorizer

re_word = re.compile(r'[a-zA-Z]+')

__all__ = []

class BagOfWordsArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("--output-feature-set", type=unicode, action="store", metavar="NAME", default=None, help="Name of output feature set (required)")
        group.add_argument("--input-feature-set", type=unicode, action="store", metavar="NAME", default=None, help="Name of input feature set (required)")
        group.add_argument("--input-feature", type=unicode, action="store", metavar="NAME", default=None, help="Name of input feature")
        group.add_argument("--chunk-size", type=int, action="store", metavar="INT", default=None, help="Number or widgets per chunk")

@export
class BagOfWords(App):
    @staticmethod
    def build_parser_groups():
        return [BagOfWordsArgs(), WorkOrderArgs()] + App.build_parser_groups()

    def __init__(self, datadir, input_feature_set=None, output_feature_set=None, input_feature=None, min_idwidget=None, max_idwidget=None, datasources=None, chunk_size=None, **kwargs):
        super(BagOfWords, self).__init__(datadir, **kwargs)
        self.config['output_feature_set'] = output_feature_set or self.config['output_feature_set']
        self.config['input_feature_set'] = input_feature_set or self.config['input_feature_set']
        self.config['input_feature'] = input_feature or self.config.get('input_feature')
        self.config['datasources'] = datasources or self.config.get('datasources')
        self.config["chunk_size"] = chunk_size or self.config.get('chunk_size', 1024)
        self.config['min_idwidget'] = (min_idwidget, None)[min_idwidget is None]
        self.config['max_idwidget'] = (max_idwidget, None)[max_idwidget is None]

    def main(self):
        import MySQLdb
        from warnings import filterwarnings
        filterwarnings('ignore', category = MySQLdb.Warning)

        import sqlalchemy
        from sqlalchemy import Column, literal, tuple_, insert
        from ..schema import widget as t_w
        from ..schema import widget_feature as t_wf
        from ..schema import feature as t_f
        from ..schema import feature_set as t_fs
        from ..schema import datasource as t_ds
        from ..schema import object_store as t_os

        with self.session_scope() as session:
            self.log.info("Preparing")
            fs_in = lookup(session, t_fs, name=self.config['input_feature_set'])
            if fs_in is None: raise KeyError("Invalid feature set: '{}'".format(self.config['input_feature_set']))

            fs_out = lookup_or_persist(session, t_fs, name=self.config['output_feature_set'])
            if fs_out is None: raise KeyError("Invalid feature set: '{}'".format(self.config['output_feature_set']))

            os_in = lookup(session, t_os, idobject_store=fs_in.idobject_store)
            if fs_in.idobject_store is None or os_in is None:
                raise ValueError("Feature set '{}' has no associated object store".format(self.config['input_feature_set']))
            else:
                object_store = ABCObjectStore.open(session, os_in.name)

            f_in = lookup(session, t_f, name=self.config['input_feature'], idfeature_set=fs_in.idfeature_set)
            if f_in is None:
                if self.config['input_feature'] is not None:
                    raise KeyError("Invalid feature: '{}' for feature_set '{}'".format(self.config['input_feature'], self.config['input_feature_set']))
                else:
                    raise KeyError("Invalid feature_set '{}' has no default feature".format(self.config['input_feature_set']))

            q_w = session.query(t_w.idwidget)
            q_w = filter_widgets(
                q_w,
                min_idwidget = self.config['min_idwidget'],
                max_idwidget = self.config['max_idwidget'],
                datasources = self.config['datasources']
            )

            q_wf = session.query(t_wf.idwidget, t_wf.idfeature) \
                .join(t_w, t_w.idwidget == t_wf.idwidget) \
                .join(t_f, t_f.idfeature == t_wf.idfeature) \
                .filter(t_f.idfeature_set == fs_out.idfeature_set)

            if self.config['min_idwidget'] is not None:
                q_wf = q_wf.filter(t_w.idwidget >= self.config['min_idwidget'])
            if self.config['max_idwidget'] is not None:
                q_wf = q_wf.filter(t_w.idwidget < self.config['max_idwidget'])
            if self.config['datasources'] is not None and len(self.config['datasources']) > 0:
                q_wf = q_wf.join(t_ds, t_ds.iddatasource == t_w.iddatasource)
                q_wf = q_wf.filter(t_ds.name.in_(self.config['datasources']))

            self.log.info("Deleting old features")
            #q_del = q_wf.delete()
            #q_del = t_wf.__table__.delete() \
            #    .where(tuple_(t_wf.idwidget, t_wf.idfeature).in_(q_wf))
            #self.log.debug("Delete widget query: {}".format(q_del.compile(bind=session.bind)))
            #session.execute(q_del)
            
            q_w = session.query(t_w.idwidget, t_w.uuid)
            q_w = filter_widgets(
                q_w,
                min_idwidget = self.config['min_idwidget'],
                max_idwidget = self.config['max_idwidget'],
                datasources = self.config['datasources']
            )
            
            q_w = q_w.join(t_wf, t_wf.idwidget == t_w.idwidget) \
                .filter(t_wf.idfeature == f_in.idfeature)

            class tmp_upload(schema.TableBase):
                idtmp_upload = Column(t_f.idfeature.type, nullable=False, primary_key=True)
                idwidget = Column(t_wf.idwidget.type, nullable=False)
                idfeature = Column(t_wf.idfeature.type, nullable=False)
                value = Column(t_wf.value.type, nullable=False)
                __table_args__ = ({'prefixes':["TEMPORARY"]},)
                __tablename__ = "tmp_upload"

            class tmp_wf(schema.TableBase):
                idwidget = Column(Integer, ForeignKey('widget.idwidget', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)
                idfeature = Column(Integer, ForeignKey('feature.idfeature', onupdate='RESTRICT', ondelete='CASCADE'), primary_key=True, nullable=False)
                value = Column(Float, nullable=True)
                __table_args__ = ({'prefixes':["TEMPORARY"]},)
                __tablename__ = "tmp_widget_feature"

            self.log.info("Beginning Execution")
            self.log.debug("Widget query: {}".format(q_w.statement.compile(bind=session.bind)))
            FC = FeatureCache(1024*1024, log=self.log)

            count_time = 0.0
            feature_time = 0.0
            widget_time = 0.0
            upload_time = 0.0
            primary_key_time = 0.0
            num_widgets = 0
            num_widget_features = 0

            start_time = time.time()
            if session.bind.dialect.name.lower() == 'mysql':
                session.execute("SET @@foreign_key_checks=0;")
                session.execute("ALTER TABLE widget_feature DISABLE KEYS;")
                insert_fout, insert_file = tempfile.mkstemp()
                os.close(insert_fout)
                os.chmod(insert_file, stat.S_IREAD | stat.S_IWRITE | stat.S_IROTH)

            begin_time = time.time()
            for it, result_chunk in enumerate(grouper(q_w, self.config['chunk_size'])):
                start_time = time.time()
                self.log.info("Executing chunk {}".format(it))
                upload_chunk = []
                N = 0
                words = []
                widgets = []
                values = []
                for row in result_chunk:
                    if row is not None:
                        idwidget, uuid = row
                        content = object_store.get(uuid, feature=f_in.name)
                        if content is None:
                            continue
                        cnt = collections.Counter(x.group(0).lower() for x in re_word.finditer(content))
                        words.extend(cnt.iterkeys())
                        values.extend(cnt.itervalues())
                        widgets.extend(idwidget for _ in xrange(len(cnt)))
                        N += len(cnt)

                end_time = time.time()
                count_time += (end_time - start_time)
                start_time = time.time()

                self.log.info("Getting feature id's")
                word_idents = FC(session, fs_out.idfeature_set, (w for w in words))
                self.log.info("Copying into upload_chunk")
                upload_chunk = [dict(idwidget=widgets[it], idfeature=word_idents[it], value=values[it]) for it in xrange(N)]
                num_widget_features += len(upload_chunk)

                end_time = time.time()
                feature_time += (end_time - start_time)
                start_time = time.time()
                dialect = session.bind.dialect.name

                with temptable_scope(session, tmp_upload), temptable_scope(session, tmp_wf):
                    self.log.info("Uploading widget_feature chunk of size: {}".format(len(upload_chunk)))
                    session.bulk_insert_mappings(tmp_upload, upload_chunk)
                    end_time = time.time()
                    upload_time += (end_time - start_time)
                    start_time = time.time()

                    self.log.info("Constructing primary key")
                    insert_stmt = insert_ignore(tmp_wf, dialect).from_select(
                            [tmp_wf.idwidget, tmp_wf.idfeature, tmp_wf.value],
                            session.query(tmp_upload.idwidget, tmp_upload.idfeature, tmp_upload.value) \
                                .select_from(tmp_upload) \
                        )
                    session.execute(insert_stmt)

                    end_time = time.time()
                    primary_key_time += (end_time - start_time)
                    start_time = time.time()

                    if session.bind.dialect.name.lower() == 'mysql':
                        with open(insert_file, 'w') as fout:
                            csvout = csv.writer(fout, delimiter=',', escapechar='\\')
                            for row in session.query(tmp_wf.idwidget, tmp_wf.idfeature, tmp_wf.value):
                                csvout.writerow(tuple(row))
                            del csvout

                        self.log.info("Temp file size: {}".format(os.path.getsize(insert_file)))

                        insert_stmt = sqlalchemy.text(r"""
                            LOAD DATA CONCURRENT LOCAL INFILE '{insert_file}'
                            IGNORE
                            INTO TABLE widget_feature
                            FIELDS TERMINATED BY ','
                                OPTIONALLY ENCLOSED BY '"'
                                ESCAPED BY '\\'
                            LINES TERMINATED BY '\n'
                            (idwidget, idfeature, value)
                        """.format(insert_file=insert_file))
                    else:
                        insert_stmt = insert_ignore(t_wf, dialect).from_select(
                                [t_wf.idwidget, t_wf.idfeature, t_wf.value],
                                session.query(tmp_wf.idwidget, tmp_wf.idfeature, tmp_wf.value)
                            )
                    start_time = time.time()
                    self.log.info("Transferring into place")
                    session.execute(insert_stmt)

                    end_time = time.time()
                    widget_time += (end_time - start_time)

                    num_widgets += len(result_chunk)

                    self.log.info("Average Times: {} {} {} {} {} {}".format(count_time / num_widgets, feature_time / num_widgets, upload_time / num_widgets, primary_key_time / num_widgets, widget_time / num_widgets, num_widget_features / num_widgets))
                    self.log.info("Average Rate: {}".format(num_widgets / (time.time() - begin_time)))
                    self.log.info("Max Rate: {}".format(num_widgets / widget_time))


            if session.bind.dialect.name.lower() == 'mysql':
                session.execute("ALTER TABLE widget_feature ENABLE KEYS;")
                session.execute("SET @@foreign_key_checks=1;")
                os.remove(insert_file)

            tmp_upload.metadata.remove(tmp_upload.__table__)
            tmp_upload.metadata.remove(tmp_wf.__table__)

if __name__ == "__main__":
    A = BagOfWords.from_args(sys.argv[1:])
    A.run()
