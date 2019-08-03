from .. import schema, App, QueryCache, insert_ignore, ABCArgumentGroup, lookup, persist, insert_ignore, batcher
import sqlalchemy
from sqlalchemy.sql.expression import select, func, literal, tuple_
import sys
import time

class IngestOldSQLArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("source_dbstring", type=str, action="store", metavar="DBSTRING", help="DataSource DBString")

class IngestOldSQL(App):
    def __init__(self, source_dbstring, *args, **kwargs):
        super(IngestOldSQL, self).__init__(*args, **kwargs)
        self.source_dbstring = source_dbstring

    @staticmethod
    def build_parser_groups():
        return [IngestOldSQLArgs()] + App.build_parser_groups()

    def main(self):
        dst_eng = self.get_engine()
        dst_session = self.get_session()
        t_ds = schema.datasource
        t_f = schema.feature
        t_fs = schema.feature_set
        t_w = schema.widget
        t_wc = schema.widget_content
        t_wf = schema.widget_feature
        t_wt = schema.widget_type
        t_ct = schema.content_type

        self.log.info("Loading from: {}".format(self.source_dbstring))
        src_eng = sqlalchemy.create_engine(self.source_dbstring)
        src_session = sqlalchemy.orm.sessionmaker(bind=src_eng)()
        meta = sqlalchemy.schema.MetaData()
        meta.reflect(bind=src_eng)
        tt_article = meta.tables['article']
        tt_subreddit = meta.tables['subreddit']
        tt_user = meta.tables['user']

        ds_reddit = lookup(dst_session, t_ds, name=u"reddit", uri="https://old.reddit.com") or persist(dst_session, t_ds(name=u"reddit", uri="https://old.reddit.com"))
        ct_article_title = lookup(dst_session, t_ct, name=u"article_title") or persist(dst_session, t_ct(name=u"article_title"))
        wt_article = lookup(dst_session, t_wt, name=u"article") or persist(dst_session, t_wt(name=u"article"))
        fs_author = lookup(dst_session, t_fs, name=u"author") or persist(dst_session, t_fs(name=u"author"))
        fs_subreddit = lookup(dst_session, t_fs, name=u"subreddit") or persist(dst_session, t_fs(name=u"subreddit"))

        users = src_session.execute(select([tt_user.c.name]))
        dst_session.execute(
            insert_ignore(t_f),
            [dict(name=unicode(x[0]), idfeature_set=fs_author.idfeature_set) for x in users]
        )

        subreddits = src_session.execute(select([tt_subreddit.c.name]))
        dst_session.execute(
            insert_ignore(t_f),
            [dict(name=unicode(x[0]), idfeature_set=fs_subreddit.idfeature_set) for x in subreddits]
        )

        articles = [(a,u,s,t) for a,u,s,t in src_session.execute(
            select([tt_article.c.uuid, tt_user.c.name, tt_subreddit.c.name, tt_article.c.title]) \
                .where(tt_article.c.iduser == tt_user.c.iduser) \
                .where(tt_article.c.idsubreddit == tt_subreddit.c.idsubreddit) \
        )]

        dst_session.execute(
            insert_ignore(t_w),
            [dict(uuid=a, idwidget_type=wt_article.idwidget_type, iddatasource=ds_reddit.iddatasource) for a,u,s,t in articles]
        )

        self.log.info("Querying all data")
        tt_wf = t_wf.__table__
        all_uid = dict([(f.name, f.idfeature) for f in dst_session.query(t_f).filter_by(idfeature_set=fs_author.idfeature_set)])
        all_sid = dict([(f.name, f.idfeature) for f in dst_session.query(t_f).filter_by(idfeature_set=fs_subreddit.idfeature_set)])
        all_wid = dict([(w.uuid, w.idwidget) for w in dst_session.query(t_w)])
        self.log.info("Running ingest")
        start_time = time.time()
        for it, tup in enumerate(articles):
            a,u,s,t = tup
            uid = all_uid[u]
            sid = all_sid[s]
            wid = all_wid[a]
#            uid = lookup(dst_session, t_f, name=u, idfeature_set=fs_author.idfeature_set).idfeature
#            sid = lookup(dst_session, t_f, name=s, idfeature_set=fs_subreddit.idfeature_set).idfeature
#            wid = lookup(dst_session, t_w, uuid=a).idwidget
            dst_session.execute(
                insert_ignore(t_wf),
                [dict(idfeature=uid, idwidget=wid, value=1), dict(idfeature=sid, idwidget=wid, value=1)]
            )
            dst_session.execute(
                insert_ignore(t_wc),
                [dict(content=t, idwidget=wid, idcontent_type=ct_article_title.idcontent_type)]
            )
            if (it+1) % 100 == 0:
                self.log.info("Ingested {}/{} articles: ETA: {}".format(
                    it,
                    len(articles),
                    (time.time() - start_time) * (len(articles) - it - 1) / (it+1)
                ))

#        for batch in batcher(articles, 16):
#
#            dst_session.execute(
#                tt_wf.insert().from_select(
#                    [tt_wf.c.idwidget, tt_wf.c.idfeature, tt_wf.c.value],
#                    select([t_w.idwidget, t_f.idfeature, literal(1, t_wf.value.type)]) \
#                        .where(tuple_(t_w.uuid, t_f.name).in_([tuple_(literal(a, t_w.uuid.type), literal(u, t_f.name.type)) for a,u,s,t in batch])) \
#                        .where(t_f.idfeature_set == fs_author.idfeature_set) \
#            ))
            
        dst_session.commit()


if __name__ == "__main__":
    IngestOldSQL.from_args(sys.argv[1:]).run()
