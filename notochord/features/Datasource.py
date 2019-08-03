from .. import schema, App, QueryCache, batcher, grouper, insert_ignore, export, lookup, persist, lookup_or_persist
import re
import sqlalchemy
import collections
import sys

re_word = re.compile(r'[a-zA-Z]+')

__all__ = []

@export
class Datasource(App):
    def main(self):
        from ..schema import datasource, widget, widget_feature, feature, feature_set
        from sqlalchemy.sql.expression import and_, literal, select

        def t(table):
            return table.__table__

        s = self.get_session()

        idfs_ds = lookup_or_persist(s, feature_set, name="datasource").idfeature_set

        for ds in s.query(datasource):
            lookup_or_persist(s, feature, idfeature_set=idfs_ds, name=ds.name)

        select_stmt = select([t(widget).c.idwidget, t(feature).c.idfeature, literal(1, widget_feature.value.type)]) \
                    .select_from(
                        t(widget)
                        .join(datasource, datasource.iddatasource == widget.iddatasource)
                    ) \
                    .where(and_(feature.name == datasource.name, feature.idfeature_set == idfs_ds))

        insert_stmt = insert_ignore(widget_feature).from_select(
                [t(widget_feature).c.idwidget, t(widget_feature).c.idfeature, t(widget_feature).c.value],
                select_stmt
            )

        s.execute(insert_stmt)

        s.commit()


if __name__ == "__main__":
    A = Datasource.from_args(sys.argv[1:])
    A.run()
