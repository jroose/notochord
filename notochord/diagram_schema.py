from __future__ import print_function
from . import schema
import sys
import sqlalchemy
import subprocess

def describe_column(t, col):
    primary_keys = [c.name for C in table.constraints for c in C.columns if isinstance(C, sqlalchemy.sql.schema.PrimaryKeyConstraint)]
    foreign_keys = [c.name for C in table.constraints for c in C.columns if isinstance(C, sqlalchemy.sql.schema.ForeignKeyConstraint)]

    pk = col.name in primary_keys
    fk = col.name in foreign_keys

    return "{}{}&nbsp;{}&nbsp;:&nbsp;{}\\l".format(pk and "P" or "&nbsp;", fk and "F" or "&nbsp;", col.name, str(col.type))

png_path = sys.argv[1]
dot_path = "{}.dot".format(png_path.rsplit('.',1)[0])
with open(dot_path, 'w') as fout:
    print("""digraph G {
    overlap = false
    splines = true
    fontname = "bitstream vera sans"
    fontsize = 10

    node [
        fontname = "bitstream vera sans"
        fontsize = 10
        shape = "record"
    ]

    edge [
        arrowhead = "empty"
    ]
""", file=fout)

    for table_name, table in schema.TableBase.metadata.tables.items():
        col_row_desc = []
        for c in table.columns:
            col_row_desc.append(describe_column(table, c))
        column_desc = "".join(col_row_desc)

        unique_row_desc = []
        for C in table.constraints:
            if isinstance(C, sqlalchemy.sql.schema.UniqueConstraint):
                unique_row_desc.append("U&nbsp; {}\\l".format(", ".join([c.name for c in C.columns])))
        unique_desc = "".join(unique_row_desc)

        print("""
    {table_name} [
        label="{{{table_name}|{column_desc}|{unique_desc}}}"
    ]
""".format(table_name=table_name, column_desc=column_desc, unique_desc=unique_desc), file=fout)

        for C in table.constraints:
            if isinstance(C, sqlalchemy.sql.schema.ForeignKeyConstraint):
                print("""{src_table} -> {dst_table}""".format(src_table=table.name, dst_table=C.referred_table), file=fout)
    print("}", file=fout)

subprocess.call(["neato","-Gepsilon=.0001","-Gmaxiter=10000","-Tpng","-o",png_path,dot_path])
