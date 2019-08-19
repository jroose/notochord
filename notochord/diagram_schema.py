from __future__ import print_function
from . import schema
import sys
import sqlalchemy
import subprocess
import os.path

def describe_column(table, col):
    primary_keys = [c.name for C in table.constraints for c in C.columns if isinstance(C, sqlalchemy.sql.schema.PrimaryKeyConstraint)]
    foreign_keys = [c.name for C in table.constraints for c in C.columns if isinstance(C, sqlalchemy.sql.schema.ForeignKeyConstraint)]

    pk = col.name in primary_keys
    fk = col.name in foreign_keys

    return "{}{}&nbsp;{}&nbsp;:&nbsp;{}\\l".format(pk and "P" or "&nbsp;", fk and "F" or "&nbsp;", col.name, str(col.type))

def main(outfile):
    outdir = os.path.dirname(outfile)
    outname = os.path.basename(outfile)
    dotname = "{}.dot".format(outname.rsplit(".",1)[0])

    assert ("." in outname) and (outname.rsplit(".",1)[-1].lower() == "png")

    dot_path = os.path.join(outdir, dotname)
    png_path = outfile
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
    """, file=fout)

        for table_name, table in schema.TableBase.metadata.tables.items():
            primary_keys = [c.name for C in table.constraints for c in C.columns if isinstance(C, sqlalchemy.sql.schema.PrimaryKeyConstraint)]

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
                    is_primary_key = any(c.name in primary_keys for c in C.columns)
                    
                    if is_primary_key:
                        arrow_style = "normal"
                    else:
                        arrow_style = "empty"

                    print(primary_keys, [c.name for c in C.columns], arrow_style)

                    print("""\t{src_table} -> {dst_table} [arrowhead={arrow_style}]""".format(src_table=table.name, dst_table=C.referred_table, arrow_style=arrow_style), file=fout)
        print("}", file=fout)

    subprocess.call(["neato","-Gepsilon=.0001","-Gmaxiter=10000","-Tpng","-o",png_path,dot_path])

if __name__ == "__main__":
    assert len(sys.argv) == 2
    main(sys.argv[1])
