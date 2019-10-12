from . import App, schema, ABCArgumentGroup, lookup_or_persist
from .ObjectStore import ABCObjectStore
import sqlalchemy
import sys
import shutil
import os.path
import json

def good_tag_table_name(x):
    return table_name.endswith("tag") and len(table_name) != 4 and table_name.startswith("_")

class InitializeArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("source_confdir", type=str, action="store", metavar="PATH", default=None, nargs="?", help="Configuration Directory")

class Initialize(App):
    @staticmethod
    def build_parser_groups():
        return [InitializeArgs()] + App.build_parser_groups()

    def __init__(self, datadir, source_confdir=None, **kwargs):
        super(Initialize, self).__init__(datadir, **kwargs)
        self.source_confdir = source_confdir

    def main(self):
        self.log.info("Create the directory")
        if not os.path.exists(self.datadir):
            os.mkdir(self.datadir)

        self.log.info("Copying configs")
        if self.source_confdir is not None:
            if os.path.exists(self.confdir):
                shutil.rmtree(self.confdir)
            shutil.copytree(self.source_confdir, self.confdir)

            config_path = os.path.join(self.confdir, "{name}.json".format(name=self.name))
            if os.path.exists(config_path): 
                with open(config_path, "rb") as fin:
                    self.config = json.loads(fin.read())

        self.log.info("Creating schema")
        schema.TableBase.metadata.create_all(self.get_engine())

        with self.session_scope() as session:
            self.log.info("Prepopulating tags")
            T = self.config.get("tags", [])
            for tag_desc in T:
                table_name = tag_desc['type']
                if (not good_tag_table_name) or (not hasattr(schema, table_name)):
                    raise ValueError("Invalid tag type: '{}'".format(table_name))

                t_tag = getattr(schema, table_name)
                t_tag_set = getattr(schema, "{}_set".format(table_name))

                cols = {
                    'name':tag_desc['tag']
                }

                if 'tag_set' in tag_desc:
                    tag_set_name = '{}_set'.format(table_name)
                    idtag_set_name = 'id{}_set'.format(table_name)

                    idtag_set = lookup_or_persist(session, t_tag_set, name=tag_desc['tag_set'])
                    cols[idtag_set_name] = getattr(idtag_set, idtag_set_name)

                lookup_or_persist(session, t_tag, **cols)

            self.log.info("Prepopulating tables")
            P = self.config.get('prepopulate', [])
            for prepop in P:
                #Get the table from the schema
                table_name = prepop['table']
                if table_name.startswith("_") or not hasattr(schema, table_name):
                    raise ValueError("Invalid table name: '{}'".format(table_name)) 
                t = getattr(schema, table_name)

                self.log.info("Prepopulating {}".format(table_name))
                for values in prepop.get('values', []):
                    lookup_or_persist(session, t, **values)

            self.log.info("Initializing object stores")
            O = self.config.get('object_stores', [])
            format_strings = dict(datadir=self.datadir)
            for obs in O:
                name = obs['name'].format(**format_strings)
                uri = obs['uri'].format(**format_strings)
                ABCObjectStore.create(session, name, uri, **obs['kwargs'])

            session.commit()

if __name__ == "__main__":
    App = Initialize.from_args(sys.argv[1:])
    App.run()
