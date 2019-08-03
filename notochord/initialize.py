from . import App, schema, ABCArgumentGroup, lookup_or_persist
from .ObjectStore import ABCObjectStore
import sqlalchemy
import sys
import shutil
import os.path
import json

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
        if not os.path.exists(self.datadir):
            os.mkdir(self.datadir)

        if self.source_confdir is not None:
            self.log.info("Copying configs")
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
            P = self.config.get('prepopulate', [])
            self.log.info("Prepopulating {} tables".format(len(P)))
            for prepop in P:
                #Get the table from the schema
                table_name = prepop['table']
                if table_name.startswith("_") or not hasattr(schema, table_name):
                    raise ValueError("Invalid table name: '{}'".format(table_name)) 
                t = getattr(schema, table_name)

                self.log.info("Prepopulating {}".format(table_name))
                for values in prepop.get('values', []):
                    lookup_or_persist(session, t, **values)

            O = self.config.get('object_stores', [])
            self.log.info("Initializing object stores {}".format(len(O)))
            format_strings = dict(datadir=self.datadir)
            for obs in O:
                name = obs['name'].format(**format_strings)
                uri = obs['uri'].format(**format_strings)
                ABCObjectStore.create(session, name, uri, **obs['kwargs'])
            session.commit()

if __name__ == "__main__":
    App = Initialize.from_args(sys.argv[1:])
    App.run()
