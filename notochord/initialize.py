from . import App, schema, ABCArgumentGroup, lookup_or_persist, create_label
from .ObjectStore import ABCObjectStore
import sqlalchemy
import sys
import shutil
import os.path
import json

def good_label_table_name(x):
    return table_name.endswith("label") and len(table_name) != 4 and table_name.startswith("_")

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

        with self.context_scope() as ctx:
            session = ctx.session

            self.log.info("Prepopulating labels")
            T = self.config.get("labels", [])
            for label_desc in T:
                table_name = label_desc['type']
                if (not good_label_table_name) or (not hasattr(schema, table_name)):
                    raise ValueError("Invalid label type: '{}'".format(table_name))
                create_label(session, table_name, label_desc['label_set'], label_desc['label_name'])


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
            stores = {}
            format_strings = dict(datadir=self.datadir)
            for obs in O:
                name = obs['name'].format(**format_strings)
                uri = obs['uri'].format(**format_strings)
                stores[name] = ABCObjectStore.create(session, name, uri, **obs['kwargs'])
            session.commit()

            self.log.info("Prepopulating widget sets")
            for ws in self.config.get('widget_sets', []):
                osinst = stores[ws['object_store']]
                ctx.create_widget_set(ws['name'], osinst)




if __name__ == "__main__":
    App = Initialize.from_args(sys.argv[1:])
    App.run()
