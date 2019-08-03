#!/usr/bin/env python

from . import build_log, export, ModelSet
from .ObjectStore import FileStore

import argparse
import json
import logging
import os.path
import sqlalchemy
import sys
import resource
from contextlib import contextmanager
import os, shutil
import uuid
import time

__all__ = []

@export
class ABCArgumentGroup(object):
    def __init__(self, name=None, description=None):
        if name is None: name = type(self).__name__
        self.name = name
        self.description = description
    
    def __call__(self, group):
        raise NotImplementedError("ABCArgumentGroup is abstract")

@export
class DataArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("--datadir", type=str, action="store", metavar="PATH", required=True, help="Data Directory")
        group.add_argument("--dbstring", type=str, action="store", metavar="DBSTR", help="SQLAlchemy DBString")
        group.add_argument("--config", type=str, action="store", metavar="PATH", default=None, help="Path to config file")

@export
class InfoArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("--logfile", dest="log", type=str, action="store", default=None, metavar="PATH", help="Logfile path")
        group.add_argument("-q", "--quiet", dest="nostderr", action="store_true", default=False, help="No logging to stderr")
        group.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0, help="Increase verbosity")
        group.add_argument("-h", "--help", action="help", help="Print this message")

@export
class WorkOrderArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("--min-idwidget", action="store", metavar="INT", type=int, default=None, help="Minimum idwidget")
        group.add_argument("--max-idwidget", action="store", metavar="INT", type=int, default=None, help="Maximum idwidget")
        group.add_argument("--datasource", dest='datasources', metavar="NAME", type=unicode, default=[], nargs="*", help="Datasources")

@export
class App(object):
    def __init__(self, datadir, name=None, dbstring=None, verbosity=0, log=None, config=None, nostderr=None):
        self.name = name or type(self).__name__

        self.datadir = os.path.abspath(datadir)
        if self.datadir is None:
            raise ValueError("Datadir must not be None")

        if not os.path.exists(self.datadir):
            os.mkdir(self.datadir)

        if dbstring is None:
            self.dbstring = "sqlite:///" + str(os.path.join(self.datadir, "sqlite.db"))
        else:
            if dbstring.startswith("$"):
                dbstring = os.getenv(dbstring[1:])
            self.dbstring = dbstring

        self.confdir = os.path.join(datadir, "config")
        if not os.path.exists(self.confdir): os.mkdir(self.confdir)

        if config is not None:
            if isinstance(config, str) or isinstance(config, unicode):
                with open(config, "rb") as fin:
                    config = json.loads(fin.read())
            else:
                #Ensure config looks like it was constructed from a json object
                config = json.loads(json.dumps(config))
        else:
            config_path = os.path.join(self.confdir, "{name}.json".format(name=self.name))
            if os.path.exists(config_path): 
                with open(config_path, "rb") as fin:
                    config = json.loads(fin.read())
            else:
                config = {}

        self.config = config

        if (log is None) or isinstance(log, str) or isinstance(log, unicode):
            levels = {
                0: logging.WARN,
                1: logging.INFO,
                2: logging.DEBUG
            }
            if log is None:
                logdir = os.path.join(self.datadir, "log")
                if not os.path.exists(logdir):
                    os.mkdir(logdir)
                log = os.path.join(logdir, "{}.txt".format(self.name))

            logname = "{}-{}".format(self.name, str(uuid.uuid4()))
            self.log = build_log(logname, levels.get(verbosity, logging.INFO), nostderr=nostderr, logfile=log)
        else:
            self.log = log

        self._engine = None
        self._Session = None
        self._object_store = None

    def get_engine(self, singleton=True):
        if hasattr(self, '_engine') and self._engine is None:
            connect_args = {}
            if self.dbstring.startswith("sqlite:"):
                connect_args["timeout"] = 600

            if singleton:
                self._engine = sqlalchemy.create_engine(self.dbstring, poolclass=sqlalchemy.pool.SingletonThreadPool, connect_args=connect_args)
            else:
                self._engine = sqlalchemy.create_engine(self.dbstring, connect_args=connect_args)

            if self._engine.dialect.name.lower() == "sqlite":
                sqlite_version = self._engine.execute("SELECT sqlite_version();").scalar()
                from distutils.version import LooseVersion
                if LooseVersion(sqlite_version) < LooseVersion("3.18.0"):
                    raise RuntimeError("SQLite version number is below 3.18.0.  Please source env.sh or install a newer SQLite version than {}".format(sqlite_version))
            elif self._engine.dialect.name.lower() == "mysql":
                self._engine.execute("SET default_storage_engine=MYISAM;")
                pass

        return self._engine

    def get_session(self):
        if self._Session is None:
            self._Session = sqlalchemy.orm.sessionmaker(bind=self.get_engine())

        return self._Session()

    def __del__(self):
        if self._engine is not None:
            self._engine.dispose()

    @staticmethod
    def build_parser_groups():
        return [DataArgs(), InfoArgs()]

    def main(self):
        pass

    def run(self, *args, **kwargs):
        try:
            self.log.info("Starting {name}".format(name=self.name))
            start_time = time.time()
            self.main(*args, **kwargs)
            end_time = time.time()
            rss = resource.getrusage(resource.RUSAGE_SELF)[2] * 1024
            self.log.info("Completed {name}".format(name=self.name))
            self.log.info("Runtime: {} seconds".format(end_time - start_time))
            self.log.info("Max RSS: {} bytes".format(rss))
        except Exception, e:
            self.log.exception("Unhandled Exception")
            raise

    @classmethod
    def from_args(klass, argv, prog=None, name=None, description=None, log=None):
        import textwrap
        class LineWrapRawTextHelpFormatter(argparse.RawDescriptionHelpFormatter):
            def _split_lines(self, text, width):
                text = self._whitespace_matcher.sub(' ', text).strip()
                return textwrap.wrap(text, width)

        if name is None: name = klass.__name__
        if prog is None: prog = klass.__name__

        formatter = argparse.RawTextHelpFormatter
        #parser = argparse.ArgumentParser(prog=prog, add_help=False, formatter_class=formatter)
        parser = argparse.ArgumentParser(prog=prog, add_help=False, formatter_class=LineWrapRawTextHelpFormatter)
        for gdef in klass.build_parser_groups():
            gdef(parser.add_argument_group(gdef.name))

        args = parser.parse_args(argv)
        params = args.__dict__

        if log is not None:
            params['log'] = log

        ret = klass(**params)

        return ret

    @contextmanager
    def session_scope(self):
        session = self.get_session()
        try:
            yield session
        except:
            session.rollback()
            raise
        else:
            session.commit()
        finally:
            session.close()
            self._session = None
    
    
