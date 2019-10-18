from .. import export
from .ABCObjectStore import ABCObjectStore, ABCObjectCollection
import json
import os
import posixpath
import urllib, urlparse
import uuid

__all__ = []

@export
class FileObjectCollection(ABCObjectCollection):
    def exists(self, item_uuid):
        return os.path.exists(self.store.uuid2path(self.name, item_uuid))

    def get(self, item_uuid, feature=None):
        path = self.store.uuid2path(self.name, item_uuid)
        with self.store.openfunc(path, 'rb') as fin:
            ret = self.store.unmarshal(fin.read())

        if feature is None:
            return ret
        else:
            return ret[feature]

    def put(self, item_uuid, content):
        path_parts = [self.store.prefix_path, self.name] + self.store.uuid2pathparts(item_uuid)

        dirpath = posixpath.join(*path_parts[:-1])
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        path = posixpath.join(dirpath, path_parts[-1])
        with self.store.openfunc(path, 'wb') as fout:
            fout.write(self.store.marshal(content))

    def post(self, item_uuid, value, feature=None):
        path = self.store.uuid2path(self.name, item_uuid)
        with self.store.openfunc(path, 'rb') as fin:
            ret = self.store.unmarshal(fin.read())
        ret[feature] = value
        with self.store.openfunc(path, 'wb') as fout:
            fout.write(self.store.marshal(ret))

    def delete(self, item_uuid):
        path_parts = [self.store.prefix_path, self.name] + self.store.uuid2pathparts(item_uuid)

        path = posixpath.join(*path_parts)
        os.remove(path)

        for itparts in reversed(xrange(2, len(path_parts))):
            try:
                os.rmdir(posixpath.join(*path_parts[:itparts]))
            except OSError:
                break


@export
@ABCObjectStore.register("file")
class FileObjectStore(ABCObjectStore):
    def __init__(self, ds, directory_layout=[2,2], compression=None, marshal=None):
        super(FileObjectStore, self).__init__(ds)

        assert(all([isinstance(x, int) for x in directory_layout]))

        if compression is None:
            openfunc = open
        elif compression == "gzip":
            import gzip
            openfunc = gzip.open
        elif compression == "bz2":
            from contextlib import closing
            import bz2
            openfunc = lambda path, mode: closing(bz2.BZ2File(path, mode))
        else:
            raise NotImplementedError("Compression scheme '{}' not implemented".format(compression))

        if marshal == "json":
            marshal = json.dumps
            unmarshal = json.loads
        elif marshal is None or marshal == "raw":
            marshal = lambda x: x
            unmarshal = lambda x: x
        else:
            raise NotImplementedError("Marshaling scheme '{}' not implemented".format(compression))

        url_parts = urlparse.urlparse(ds.uri)
        assert url_parts.scheme in (u"file", None, "")

        self.__dict__.update(dict(
            prefix_path = url_parts.path,
            directory_layout = directory_layout,
            openfunc = openfunc,
            marshal = marshal,
            unmarshal = unmarshal
        ))

    def initialize(self):
        if not os.path.exists(self.prefix_path):
            os.mkdir(self.prefix_path)

    def uuid2pathparts(self, item_uuid):
        hexid = uuid.UUID(item_uuid).hex

        ret = []
        count = 0
        for dc in self.directory_layout:
            ret.append(hexid[count:count+dc])
            count += dc
        ret.append(hexid[count:])

        return ret

    def uuid2path(self, collection_name, item_uuid):
        return posixpath.join(self.prefix_path, collection_name, *self.uuid2pathparts(item_uuid))

    def create_collection(self, collection_name):
        os.mkdir(posixpath.join(self.prefix_path, collection_name))
        return self.get_collection(collection_name)

    def delete_collection(self, collection_name):
        os.rmdir(posixpath.join(self.prefix_path, collection_name))

    def has_collection(self, collection_name):
        return os.path.exists(posixpath.join(self.prefix_path, collection_name))

    def get_collection(self, collection_name):
        return FileObjectCollection(self, collection_name)
