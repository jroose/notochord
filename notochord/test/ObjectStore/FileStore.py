from .. import InitializedTestCase, test_data_dir
from ...ObjectStore.FileStore import *
from ... import export
import os
import unittest
import uuid

__all__ = []

@export
class TestFileStoreNew(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir)

@export
class TestFileStoreNewGzipped(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='gzip')

@export
class TestFileStoreNewBzipped(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='bz2')

@export
class TestFileStoreNewPut(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, directory_layout=[2,2])
        u = uuid.uuid4()
        fs.put(str(u), "Hello World!")
        first, second, rest = u.hex[:2], u.hex[2:4], u.hex[4:]
        self.assertEqual(
            list(os.walk(fs.prefix_path)),
            [(fs.prefix_path, [first], []), (os.path.join(fs.prefix_path, first), [second], []), (os.path.join(fs.prefix_path, first, second), [], [rest])]
        )

@export
class TestFileStoreNewGzippedPut(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='gzip', directory_layout=[2,2])
        u = uuid.uuid4()
        fs.put(str(u), "Hello World!")
        first, second, rest = u.hex[:2], u.hex[2:4], u.hex[4:]
        self.assertEqual(
            list(os.walk(fs.prefix_path)),
            [(fs.prefix_path, [first], []), (os.path.join(fs.prefix_path, first), [second], []), (os.path.join(fs.prefix_path, first, second), [], [rest])]
        )

@export
class TestFileStoreNewBzippedPut(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='bz2', directory_layout=[2,2])
        u = uuid.uuid4()
        fs.put(str(u), "Hello World!")
        first, second, rest = u.hex[:2], u.hex[2:4], u.hex[4:]
        self.assertEqual(
            list(os.walk(fs.prefix_path)),
            [(fs.prefix_path, [first], []), (os.path.join(fs.prefix_path, first), [second], []), (os.path.join(fs.prefix_path, first, second), [], [rest])]
        )

@export
class TestFileStoreNewPutGet(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, directory_layout=[2,2])
        u = str(uuid.uuid4())
        fs.put(u, "Hello World!")
        self.assertEqual(fs.get(u), "Hello World!")

@export
class TestFileStoreNewGzippedPutGet(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='gzip', directory_layout=[2,2])
        u = str(uuid.uuid4())
        fs.put(u, "Hello World!")
        self.assertEqual(fs.get(u), "Hello World!")

@export
class TestFileStoreNewBzippedPutGet(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='bz2', directory_layout=[2,2])
        u = str(uuid.uuid4())
        fs.put(u, "Hello World!")
        self.assertEqual(fs.get(u), "Hello World!")

@export
class TestFileStoreNewPutGetDelete(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, directory_layout=[2,2])
        u = str(uuid.uuid4())
        fs.put(u, "Hello World!")
        self.assertEqual(fs.get(u), "Hello World!")
        fs.delete(u)
        self.assertEqual(os.listdir(fs.prefix_path), [])

@export
class TestFileStoreNewGzippedPutGetDelete(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='gzip', directory_layout=[2,2])
        u = str(uuid.uuid4())
        fs.put(u, "Hello World!")
        self.assertEqual(fs.get(u), "Hello World!")
        fs.delete(u)
        self.assertEqual(os.listdir(fs.prefix_path), [])

@export
class TestFileStoreNewBzippedPutGetDelete(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='bz2', directory_layout=[2,2])
        u = str(uuid.uuid4())
        fs.put(u, "Hello World!")
        self.assertEqual(fs.get(u), "Hello World!")
        fs.delete(u)
        self.assertEqual(os.listdir(fs.prefix_path), [])

@export
class TestFileStoreNewExists(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, directory_layout=[2,2])
        u = str(uuid.uuid4())
        self.assertEqual(fs.exists(u), False)
        fs.put(u, "Hello World!")
        self.assertEqual(fs.get(u), "Hello World!")
        self.assertEqual(fs.exists(u), True)
        fs.delete(u)
        self.assertEqual(os.listdir(fs.prefix_path), [])
        self.assertEqual(fs.exists(u), False)

@export
class TestFileStoreNewGzippedExists(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='gzip', directory_layout=[2,2])
        u = str(uuid.uuid4())
        self.assertEqual(fs.exists(u), False)
        fs.put(u, "Hello World!")
        self.assertEqual(fs.exists(u), True)
        self.assertEqual(fs.get(u), "Hello World!")
        fs.delete(u)
        self.assertEqual(os.listdir(fs.prefix_path), [])
        self.assertEqual(fs.exists(u), False)

@export
class TestFileStoreNewBzippedExists(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='bz2', directory_layout=[2,2])
        u = str(uuid.uuid4())
        self.assertEqual(fs.exists(u), False)
        fs.put(u, "Hello World!")
        self.assertEqual(fs.exists(u), True)
        self.assertEqual(fs.get(u), "Hello World!")
        fs.delete(u)
        self.assertEqual(os.listdir(fs.prefix_path), [])
        self.assertEqual(fs.exists(u), False)

@export
class TestFileStoreNewGzippedCompress(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='gzip', directory_layout=[2,2])
        u = str(uuid.uuid4())
        self.assertEqual(fs.exists(u), False)

        val = "DEADBEEF" * 100
        fs.put(u, val)
        self.assertEqual(fs.exists(u), True)
        self.assertEqual(fs.get(u), val)
        self.assertLess(os.path.getsize(fs.uuid2path(u)), len(val))

@export
class TestFileStoreNewBzippedCompress(InitializedTestCase):
    def runTest(self):
        session = self.Session()
        fsdir = "file://" + os.path.join(self.result_dir, "file_store")
        fs = FileStore.create(session, u"TestFileStore", fsdir, compression='bz2', directory_layout=[2,2])
        u = str(uuid.uuid4())
        self.assertEqual(fs.exists(u), False)

        val = "DEADBEEF" * 100
        fs.put(u, val)
        self.assertEqual(fs.exists(u), True)
        self.assertEqual(fs.get(u), val)
        self.assertLess(os.path.getsize(fs.uuid2path(u)), len(val))
