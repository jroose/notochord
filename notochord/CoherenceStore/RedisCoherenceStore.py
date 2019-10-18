from .. import export
from .ABCCoherenceStore import ABCCoherenceStore, ABCCoherenceCollection
import urlparse
import redis

__all__ = []

@export
class RedisCoherenceCollection(ABCCoherenceCollection):
    def __init__(self, coherence_store, name):
        super(RedisCoherenceCollection, self).__init__(coherence_store, name)
        self.hashset = self.name + "/collection"
        self.counter = self.name + "/counter"

    def exists(self, name):
        self.store.redis.hexists(self.hashset, name)

    def get(self, name):
        return self.store.redis.hget(self.hashset, name)

    def get_or_generate(self, name):
        ret = self.get(name)
        if ret: return ret

        ret = self.next()
        if self.store.redis.hsetnx(name, ret): return ret
        else: return self.get(name)

    def set(self, name, ident):
        self.store.redis.hset(self.hashset, name, ident)

    def delete(self, name):
        self.store.redis.hdelete(self.hashset, name)

    def current(self):
        return None

    def next(self):
        return self.store.redis.incr(self.counter)

@export
@ABCCoherenceStore.register("redis")
class RedisCoherenceStore(ABCCoherenceStore):
    def __init__(self, cs):
        super(RedisCoherenceStore, self).__init__(cs)
        self.__dict__.update({
            'redis':redis.Redis.from_url(cs.uri)
        })

    def initialize(self):
        pass

    def create_collection(self, collection_name):
        self.redis.setnx(collection_name + "/counter", 0)

    def delete_collection(self, collection_name):
        vals = [
            collection_name + "/counter",
            collection_name + "/collection"
        ]
        self.redis.delete(*vals)

    def has_collection(self, collection_name):
        vals = [
            collection_name + "/counter",
            collection_name + "/collection"
        ]
        return self.redis.exists(*vals) == len(vals)

    def get_collection(self, collection_name):
        return RedisCoherenceCollection(self, collection_name)
