import time
import json

class Head:
    def __init__(self):
        self._next = self

class Entry:
    def __init__(self, cache, stringified, value):
        self._cache = cache
        self._stringified = stringified
        self.when = time.time()
        self._next = cache._head._next
        self._previous = cache._head
        self._next._previous = self
        self._previous._next = self
        self.value = value
        self._references = 1
        self._heft = 0

    def release(self):
        self._references -= 1

    def remove(self):
        assert self._references == 1
        self._cache._remove(self)

    def get_heft(self):
        return self._heft

    def set_heft(self, heft):
        self._cache.heft -= self._heft
        self._heft = heft
        self._cache.heft += self._heft

    heft = property(get_heft, set_heft)

class Cache:
    def __init__(self):
        self._map = dict([])
        self._head = Head()
        self.entries = 0
        self.heft = 0

    def hold (self, key, initializer):
        stringified = json.dumps(key)
        if stringified not in self._map:
            self.entries += 1
            entry = Entry(self, stringified, initializer)
            self._map[stringified] = entry
            return entry
        else:
            entry = self._map[stringified]

        entry._next._previous = entry._previous
        entry._previous._next = entry._next

        entry._next = self._head._next
        entry._previous = self._head
        entry._next._previous = entry
        entry._previous._next = entry

        entry._references += 1
        return entry

    def _remove(self, entry):
        self.heft -= entry._heft
        entry._cache = None
        entry._next._previous = entry._previous
        entry._previous._next = entry._next
        del self._map[entry._stringified]

    def purge(self, heft):
        iterator = self._head._previous
        while self.heft > heft and iterator != self._head:
            if iterator._references == 0:
                self._remove(iterator)
            iterator = iterator._previous
