import unittest

from cache import Cache

class TestCache(unittest.TestCase):
    def test_constructor(self):
        cache = Cache()
        self.assertEqual(cache.entries, 0, 'empty')
        self.assertEqual(cache.heft, 0, 'very light')

    def test_hold(self):
        cache = Cache()
        entry = cache.hold([ 1 ], 1)
        self.assertEqual(cache.entries, 1, 'inserted')
        self.assertEqual(entry.value, 1, 'value')
        entry.release()

    def test_heft(self):
        cache = Cache()
        self.assertEqual(cache.heft, 0, 'initial cache heft')
        entry = cache.hold([ 1 ], 1)
        self.assertEqual(entry.value, 1, 'cached')
        self.assertEqual(entry.heft, 0, 'initial entry heft')
        entry.heft = 1
        self.assertEqual(entry.heft, 1, 'updated entry heft')
        self.assertEqual(cache.heft, 1, 'updated cache heft')
        entry.release()

    def test_cached_get(self):
        cache = Cache()
        self.assertEqual(cache.entries, 0, 'initial entries count')
        first = cache.hold([ 1 ], 1)
        self.assertEqual(cache.entries, 1, 'insert entries count')
        self.assertEqual(first.value, 1, 'cached')
        first.release()
        second = cache.hold([ 1 ], 2)
        self.assertEqual(first.value, 1, 'got cached')
        self.assertEqual(cache.entries, 1, 'get entries count')
        first.release()
        second.release()

    def test_remove(self):
        cache = Cache()
        self.assertEqual(cache.heft, 0, 'initial cache heft')
        first = cache.hold([ 1 ], 1)
        self.assertEqual(first.value, 1, 'cached')
        first.heft = 1
        self.assertEqual(cache.heft, 1, 'set entry heft')
        first.release()
        second = cache.hold([ 1 ], 2)
        self.assertEqual(second.value, 1, 'got cached')
        second.remove()
        self.assertEqual(cache.heft, 0, 'removed object')
        third = cache.hold([ 1 ], 2)
        self.assertEqual(third.value, 2, 'inserted new object')
        third.remove()

    def test_purge(self):
        cache = Cache()
        first = cache.hold([ 1 ], 1)
        first.heft = 1
        second = cache.hold([ 2 ], 1)
        second.heft = 1
        second.release()
        third = cache.hold([ 3 ], 1)
        third.heft = 1
        third.release()
        self.assertEqual(cache.heft, 3, 'cache heft at 3')
        cache.purge(2)
        self.assertEqual(cache.heft, 2, 'cache heft at 2')
