#!/usr/bin/env python
# encoding: utf-8

""" test_mc.py
"""

import unittest
import random
import time

import cmemcached
from douban.mc import mc_from_config
from douban.mc.wrapper import AdjustMC, Replicated, LocalCached, \
        VersionedLocalCached
from mock import patch, Mock, call

class PureMCTest(unittest.TestCase):
    config = {
        'servers': ['127.0.0.1:11211'],
    }
    mc = mc_from_config(config)

    def get_random_key(self):
        return 'MCTEST:%s:%s' % (time.time(), random.random())

    def test_wrapper_is_right(self):
        self.assertTrue(isinstance(self.mc.mc, cmemcached.Client))

    def test_incr_decr(self):
        key = self.get_random_key()

        self.assertTrue(self.mc.set(key, 10))
        self.mc.incr(key)
        self.assertEqual(11, self.mc.get(key))
        self.mc.decr(key, 10)
        self.assertEqual(1, self.mc.get(key))
        self.mc.delete(key)

    def test_add_replace(self):
        key = self.get_random_key()
        val1 = str(random.random())
        val2 = str(random.random())

        self.assertTrue(self.mc.get(key) is None)
        self.assertTrue(self.mc.add(key, val1))
        self.assertEquals(val1, self.mc.get(key))

        self.assertFalse(self.mc.add(key, val2))
        self.assertEqual(val1, self.mc.get(key))

        self.mc.delete(key)
        self.assertTrue(self.mc.get(key) is None)
        self.assertFalse(self.mc.replace(key, val1))
        self.assertTrue(self.mc.get(key) is None)
        self.mc.set(key, val1)
        self.assertEqual(val1, self.mc.get(key))
        self.assertTrue(self.mc.replace(key, val2))
        self.assertEqual(val2, self.mc.get(key))

        self.mc.delete(key)

    def test_set_delete_append_prepend(self):
        key = self.get_random_key()
        val = str(random.random())

        self.mc.delete(key)
        self.assertTrue(self.mc.get(key) is None)
        self.mc.set(key, val)
        self.assertEqual(val, self.mc.get(key))

        self.mc.append(key, 'a')
        self.assertEqual(val+'a', self.mc.get(key))

        self.mc.prepend(key, 'a')
        self.assertEqual('a'+val+'a', self.mc.get(key))

        self.mc.delete(key)
        self.assertTrue(self.mc.get(key) is None)

    def test_multi_set_delete_append_prepend(self):
        key1, key2 = self.get_random_key(), self.get_random_key()
        val1, val2 = str(random.random()), str(random.random())

        self.mc.delete_multi([key1, key2])
        self.assertEqual({}, self.mc.get_multi([key1, key2]))

        data = {key1: val1, key2: val2}
        self.mc.set_multi(data)
        self.assertEqual(data, self.mc.get_multi([key1, key2]))

        self.mc.append_multi([key1, key2], 'a')
        self.assertEqual({key1: val1+'a', key2: val2+'a'}, self.mc.get_multi([key1, key2]))

        self.mc.prepend_multi([key1, key2], 'a')
        self.assertEqual({key1: 'a'+val1+'a', key2: 'a'+val2+'a'},
                         self.mc.get_multi([key1, key2]))

        self.mc.delete_multi([key1, key2])
        self.assertEqual({}, self.mc.get_multi([key1, key2]))

class AdjustMCTest(PureMCTest):
    config = {
        'servers': ['127.0.0.1:11211'],
        'new_servers': ['127.0.0.1:11212'],
    }
    mc = mc_from_config(config)

    def test_wrapper_is_right(self):
        self.assertTrue(isinstance(self.mc.mc, AdjustMC))

class ReplicatedTest(PureMCTest):
    config = {
        'servers': ['127.0.0.1:11211'],
        'new_servers': [],
        'backup_servers': ['127.0.0.1:11213'],
    }
    mc = mc_from_config(config)

    def test_wrapper_is_right(self):
        self.assertTrue(isinstance(self.mc.mc, Replicated))

class LocalCachedTest(PureMCTest):
    config = {
        'servers': ['127.0.0.1:11211'],
    }
    mc = LocalCached(mc_from_config(config))

    def test_wrapper_is_right(self):
        self.assertTrue(isinstance(self.mc, LocalCached))

class VersionedLocalCachedTestCase(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)

        config = {
            'servers': ['127.0.0.1:11211'],
        }
        self.mc = mc_from_config(config)
        self.cache = VersionedLocalCached(self.mc)

    def test_get_should_return_None_when_not_exists_in_mc(self):
        r = self.cache.get('test')
        self.assertEqual(r, None)

    def test_get_should_return_None_when_only_version_exists(self):
        self.mc.set('key:VER2', '1234567890')
        r = self.cache.get('key')
        self.assertEqual(r, None)

    def test_get_should_return_None_when_no_version_exists(self):
        self.mc.delete('key:VER2')
        self.mc.set('key:V:1234567890', 1)
        r = self.cache.get('key')
        self.assertEqual(r, None)

    def test_get_should_return_value_after_set(self):
        self.cache.set('key', 1)
        r = self.cache.get('key')
        self.assertEqual(r, 1)

    def test_get_should_return_value_when_another_process_set_it(self):
        c2 = VersionedLocalCached(self.mc)
        c2.set('key', 1)
        r = self.cache.get('key')
        self.assertEqual(r, 1)

    def test_set_should_set_local_cache(self):
        self.cache.set('key', 1)
        self.assertTrue('key' in self.cache.dataset)

    def test_get_should_return_from_local_cache_when_version_matches(self):
        self.cache.set('key', 1)
        # change local cache for comparation
        value, version = self.cache.dataset['key']
        self.cache.dataset['key'] = (2, version)
        r = self.cache.get('key')
        self.assertEqual(r, 2)

    def test_get_should_return_from_mc_when_cached_version_mismatches(self):
        self.cache.set('key', 1)
        c2 = VersionedLocalCached(self.mc)
        c2.set('key', 2)
        self.assertEqual(self.cache.dataset['key'][0], 1)
        r = self.cache.get('key')
        self.assertEqual(r, 2)
        self.assertEqual(self.cache.dataset['key'][0], 2)
        self.assertEqual(self.cache.dataset['key'][1], self.mc.get('key:VER2'))

    def test_get_multi_should_work(self):
        self.cache.set('key1', 1)
        self.cache.set('key2', 2)
        r = self.cache.get_multi(['key1', 'key2'])
        self.assertEqual(r, {'key1': 1, 'key2': 2})

    def test_get_list_should_work(self):
        self.cache.set('key1', 1)
        self.cache.set('key2', 2)
        r = self.cache.get_list(['key1', 'key2'])
        self.assertEqual(r, [1, 2])

    def test_delete_should_work(self):
        self.cache.set('key1', 1)
        self.cache.delete('key1')
        r = self.cache.get('key1')
        self.assertEqual(r, None)

    def test_version_should_be_consistent_for_same_value(self):
        self.cache.set('key1', 1)
        ver = self.mc.get('key1:VER2')
        self.cache.delete('key1')
        self.cache.set('key1', 1)
        ver2 = self.mc.get('key1:VER2')
        self.assertEqual(ver, ver2)

class AsyncSendTest(unittest.TestCase):
    config = {
            'servers' : ['127.0.0.1:11299'],
    }

    mq = Mock()
    def test_delete_fail_will_cause_async(self):
        self.mq.reset_mock()
        def async_log(key):
            self.mq.send(key)
        mc = mc_from_config(self.config, async_cleaner = async_log)
        mc.delete('test_key')
        self.mq.send.assert_called_with('test_key')

    def test_set_fail_will_cause_async(self):
        self.mq.reset_mock()
        def async_log(key):
            self.mq.send(key)
        mc = mc_from_config(self.config, async_cleaner = async_log)
        mc.set('test_set_key', 'test_value')
        self.mq.send.assert_called_with('test_set_key')
        #assert mq.send.called

    def test_set_multi_fail_will_cause_async(self):
        self.mq.reset_mock()
        async_log = lambda key: self.mq.send(key)
        mc = mc_from_config(self.config, async_cleaner = async_log)
        mc.set_multi({'key1':'value1', 'key2':'value2'})
        assert self.mq.send.call_count == 2
        calls = self.mq.send.call_args_list
        for call in calls:
            assert call == call('key1') or call == call('key2')

    def test_delete_multi_fail_will_cause_async(self):
        self.mq.reset_mock()
        async_log = lambda key: self.mq.send(key)
        mc = mc_from_config(self.config, async_cleaner = async_log)
        mc.delete_multi(['key1', 'key2', 'key3'])
        assert self.mq.send.call_count == 3
        calls = self.mq.send.call_args_list
        for call in calls:
            assert call == call('key1') or call == call('key2') or \
                    call == call('key3')


    def test_get_fail_will_never_use_async(self):
        self.mq.reset_mock()
        async_log = lambda key: self.mq.send(key)
        mc = mc_from_config(self.config, async_cleaner = async_log)
        mc.get('test_key')
        assert not self.mq.send.called

if __name__ == '__main__':
    unittest.main()
