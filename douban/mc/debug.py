#!/usr/bin/env python
# encoding: utf-8
"""
debug.py
"""

import time
import sys
from itertools import izip
from cPickle import dumps

class LocalMemcache(object):

    def __init__(self):
        self.dataset = {}

    def set(self, key, val, time=0, compress=True):
        _, version = self.dataset.get(key, (None, -1))
        self.dataset[key] = (val, version + 1)
        return True

    def add(self, key, val):
        if not self.dataset.has_key(key):
            self.dataset[key] = (val, 1)
            return True
        else:
            return False


    def set_multi(self, values, time=0, compress = True, return_failure = False):
        for k, v in values.iteritems():
            _, version = self.dataset.get(k, (None, -1))
            self.dataset[k] = (v, version + 1)
        if return_failure:
            return True, []
        else: return True

    def cas(self, key, val, time = 0, cas = 0):
        if key in self.dataset:
            _, version = self.dataset.get(key)
            if version == cas:
                self.dataset[key] = (val, version + 1)
                return True
        return False

    def delete(self, key, time=0):
        if key in self.dataset:
            del self.dataset[key]
        return 1

    def delete_multi(self, keys, time=0, return_failure = False):
        for k in keys:
            self.delete(k)
        if return_failure:
            return True, []
        else: return True

    def get(self, key):
        return self.dataset.get(key, (None, 0))[0]

    def gets(self, key):
        return self.dataset.get(key, (None, 0))

    def get_raw(self, key):
        raise NotImplementedError()

    def get_multi(self, keys):
        rets = {}
        for k in keys:
            r = self.dataset.get(k)
            if r is not None:
                rets[k] = r[0]
        return rets

    def get_list(self, keys):
        return [self.dataset.get(k)[0] for k in keys]

    def incr(self, key, val=1):
        raise NotImplementedError()

    def decr(self, key, val=1):
        raise NotImplementedError()

    def clear(self):
        self.dataset.clear()

    def get_last_error(self):
        return 0

class FakeMemcacheClient(object):
    def set(self, key, val, expire_secs=0, compress=True):
        return 1

    def set_multi(self, values, expire_secs=0, compress=True):
        return 1

    def delete(self, key, timeout=0):
        return 1

    def delete_multi(self, keys):
        return 1

    def get(self, key):
        return None

    def get_raw(self, key):
        return None

    def get_multi(self, keys):
        return {}

    def get_list(self, keys):
        return [None] * len(keys)

    def incr(self, key, val=1):
        return 0

    def decr(self, key, val=1):
        return 0

    def clear(self):
        return

    def close(self):
        return

    def get_last_error(self):
        return 0

    def prepend_multi(self, *args, **kws):
        return

    def append(self, *args, **kws):
        return

    def add(self, *args, **kws):
        return 1

class LogMemcache:
    def __init__(self, mc):
        self.mc = mc

    def dumps(self, val):
        if val is None:
            return ''
        if isinstance(val, basestring):
            pass
        elif isinstance(val, int) or isinstance(val, long):
            val = str(val)
        else:
            val = dumps(val, -1)
        return val

    def log(self, s):
        print >> sys.stderr, "[%s] memcache %s" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), s)

    def set(self, key, val, expire_secs=0):
        self.log("set %r:%d" % (key, len(self.dumps(val))))
        return self.mc.set(key, val, expire_secs)

    def delete(self, key, timeout=0):
        self.log("delete %r" % key)
        return self.mc.delete(key, timeout)

    def get(self, key):
        val = self.mc.get(key)
        self.log("get %r:%d" % (key, len(self.dumps(val))))
        return val

    def get_multi(self, keys):
        vals = self.mc.get_multi(keys)
        self.log("get_multi %s" % (", ".join(
            "%r:%d" % (k, len(self.dumps(v)))
            for k, v in vals.iteritems())))
        return vals

    def get_list(self, keys):
        vals = self.mc.get_list(keys)
        self.log("get_list %s" % (", ".join(
            "%r:%d" % (k, len(self.dumps(v)))
            for k, v in izip(keys, vals))))
        return vals

    def incr(self, key, val=1):
        self.log("incr %r" % key)
        return self.mc.incr(key, val)

    def decr(self, key, val=1):
        self.log("decr %r" % key)
        return self.mc.decr(key, val)

    def close(self):
        self.mc.close()
