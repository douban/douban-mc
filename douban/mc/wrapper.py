# -*- coding: utf-8 -*-

from random import randint
from hashlib import md5

import cmemcached

from .util import LogMixin

class AdjustMC(object):
    def __init__(self, oldmc, newmc):
        self._oldmc = oldmc
        self._newmc = newmc

    def moved(self, key):
        return self._oldmc.get_host_by_key(key) != self._newmc.get_host_by_key(key)

    def get(self, key):
        v = self._newmc.get(key)
        if v is None and self.moved(key):
            v = self._oldmc.get(key)
            if v is not None:
                self._newmc.set(key, v, 3600 + randint(0, 3600))
                self._oldmc.delete(key)
        return v

    def get_multi(self, keys):
        r = self._newmc.get_multi(keys)
        rs = self._oldmc.get_multi([k for k in keys if k not in r and self.moved(k)])
        for k,v in rs.iteritems():
            self._newmc.set(k, v, 3600 + randint(0, 3600))
            self._oldmc.delete(k)
        r.update(rs)
        return r

    def get_list(self, keys):
        rs = self.get_multi(keys)
        return [rs.get(k) for k in keys]

    def clear(self):
        pass

    def close(self):
        self._oldmc.close()
        self._newmc.close()

    def reset(self):
        self._oldmc.reset()
        self._newmc.reset()

    def clear_thread_ident(self):
        self._oldmc.clear_thread_ident()
        self._newmc.clear_thread_ident()

    def __getattr__(self, name):
        if name in ('add','replace','set','cas','delete','incr','decr',
                    'prepend','append','touch','expire'):
            def func(key, *args, **kwargs):
                if self.moved(key):
                    self._oldmc.delete(key)
                return getattr(self._newmc, name)(key, *args, **kwargs)
            return func
        elif name in ('append_multi', 'prepend_multi', 'delete_multi',
                      'set_multi'):
            def func(keys, *args, **kwargs):
                moved_keys = [k for k in keys if self.moved(k)]
                self._oldmc.delete_multi(moved_keys)
                return  getattr(self._newmc, name)(keys, *args, **kwargs)
            return func
        elif not name.startswith('__'):
            def func(*args, **kwargs):
                return getattr(self.mc, name)(*args, **kwargs)
            return func
        raise AttributeError(name)

class Replicated(LogMixin):
    "replacated memcached for fail-over"
    def __init__(self, master, rep):
        self.mc = master
        self.rep = rep

    def __repr__(self):
        return "replicated " + str(self.mc)

    def get(self, key):
        v = self.mc.get(key)
        if v is None:
            v = self.rep.get(key)
            if v is not None:
                self.mc.set(key, v, 60 * 10)
        return v

    def get_multi(self, keys):
        r = self.mc.get_multi(keys)
        rs = self.rep.get_multi([k for k in keys if k not in r])
        for k,v in rs.items():
            self.mc.set(k, v, 60 * 10)
        r.update(rs)
        return r

    def get_list(self, keys):
        rs = self.get_multi(keys)
        return [rs.get(k) for k in keys]

    def set(self, key, value, time=0, compress=True):
        if value is None:
            return
        # let key expire in rep first
        self.rep.set(key, value, time/2, compress)
        return self.mc.set(key, value, time, compress)

    def clear(self):
        pass

    def close(self):
        self.mc.close()
        self.rep.close()

    def reset(self):
        self.mc.reset()
        self.rep.reset()

    def clear_thread_ident(self):
        self.mc.clear_thread_ident()
        self.rep.clear_thread_ident()

    def __getattr__(self, name):
        if name in ('add','replace','delete','incr','decr',
                    'prepend','append','touch','expire'):
            def func(key, *args, **kwargs):
                self.rep.delete(key)
                return getattr(self.mc, name)(key, *args, **kwargs)
            return func
        elif name in ('append_multi', 'prepend_multi', 'delete_multi',
                      'set_multi'):
            def func(keys, *args, **kwargs):
                self.rep.delete_multi(keys)
                return getattr(self.mc, name)(keys, *args, **kwargs)
            return func
        elif not name.startswith('__'):
            def func(*args, **kwargs):
                return getattr(self.mc, name)(*args, **kwargs)
            return func
        raise AttributeError(name)


class LocalCached(LogMixin):
    " cache obj in local process, wrapper for memcache "
    def __init__(self, mc_client, size=10000):
        self.dataset = {}
        self.mc = mc_client
        self.size = size

    def clear(self):
        self.dataset.clear()
        if hasattr(self.mc, 'clear'):
            self.mc.clear()

    def _cache(self, key, value):
        if len(self.dataset) >= self.size:
            self.dataset.clear()
        self.dataset[key] = value

    def __repr__(self):
        return "Locally Cached " + str(self.mc)

    def get(self, key):
        if key in self.dataset:
            return self.dataset[key]
        r = self.mc.get(key)
        if r is not None:
            self._cache(key, r)
        return r

    def gets(self, key):
        return self.mc.gets(key)

    def get_multi(self, keys):
        ds = self.dataset
        ds_get = ds.get
        r = dict((k, ds[k]) for k in keys if ds_get(k) is not None)
        missed = [k for k in keys if k not in ds]
        if missed:
            rs = self.mc.get_multi(missed)
            r.update(rs)
            ds.update(dict((k, rs.get(k)) for k in missed))
        return r

    def get_list(self, keys):
        rs = self.get_multi(keys)
        return [rs.get(k) for k in keys]

    def set(self, key, value, time=0, compress=True):
        self._cache(key, value)
        return self.mc.set(key, value, time, compress)

    def cas(self, key, value, time=0, cas=0):
        if self.mc.cas(key, value, time, cas):
            self._cache(key, value)
            return True
        else:
            self.dataset.pop(key, None) # FIXME
            return False

    def __getattr__(self, name):
        if name in ('add','replace','delete','incr','decr',
                    'prepend','append','touch','expire'):
            def func(key, *args, **kwargs):
                self.dataset.pop(key, None)
                return getattr(self.mc, name)(key, *args, **kwargs)
            return func
        elif name in ('append_multi', 'prepend_multi', 'delete_multi', 'set_multi'):
            def func(keys, *args, **kwargs):
                for k in keys:
                    self.dataset.pop(k, None)
                return getattr(self.mc, name)(keys, *args, **kwargs)
            return func
        elif not name.startswith('__'):
            def func(*args, **kwargs):
                return getattr(self.mc, name)(*args, **kwargs)
            return func
        raise AttributeError(name)

    def reset(self):
        self.mc.reset()
        self.clear()

class VersionedLocalCached(object):
    def __init__(self, _mc):
        self.mc = _mc
        self.dataset = {}

    def get(self, key):
        ver = self.mc.get(key+':VER2')
        if ver is None:
            return None
        val, cached_ver = self.dataset.get(key, (None, None))
        if cached_ver != ver:
            val = self.mc.get(key+':V_'+ver)
            if val is None:
                return None
            self.dataset[key] = (val, ver)
        return val

    def add(self, key, value, time=0):
        self.dataset.pop(key, None)
        ver = self._get_version(value)
        if self.mc.add(key+':VER2', ver, time):
            self.mc.set(key+':V_'+ver, value, time)
            return 1
        return 0

    def set(self, key, value, time=0):
        if value is None:
            return
        ver = self._get_version(value)
        r = self.mc.set(key+':V_'+ver, value, time)
        self.mc.set(key+':VER2', ver, time)
        self.dataset[key] = (value, ver)
        return r

    def get_multi(self, keys):
        #TODO to optimize
        d = {}
        for key in keys:
            val = self.get(key)
            if val is not None:
                d[key] = val
        return d

    def get_list(self, keys):
        #TODO to optimize
        return [self.get(k) for k in keys]

    def delete(self, key):
        self.dataset.pop(key, None)
        return self.mc.delete(key+':VER2')

    def touch(self, key, exptime):
        return self.mc.touch(key+':VER2', exptime)

    def expire(self, key):
        self.dataset.pop(key, None)
        return self.mc.expire(key+':VER2')

    def _get_version(self, value):
        serialed, flag = cmemcached.prepare(value, 0)
        return md5(serialed).hexdigest()

    def _get_version(self, value):
        serialized, flag = cmemcached.prepare(value, 0)
        return md5(serialized).hexdigest()

class SyncMC(object):
    def __init__(self, main_mc, sync_mc):
        self.mc = main_mc
        self.sync_mc = sync_mc

    def clear_thread_ident(self):
        self.mc.clear_thread_ident()
        self.sync_mc.clear_thread_ident()

    def reset(self):
        self.mc.reset()
        self.sync_mc.reset()

    def __getattr__(self, name):
        if name in ('add', 'replace', 'set', 'delete','incr','decr',
                    'append','prepend','expire','touch'):
            def func(key, *args, **kwargs):
                self.sync_mc.delete(key)
                return getattr(self.mc, name)(key, *args, **kwargs)
            return func
        else:
            return getattr(self.mc, name)
