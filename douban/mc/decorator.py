#!/usr/bin/env python
# encoding: utf-8

''' memcache decorator '''

import inspect
from functools import wraps
import sys
import time
import struct
from warnings import warn

from douban.utils import format, Empty


_MC_CHUNK_SIZE = 1000000 - 1000 # from python-libmemcached, split_mc.h


def gen_key(key_pattern, arg_names, defaults, *a, **kw):
    return gen_key_factory(key_pattern, arg_names, defaults)(*a, **kw)

def gen_key_factory(key_pattern, arg_names, defaults):
    args = dict(zip(arg_names[-len(defaults):], defaults)) if defaults else {}
    if callable(key_pattern):
        names = inspect.getargspec(key_pattern)[0]
    def gen_key(*a, **kw):
        aa = args.copy()
        aa.update(zip(arg_names, a))
        aa.update(kw)
        if callable(key_pattern):
            key = key_pattern(*[aa[n] for n in names])
        else:
            key = format(key_pattern, *[aa[n] for n in arg_names], **aa)
        return key and key.replace(' ','_'), aa
    return gen_key

def cache(key_pattern, mc, expire=0, max_retry=0):
    def deco(f):
        arg_names, varargs, varkw, defaults = inspect.getargspec(f)
        if varargs or varkw:
            raise Exception("do not support varargs")
        gen_key = gen_key_factory(key_pattern, arg_names, defaults)
        @wraps(f)
        def _(*a, **kw):
            key, args = gen_key(*a, **kw)
            if not key:
                return f(*a, **kw)
            force = kw.pop('force', False)
            r = mc.get(key) if not force else None

            # anti miss-storm
            retry = max_retry
            while r is None and retry > 0:
                # when node is down, add() will failed
                if mc.add(key + '#mutex', 1, int(max_retry * 0.1)):
                    break
                time.sleep(0.1)
                r = mc.get(key)
                retry -= 1

            if r is None:
                r = f(*a, **kw)
                if r is not None:
                    mc.set(key, r, expire)
                if max_retry > 0:
                    mc.delete(key + '#mutex')

            if isinstance(r, Empty):
                r = None
            return r
        _.original_function = f
        return _
    return deco

def pcache(key_pattern, mc, count=300, expire=0, max_retry=0):
    def deco(f):
        arg_names, varargs, varkw, defaults = inspect.getargspec(f)
        if varargs or varkw:
            raise Exception("do not support varargs")
        if not ('limit' in arg_names):
            raise Exception("function must has 'limit' in args")
        gen_key = gen_key_factory(key_pattern, arg_names, defaults)
        @wraps(f)
        def _(*a, **kw):
            key, args = gen_key(*a, **kw)
            start = args.pop('start', 0)
            limit = args.pop('limit')
            start = int(start)
            limit = int(limit)
            if not key or limit is None or start+limit > count:
                return f(*a, **kw)

            force = kw.pop('force', False)
            r = mc.get(key) if not force else None

            # anti miss-storm
            retry = max_retry
            while r is None and retry > 0:
                # when node is down, add() will failed
                if mc.add(key + '#mutex', 1, int(max_retry*0.1)):
                    break
                print >>sys.stderr, "@cache(): wait for ", key, 'to return'
                time.sleep(0.1)
                r = mc.get(key)
                retry -= 1

            if r is None:
                r = f(limit=count, **args)
                mc.set(key, r, expire)
            mc.delete(key + '#mutex')
            return r[start:start+limit]
        _.original_function = f
        return _
    return deco

def pcache2(key_pattern, mc, count=300, expire=0):
    def deco(f):
        arg_names, varargs, varkw, defaults = inspect.getargspec(f)
        if varargs or varkw:
            raise Exception("do not support varargs")
        if not ('limit' in arg_names):
            raise Exception("function must has 'limit' in args")
        gen_key = gen_key_factory(key_pattern, arg_names, defaults)
        @wraps(f)
        def _(*a, **kw):
            key, args = gen_key(*a, **kw)
            start = args.pop('start', 0)
            limit = args.pop('limit')
            if not key or limit is None or start+limit > count:
                return f(*a, **kw)

            n = 0
            force = kw.pop('force', False)
            d = mc.get(key) if not force else None
            if d is None:
                n, r = f(limit=count, **args)
                mc.set(key, (n, r), expire)
            else:
                n, r = d
            return (n, r[start:start+limit])
        _.original_function = f
        return _
    return deco

def listcache(key_pattern, mc, expire=0, fmt='I'):
    "cache list(int) using struct.pack, for append/prepend"
    def deco(f):
        arg_names, varargs, varkw, defaults = inspect.getargspec(f)
        if varargs or varkw:
            raise Exception("do not support varargs")
        gen_key = gen_key_factory(key_pattern, arg_names, defaults)
        size = struct.calcsize(fmt)
        @wraps(f)
        def _(*a, **kw):
            key, args = gen_key(*a, **kw)
            if not key:
                return f(*a, **kw)
            force = kw.pop('force', False)
            r = mc.get(key) if not force else None
            if r and len(r) > _MC_CHUNK_SIZE:
                # python-libmemcached会将大于`CHUNK_SIZE`的值split为多个再set
                # 会让`append/prepend`行为不符合预期
                # 这里认为接近`CHUNK_SIZE`的值都可能是有错的
                r = None
            if r is not None and len(r)%size == 0:
                r = struct.unpack(fmt*(len(r)/size), r)
            else:
                r = f(*a, **kw)
                if isinstance(r, (list, tuple)):
                    mc.set(key, struct.pack(fmt*len(r), *r), expire, compress=False)
                else:
                    warn("func %s (%s) should return list or tuple" % (f.__name__, key))
            return r
        _.original_function = f
        return _
    return deco

def delete_cache(key_pattern,mc):
    def deco(f):
        arg_names, varargs, varkw, defaults = inspect.getargspec(f)
        if varargs or varkw:
            raise Exception("do not support varargs")
        gen_key = gen_key_factory(key_pattern, arg_names, defaults)
        @wraps(f)
        def _(*a, **kw):
            key, args = gen_key(*a, **kw)
            r = f(*a, **kw)
            mc.delete(key)
            return r
        return _
        _.original_function = f
    return deco

def cache_in_obj(key, mc, expire=0):
    def deco(f):
        @wraps(f)
        def _(obj, *a, **kw):
            name = '_cached_' + f.__name__
            force = kw.pop('force', False)
            v = getattr(obj, name, None) if not force else None
            if v is None:
                v = f(obj, *a, **kw)
                if v is not None:
                    setattr(obj, name, v)
                    mc.set(key % obj.id, obj, expire)
            return v
        _.original_function = f
        return _
    return deco

def create_decorators(mc):
    # 因为cache的调用有太多对expire参数的非关键字调用，因此没法用partial方式生成函数

    def _cache(key_pattern, expire=0, mc=mc, max_retry=0):
        return cache(key_pattern, mc, expire=expire, max_retry=max_retry)

    def _pcache(key_pattern, count=300, expire=0, max_retry=0):
        return pcache(key_pattern, mc, count=count, expire=expire, max_retry=max_retry)

    def _pcache2(key_pattern, count=300, expire=0):
        return pcache2(key_pattern, count=count, expire=expire, mc=mc)

    def _listcache(key_pattern, expire=0):
        return listcache(key_pattern, expire=expire, mc=mc)

    def _cache_in_obj(key, expire=0):
         return cache_in_obj(key, expire=expire, mc=mc)

    def _delete_cache(key_pattern):
        return delete_cache(key_pattern, mc=mc)

    return dict(cache=_cache, pcache=_pcache,
                pcache2=_pcache2, listcache=_listcache,
                cache_in_obj=_cache_in_obj,
                delete_cache=_delete_cache)


