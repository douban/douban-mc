import os
import sys
import time
import random
import socket
import traceback
from warnings import warn

import cmemcached

from douban.utils.config import read_config
from douban.utils import hashdict

from douban.utils.slog import log as slog
from functools import wraps

log = lambda message: slog('memcached', message)

def create_mc(addr, **kwargs):
    client = cmemcached.Client(addr, comp_threshold=1024, logger = log, **kwargs)
    client.set_behavior(cmemcached.BEHAVIOR_CONNECT_TIMEOUT, 10) # 0.01s
    client.set_behavior(cmemcached.BEHAVIOR_POLL_TIMEOUT, 300) # 0.3s
    client.set_behavior(cmemcached.BEHAVIOR_RETRY_TIMEOUT, 5) # 5 sec
    client.set_behavior(cmemcached.BEHAVIOR_SERVER_FAILURE_LIMIT, 2) # not used in v1.0
    return client

MUTABLE_ATTR = ('set', 'delete', 'set_multi', 'delete_multi')

def async_clean(func, async):
    @wraps(func)
    def _(arg_with_sense, *a, **kw):
        r = func(arg_with_sense, *a, **kw)
        if not r:
            if isinstance(arg_with_sense, str):
                async(arg_with_sense)
            elif isinstance(arg_with_sense, list) or \
                    isinstance(arg_with_sense, dict):
                for k in arg_with_sense:
                    async(k)
            else:
                raise Exception('calling MCManager with wrong type argument')
        return r
    return _


class MCManager(object):
    def __init__(self, config, async_cleaner=None, **kwargs):
        self.mc = None
        self.mc_config_path = None
        self.mc_config_version = None
        self.mc_config = None
        self.mc_config_change_history = []
        self.cfgreloader = None
        self.kwargs = kwargs
        self.parse_config(config)
        if async_cleaner is not None:
            # replace set/set_multi/delete/delete_multi behaviour
            # with wrapped edtion
            for attr in MUTABLE_ATTR:
                method = getattr(self.mc, attr)
                new_method = async_clean(method, async_cleaner)
                setattr(self.mc, attr, new_method)

    def parse_config(self, config):
        cfgreloader_conf = config.get('cfgreloader', {})
        self.mc_config_path = cfgreloader_conf.get('config_path', None)

        # don't explicitly close mc
        # http://code.dapps.douban.com/douban-corelib/commit/9a2884b35d0294169297b13023cf3d03300faa89#commit-linecomment-522
        #if self.mc:
        #    try:
        #        self.mc.close()
        #    except Exception, exc:
        #        print >> sys.stderr, 'Failed closing mc: ', exc

        # don't re-create mc if config does not change
        if self.mc_config == config and self.mc:
            return False

        hostname = socket.gethostname()
        disabled = config.get('disabled', False)
        in_disabled_list = hostname in config.get('disabled_client_hosts', [])
        disabled_via_env = os.environ.get('DOUBAN_CORELIB_DISABLE_MC', False)
        if disabled or in_disabled_list or disabled_via_env:
            from .debug import FakeMemcacheClient
            _mc = FakeMemcacheClient()
        else:
            _mc = create_mc(config.get('servers'), **self.kwargs)

        from .wrapper import AdjustMC, Replicated
        new_servers = config.get('new_servers',[])
        if new_servers:
            _mc = AdjustMC(_mc, create_mc(new_servers, **self.kwargs))

        backup_servers = config.get('backup_servers',[])
        if backup_servers:
            _mc = Replicated(_mc, create_mc(backup_servers, **self.kwargs))

        if config.get('log_every_actions', False) and os.getpid() % 25 == 0:
            from douban.mc.debug import LogMemcache
            _mc = LogMemcache(_mc)

        self.mc_config = config
        self.mc = _mc

        if self.mc_config_path:
            try:
                from douban.cfgreloader import cfgreloader
                self.cfgreloader = cfgreloader
            except Exception, exc:
                warn('Failed creating cfgreloader: %s' % exc)

            if self.cfgreloader:
                try:
                    self.cfgreloader.register(self.mc_config_path,
                                              self.receive_conf,
                                              identity=self)
                except Exception, exc:
                    print >> sys.stderr, \
                            'Failed registering callback', self.receive_conf, \
                            'for path', self.mc_config_path, ':', exc

        return True

    def receive_conf(self, data, version=None, mtime=None):
        ''' callback function for cfgreloader to reload lastest config
        '''

        if self.mc_config_version == version:
            return (True, '')

        try:
            config = eval(data)
            time.sleep(random.random()*3)
            if self.parse_config(config):
                self.mc_config_version = version
                version_info = {'time': time.time(), 'version': version}
                self.mc_config_change_history.append(version_info)
            return (True, '')
        except Exception, exc:
            msg = 'in douban.mc.MCManager.receive_conf, '
            msg += 'Failed parsing config received from cfgreloader: %s'
            msg = msg % exc
            msg += ''.join(traceback.format_stack())
            return (False, msg)

    def __getattr__(self, name):
        if name == 'mc':
            raise AttributeError
        return getattr(self.mc, name)

    def __repr__(self):
        return 'MCManager (%r)' % self.mc

_clients = {}
def mc_from_config(config, use_cache = True, async_cleaner = None, **kwargs):
    if isinstance(config, basestring):
        config = read_config(config, 'mc')

    cache_key = ''
    if use_cache:
        cache_key = hashdict([config, kwargs])
        mc = _clients.get(cache_key)
        if mc:
            return mc

    mc = MCManager(config, async_cleaner = async_cleaner, **kwargs)
    if use_cache and cache_key:
        _clients[cache_key] = mc

    return mc

from .decorator import create_decorators
