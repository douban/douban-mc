# -*- coding: utf-8 -*-

from cStringIO import StringIO
from operator import itemgetter

from douban.utils.debug import ObjCallLogger

class LogMixin(object):

    def start_log(self):
        self.mc = ObjCallLogger(self.mc)

    def stop_log(self):
        if isinstance(self.mc, ObjCallLogger):
            self.mc = self.mc.obj

    def get_log(self, detail=False):
        from collections import defaultdict
        d = defaultdict(int)
        nd = defaultdict(lambda: [0, 0])
        for call, ncall, cost in self.mc.log:
            d[call] += 1
            x = nd[ncall]
            x[0] += cost
            x[1] += 1
        sio = StringIO()
        print >> sio, "Memcache access (%s/%s calls):" % (len(d),
                                                         sum(d.itervalues()))
        print >> sio
        for ncall, (cost, times) in sorted(nd.iteritems(), key=itemgetter(1),
                                           reverse=True):
            print >> sio, "%s: %d times, %f seconds" % (ncall, times, cost)
        print >> sio
        if detail:
            print >> sio, "Detail:"
            print >> sio
            for key, n in sorted(d.iteritems()):
                print >> sio, "%s: %d times" % (key, n)
            print >> sio
        return sio.getvalue()

