"""Fast counter library for App Engine.

Counter increments generally only touch memcache, and occasionally enqueue a
task.  Both are very fast and low overhead.  The downside is that the counter
could undercount (e.g., if memcache data is evicted before it is persisted via
a task).  The task which increments the datastore-based counter is not
idempotent and would double-count if ran extra time(s).  However, this should
be rather exceptional based on App Engine's documentation.
"""
import logging

from google.appengine.api import memcache
from google.appengine.api.taskqueue import taskqueue
from google.appengine.ext import db, webapp

__all__ = ['get_count', 'get_counts', 'incr']


VALUE_PREFIX = 'counter:value:'
LOCK_PREFIX = 'counter:lock:'
PENDING_PREFIX = 'counter:pending:'


class Counter(db.Model):
    """Persistent storage of a counter's values"""
    # key_name is the counter's name
    value = db.IntegerProperty(indexed=False)

    @classmethod
    def kind(self):
        return 'memcache_counter'


def get_count(name):
    """Returns the count of the specified counter name.

    If it doesn't exist, 0 is returned.  For counters which do exist, the
    returned count includes the persisted (datastore) count plus the
    unpersisted memcache count plus any count waiting in the task queue.
    """
    counter = Counter.get_by_key_name(name)
    from_cache = int(memcache.get(VALUE_PREFIX + name) or BASE_VALUE) - BASE_VALUE
    pending = int(memcache.get(PENDING_PREFIX + name) or BASE_VALUE) - BASE_VALUE
    if counter:
        return counter.value + from_cache + pending
    else:
        return from_cache + pending


def get_counts(names, from_db=True, from_memcache=True):
    """Like get_count, but fetches multiple counts at once which is much
    more efficient than getting them one at a time.
    """
    if from_db:
        db_keys = [db.Key.from_path(Counter.kind(), name) for name in names]
        db_counters = db.get(db_keys)
        db_counts = [(db_counter and db_counter.value) or 0 for db_counter in db_counters]
    else:
        db_counts = [0 for name in names]

    if from_memcache:
        mc_counters = memcache.get_multi(names, VALUE_PREFIX)
        mc_counts = [int(mc_counters.get(name, BASE_VALUE)) - BASE_VALUE for name in names]
    else:
        mc_counts = [0 for name in names]

    return [sum(counts) for counts in zip(db_counts, mc_counts)]


# Memcache incr/decr only works on 64-bit *unsigned* integers, and it
# does not underflow.  By starting the memcache count at half of the
# maximum value, we can still allow both positive and negative deltas.
BASE_VALUE = 2 ** 63


def incr(name, delta=1, update_interval=10):
    """Increments a counter.  The increment is generally a memcache-only
    operation, though a task will also be enqueued about once per
    update_interval.  May under-count if memcache contents is lost.

    Args:
      name: The name of the counter.
      delta: Amount to increment counter by, defaulting to 1.
      update_interval: Approximate interval, in seconds, between updates.  Must
                       be greater than zero.
    """
    lock_key = LOCK_PREFIX + name
    delta_key = VALUE_PREFIX + name

    # update memcache
    if delta >= 0:
        v = memcache.incr(delta_key, delta, initial_value=BASE_VALUE)
    elif delta < 0:
        v = memcache.decr(delta_key, -delta, initial_value=BASE_VALUE)

    if memcache.add(lock_key, None, time=update_interval):
        # time to enqueue a new task to persist the counter
        delta_to_persist = v - BASE_VALUE
        if delta_to_persist == 0:
            return  # nothing to save

        try:
            pending_key = PENDING_PREFIX + name
            memcache.set(pending_key, v, time=update_interval)
            taskqueue.add(url='/tasks/persist_memcache_counter',
                          queue_name='memcache-counter',
                          params=dict(name=name,
                                      delta=delta_to_persist))
        except:
            # task queue failed but we already put the delta in memcache;
            # just try to enqueue the task again next interval
            return

        # we added the task --> need to decr memcache so we don't double-count
        failed = False
        if delta_to_persist > 0:
            if memcache.decr(delta_key, delta=delta_to_persist) is None:
                failed = True
        elif delta_to_persist < 0:
            if memcache.incr(delta_key, delta=-delta_to_persist) is None:
                failed = True
        if failed:
            logging.warn("counter %s reset failed (will double-count): %d",
                         name, delta_to_persist)


class PersistMemcacheCounter(webapp.RequestHandler):
    """Task handler for incrementing the datastore's counter value."""
    def post(self):
        name = self.request.get('name')
        delta = int(self.request.get('delta'))
        db.run_in_transaction(PersistMemcacheCounter.incr_counter, name, delta)

    @staticmethod
    def incr_counter(name, delta):
        c = Counter.get_by_key_name(name)
        if not c:
            c = Counter(key_name=name, value=delta)
        else:
            c.value += delta
        c.put()
        memcache.delete(PENDING_PREFIX + name)


application = webapp.WSGIApplication([(".*", PersistMemcacheCounter)], debug=True)
