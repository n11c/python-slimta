# Copyright (c) 2012 Ian C. Good
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

"""Package implementing the :mod:`~slimta.queue` storage system on disk. Disk
reads and writes are run in the gevent hub's native threadpool, so they do not
block other greenlets.

"""

from __future__ import absolute_import

import os
import uuid
import os.path
import pickle
from tempfile import mkstemp

import gevent

from slimta.queue import QueueStorage
from slimta import logging

__all__ = ['DiskStorage']

log = logging.getQueueStorageLogger(__name__)


class AioFile(object):
    """Reads and writes whole files without blocking the gevent hub, by
    running the blocking file I/O in the hub's native threadpool.

    """

    def __init__(self, path, tmp_dir=None):
        self.path = path
        self.tmp_dir = tmp_dir

    @classmethod
    def _run(cls, func, *args):
        return gevent.get_hub().threadpool.apply(func, args)

    @classmethod
    def _dump_blocking(cls, path, tmp_dir, data):
        fd, filename = mkstemp(dir=tmp_dir)
        try:
            with os.fdopen(fd, 'wb') as tmp_file:
                tmp_file.write(data)
            os.rename(filename, path)
        except BaseException:
            try:
                os.unlink(filename)
            except OSError:
                pass
            raise

    @classmethod
    def _load_blocking(cls, path):
        with open(path, 'rb') as in_file:
            return in_file.read()

    def dump(self, data):
        self._run(self._dump_blocking, self.path, self.tmp_dir, data)

    def pickle_dump(self, obj):
        return self.dump(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL))

    def load(self):
        return self._run(self._load_blocking, self.path)

    def pickle_load(self):
        return pickle.loads(self.load())


class DiskOps(object):

    def __init__(self, env_dir, meta_dir, tmp_dir):
        self.env_dir = env_dir
        self.meta_dir = meta_dir
        self.tmp_dir = tmp_dir

    def check_exists(self, id):
        path = os.path.join(self.env_dir, id+'.env')
        return os.path.lexists(path)

    def write_env(self, id, envelope):
        final_path = os.path.join(self.env_dir, id+'.env')
        AioFile(final_path, self.tmp_dir).pickle_dump(envelope)

    def write_meta(self, id, meta):
        final_path = os.path.join(self.meta_dir, id+'.meta')
        AioFile(final_path, self.tmp_dir).pickle_dump(meta)

    def read_meta(self, id):
        path = os.path.join(self.meta_dir, id+'.meta')
        return AioFile(path).pickle_load()

    def read_env(self, id):
        path = os.path.join(self.env_dir, id+'.env')
        return AioFile(path).pickle_load()

    def get_ids(self):
        return [fn[:-4] for fn in os.listdir(self.env_dir)
                if fn.endswith('.env')]

    def delete_env(self, id):
        env_path = os.path.join(self.env_dir, id+'.env')
        try:
            os.remove(env_path)
        except OSError:
            pass

    def delete_meta(self, id):
        meta_path = os.path.join(self.meta_dir, id+'.meta')
        try:
            os.remove(meta_path)
        except OSError:
            pass


class DiskStorage(QueueStorage):
    """|QueueStorage| mechanism that stores |Envelope| and queue metadata in
    two separate files on disk.

    :param env_dir: Directory where queue envelope files are stored. These
                    files may be large and will not be modified after initial
                    writing.
    :param meta_dir: Directory where queue meta files are stored. These files
                     will be small and volatile.
    :param tmp_dir: Directory that may be used as scratch space. New files are
                    written here and then moved to their final destination.
                    System temp directories are used by default.

    """

    def __init__(self, env_dir, meta_dir, tmp_dir=None):
        super(DiskStorage, self).__init__()
        self.ops = DiskOps(env_dir, meta_dir, tmp_dir)

    def write(self, envelope, timestamp):
        meta = {'timestamp': timestamp, 'attempts': 0}
        while True:
            id = uuid.uuid4().hex
            if not self.ops.check_exists(id):
                self.ops.write_env(id, envelope)
                self.ops.write_meta(id, meta)
                log.write(id, envelope)
                return id

    def set_timestamp(self, id, timestamp):
        meta = self.ops.read_meta(id)
        meta['timestamp'] = timestamp
        self.ops.write_meta(id, meta)
        log.update_meta(id, timestamp=timestamp)

    def increment_attempts(self, id):
        meta = self.ops.read_meta(id)
        new_attempts = meta['attempts'] + 1
        meta['attempts'] = new_attempts
        self.ops.write_meta(id, meta)
        log.update_meta(id, attempts=new_attempts)
        return new_attempts

    def set_recipients_delivered(self, id, rcpt_indexes):
        meta = self.ops.read_meta(id)
        current = meta.get('delivered_indexes', [])
        new = current + rcpt_indexes
        meta['delivered_indexes'] = new
        self.ops.write_meta(id, meta)
        log.update_meta(id, delivered_indexes=rcpt_indexes)

    def load(self):
        for id in self.ops.get_ids():
            try:
                meta = self.ops.read_meta(id)
                yield (meta['timestamp'], id)
            except OSError:
                logging.log_exception(__name__, queue_id=id)

    def get(self, id):
        meta = self.ops.read_meta(id)
        env = self.ops.read_env(id)
        delivered_rcpts = meta.get('delivered_indexes', [])
        self._remove_delivered_rcpts(env, delivered_rcpts)
        return env, meta['attempts']

    def remove(self, id):
        self.ops.delete_env(id)
        self.ops.delete_meta(id)
        log.remove(id)


# vim:et:fdm=marker:sts=4:sw=4:ts=4
