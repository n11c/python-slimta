# Copyright (c) 2015 Ian C. Good
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

"""This module implements DNS resolution with :mod:`pycares`.

Queries run on the :mod:`pycares` event thread (the default since pycares 5)
and their results are handed back to the gevent hub that issued them, so
callers simply block on the returned :class:`~gevent.event.AsyncResult`.

"""

import pycares
import pycares.errno
import gevent
from gevent.event import AsyncResult  # type: ignore

__all__ = ['DNSError', 'DNSResolver', 'DNSRecord']


class DNSError(Exception):
    """Exception raised with DNS resolution failed. The exception message will
    contain more information.

    .. attribute:: errno

       The error number, as per :mod:`pycares.errno`.

    """

    def __init__(self, errno):
        str_error = pycares.errno.strerror(errno)
        if isinstance(str_error, bytes):
            str_error = str_error.decode('utf-8')
        msg = '{0} [{1}]'.format(str_error, pycares.errno.errorcode[errno])
        super(DNSError, self).__init__(msg)
        self.errno = errno


#: Maps attribute names exposed by pycares < 5 result objects to the field
#: names of the pycares 5 record data, per record data class name.
_LEGACY_ALIASES = {
    'ARecordData': {'host': 'addr'},
    'AAAARecordData': {'host': 'addr'},
    'MXRecordData': {'host': 'exchange'},
    'PTRRecordData': {'name': 'dname'},
    'NSRecordData': {'host': 'nsdname'},
    'SRVRecordData': {'host': 'target'},
    'SOARecordData': {'nsname': 'mname', 'hostmaster': 'rname',
                      'expires': 'expire', 'minttl': 'minimum'},
    'NAPTRRecordData': {'regex': 'regexp'},
    'CAARecordData': {'property': 'tag'},
}


class DNSRecord(object):
    """One answer record of a DNS query.

    The record data fields of :mod:`pycares` are available as attributes
    (e.g. ``exchange`` and ``priority`` for ``MX``), as well as the attribute
    names used by previous :mod:`pycares` versions (e.g. ``host`` for ``A`` and
    ``MX``, ``text`` for ``TXT``), so existing code keeps working.

    .. attribute:: ttl

       The time-to-live of the record, in seconds.

    .. attribute:: data

       The underlying :mod:`pycares` record data object.

    """

    __slots__ = ['ttl', 'type', 'data']

    def __init__(self, record):
        self.ttl = record.ttl
        self.type = record.type
        self.data = record.data

    @property
    def text(self):
        """The ``TXT`` record value, decoded as a string."""
        value = getattr(self.data, 'data')
        if isinstance(value, bytes):
            return value.decode('utf-8', 'replace')
        return value

    def __getattr__(self, name):
        data = object.__getattribute__(self, 'data')
        aliases = _LEGACY_ALIASES.get(type(data).__name__, {})
        return getattr(data, aliases.get(name) or name)

    def __repr__(self):
        return '<DNSRecord ttl={0!r} data={1!r}>'.format(self.ttl, self.data)


class DNSResolver(object):
    """Manages all the active DNS queries using a single, static
    :class:`pycares.Channel` object.

    .. attribute:: channel

       Before making any queries, this attribute may be set to override the
       default with a :class:`pycares.Channel` object that will manage all DNS
       queries. It must use the :mod:`pycares` event thread, i.e. it must not
       be created with a ``sock_state_cb``.

    """

    channel = None
    _channel = None

    @classmethod
    def query(cls, name, query_type):
        """Begin a DNS lookup. The result (or exception) will be in the
        returned :class:`~gevent.event.AsyncResult` when it is available.

        The result is a list of :class:`DNSRecord` objects of the requested
        type. Other records of the answer section (e.g. ``CNAME`` records
        followed to reach the answer) are left out. A query that succeeds
        without any record of the requested type fails with a
        :class:`DNSError` with ``ARES_ENODATA``.

        :param name: The DNS name to resolve.
        :type name: str
        :param query_type: The DNS query type, see
                           :meth:`pycares.Channel.query` for options. A string
                           may be given instead, e.g. ``'MX'``.
        :rtype: :class:`~gevent.event.AsyncResult`

        """
        result = AsyncResult()
        query_type = cls._get_query_type(query_type)
        loop = gevent.get_hub().loop
        callback = cls._make_callback(loop, result, query_type)
        cls._channel = cls._channel or cls.channel or pycares.Channel()
        cls._channel.query(name, query_type, callback=callback)
        return result

    @classmethod
    def _get_query_type(cls, query_type):
        if isinstance(query_type, str):
            type_attr = 'QUERY_TYPE_{0}'.format(query_type.upper())
            return getattr(pycares, type_attr)
        return query_type

    @classmethod
    def _make_callback(cls, loop, result, query_type):
        # pycares calls this from its own event thread: hand the answer back
        # to the hub that is waiting on the result.
        def callback(answer, errno):
            loop.run_callback_threadsafe(cls._result_cb, result,
                                         query_type, answer, errno)
        return callback

    @classmethod
    def _filter_answer(cls, query_type, answer):
        records = getattr(answer, 'answer', None) or []
        if query_type == pycares.QUERY_TYPE_ANY:
            return [DNSRecord(rec) for rec in records]
        return [DNSRecord(rec) for rec in records if rec.type == query_type]

    @classmethod
    def _result_cb(cls, result, query_type, answer, errno):
        if errno:
            result.set_exception(DNSError(errno))
            return
        records = cls._filter_answer(query_type, answer)
        if records:
            result.set(records)
        else:
            result.set_exception(DNSError(pycares.errno.ARES_ENODATA))


# vim:et:fdm=marker:sts=4:sw=4:ts=4
