import threading
import unittest
from unittest import mock

import pycares
import pycares.errno
import gevent
from gevent.event import AsyncResult

from slimta.util.dns import DNSResolver, DNSError, DNSRecord


def _record(rtype, data, ttl=300):
    return mock.Mock(type=rtype, ttl=ttl, data=data)


def _answer(*records):
    return mock.Mock(answer=list(records))


class TestDNSRecord(unittest.TestCase):

    def test_a(self):
        rec = DNSRecord(_record(pycares.QUERY_TYPE_A,
                                pycares.ARecordData(addr='1.2.3.4'), ttl=60))
        self.assertEqual('1.2.3.4', rec.addr)
        self.assertEqual('1.2.3.4', rec.host)
        self.assertEqual(60, rec.ttl)

    def test_mx(self):
        rec = DNSRecord(_record(pycares.QUERY_TYPE_MX, pycares.MXRecordData(
            priority=10, exchange='mx.example.com')))
        self.assertEqual(10, rec.priority)
        self.assertEqual('mx.example.com', rec.exchange)
        self.assertEqual('mx.example.com', rec.host)

    def test_txt(self):
        rec = DNSRecord(_record(pycares.QUERY_TYPE_TXT,
                                pycares.TXTRecordData(data=b'some reason')))
        self.assertEqual('some reason', rec.text)

    def test_unknown_attribute(self):
        rec = DNSRecord(_record(pycares.QUERY_TYPE_A,
                                pycares.ARecordData(addr='1.2.3.4')))
        with self.assertRaises(AttributeError):
            rec.nonexistent


class TestDNS(unittest.TestCase):

    def tearDown(self):
        DNSResolver._channel = None

    def test_get_query_type(self):
        self.assertEqual(pycares.QUERY_TYPE_MX,
                         DNSResolver._get_query_type('MX'))
        self.assertEqual(13, DNSResolver._get_query_type(13))

    def test_result_cb(self):
        result = AsyncResult()
        cname = _record(pycares.QUERY_TYPE_CNAME,
                        pycares.CNAMERecordData(cname='example.com'))
        a = _record(pycares.QUERY_TYPE_A, pycares.ARecordData(addr='1.2.3.4'))
        DNSResolver._result_cb(result, pycares.QUERY_TYPE_A,
                               _answer(cname, a), None)
        records = result.get()
        self.assertEqual(['1.2.3.4'], [rec.host for rec in records])

    def test_result_cb_any(self):
        result = AsyncResult()
        cname = _record(pycares.QUERY_TYPE_CNAME,
                        pycares.CNAMERecordData(cname='example.com'))
        a = _record(pycares.QUERY_TYPE_A, pycares.ARecordData(addr='1.2.3.4'))
        DNSResolver._result_cb(result, pycares.QUERY_TYPE_ANY,
                               _answer(cname, a), None)
        self.assertEqual(2, len(result.get()))

    def test_result_cb_nodata(self):
        result = AsyncResult()
        cname = _record(pycares.QUERY_TYPE_CNAME,
                        pycares.CNAMERecordData(cname='example.com'))
        DNSResolver._result_cb(result, pycares.QUERY_TYPE_A,
                               _answer(cname), None)
        with self.assertRaises(DNSError) as cm:
            result.get()
        self.assertEqual(pycares.errno.ARES_ENODATA, cm.exception.errno)

    def test_result_cb_error(self):
        result = AsyncResult()
        DNSResolver._result_cb(result, pycares.QUERY_TYPE_A, None,
                               pycares.errno.ARES_ENOTFOUND)
        with self.assertRaises(DNSError) as cm:
            result.get()
        self.assertEqual('Domain name not found [ARES_ENOTFOUND]',
                         str(cm.exception))

    def test_query(self):
        channel = mock.Mock()
        with mock.patch.object(pycares, 'Channel', return_value=channel):
            DNSResolver.query('example.com', 'A')
        channel.query.assert_called_once_with(
            'example.com', pycares.QUERY_TYPE_A, callback=mock.ANY)

    def test_query_reuses_channel(self):
        channel = mock.Mock()
        with mock.patch.object(pycares, 'Channel',
                               return_value=channel) as channel_cls:
            DNSResolver.query('example.com', 'A')
            DNSResolver.query('example.com', 'MX')
        channel_cls.assert_called_once_with()
        self.assertEqual(2, channel.query.call_count)

    def test_query_callback_from_other_thread(self):
        # pycares 5 runs callbacks on its own event thread.
        channel = mock.Mock()

        def fake_query(name, query_type, callback):
            answer = _answer(_record(pycares.QUERY_TYPE_A,
                                     pycares.ARecordData(addr='1.2.3.4')))
            threading.Thread(target=callback, args=(answer, None)).start()
        channel.query.side_effect = fake_query
        with mock.patch.object(pycares, 'Channel', return_value=channel):
            result = DNSResolver.query('example.com', 'A')
        with gevent.Timeout(5.0):
            records = result.get()
        self.assertEqual(['1.2.3.4'], [rec.host for rec in records])


# vim:et:fdm=marker:sts=4:sw=4:ts=4
