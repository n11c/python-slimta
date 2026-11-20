import json
import unittest
from unittest import mock

import pytest

boto3 = pytest.importorskip('boto3')
moto = pytest.importorskip('moto')

import gevent  # noqa: E402

from slimta.envelope import Envelope  # noqa: E402
from slimta.cloudstorage import CloudStorage  # noqa: E402
from slimta.cloudstorage.aws import (  # noqa: E402
    SimpleStorageService, SimpleQueueService)

REGION = 'us-east-1'


class _AwsTestCase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        env = mock.patch.dict('os.environ', {
            'AWS_ACCESS_KEY_ID': 'testing',
            'AWS_SECRET_ACCESS_KEY': 'testing',
            'AWS_DEFAULT_REGION': REGION,
        })
        env.start()
        self.addCleanup(env.stop)
        self.aws = moto.mock_aws()
        self.aws.start()
        self.addCleanup(self.aws.stop)
        self.env = Envelope('sender@example.com',
                            ['rcpt1@example.com', 'rcpt2@example.com'])
        self.env.parse(b'Subject: test\r\n\r\nbody\r\n')


class TestSimpleStorageService(_AwsTestCase):

    def setUp(self):
        super().setUp()
        self.bucket = boto3.resource('s3', region_name=REGION) \
            .create_bucket(Bucket='slimta-test')
        self.s3 = SimpleStorageService(self.bucket, prefix='test-')

    def test_write_message(self):
        id = self.s3.write_message(self.env, 1234.0)
        self.assertTrue(id.startswith('test-'))
        obj = self.bucket.Object(id)
        obj.load()
        self.assertEqual({'timestamp': '1234.0', 'attempts': '',
                          'delivered_indexes': ''}, obj.metadata)

    def test_get_message(self):
        id = self.s3.write_message(self.env, 1234.0)
        env, meta = self.s3.get_message(id)
        self.assertEqual('sender@example.com', env.sender)
        self.assertEqual(['rcpt1@example.com', 'rcpt2@example.com'],
                         env.recipients)
        self.assertEqual('test', env.headers['Subject'])
        self.assertEqual({'timestamp': 1234.0}, meta)

    def test_set_message_meta_persists(self):
        id = self.s3.write_message(self.env, 1234.0)
        self.s3.set_message_meta(id, attempts=3)
        self.s3.set_message_meta(id, timestamp=5678.0, delivered_indexes=[1])
        self.assertEqual({'timestamp': 5678.0, 'attempts': 3,
                          'delivered_indexes': [1]},
                         self.s3.get_message_meta(id))
        env, _ = self.s3.get_message(id)
        self.assertEqual('sender@example.com', env.sender)

    def test_delete_message(self):
        id = self.s3.write_message(self.env, 1234.0)
        self.s3.delete_message(id)
        self.assertRaises(KeyError, self.s3.get_message, id)

    def test_missing_message(self):
        self.assertRaises(KeyError, self.s3.get_message, 'test-nope')
        self.assertRaises(KeyError, self.s3.get_message_meta, 'test-nope')
        self.assertRaises(KeyError, self.s3.set_message_meta, 'test-nope',
                          attempts=1)
        self.assertRaises(KeyError, self.s3.delete_message, 'test-nope')

    def test_list_messages(self):
        id1 = self.s3.write_message(self.env, 1234.0)
        id2 = self.s3.write_message(self.env, 5678.0)
        self.bucket.put_object(Key='other-key', Body=b'',
                               Metadata={'timestamp': '1.0'})
        self.assertEqual(sorted([(1234.0, id1), (5678.0, id2)]),
                         sorted(self.s3.list_messages()))


class TestSimpleQueueService(_AwsTestCase):

    def setUp(self):
        super().setUp()
        self.queue = boto3.resource('sqs', region_name=REGION) \
            .create_queue(QueueName='slimta-test')
        self.sqs = SimpleQueueService(self.queue)

    def test_queue_message_and_poll(self):
        self.sqs.queue_message('storeid1', 1234.0)
        self.sqs.queue_message('storeid2', 5678.0)
        ret = list(self.sqs.poll())
        self.assertEqual([(1234.0, 'storeid1'), (5678.0, 'storeid2')],
                         sorted((ts, id) for ts, id, _ in ret))
        body = json.loads(ret[0][2].body)
        self.assertEqual({'timestamp', 'storage_id'}, set(body))

    def test_delete(self):
        self.sqs.queue_message('storeid', 1234.0)
        (_, _, msg), = self.sqs.poll()
        self.sqs.delete(msg)
        self.queue.reload()
        self.assertEqual('0', self.queue.attributes[
            'ApproximateNumberOfMessages'])
        self.assertEqual('0', self.queue.attributes[
            'ApproximateNumberOfMessagesNotVisible'])

    def test_sleep(self):
        sqs = SimpleQueueService(None, poll_pause=13.0)
        with mock.patch.object(gevent, 'sleep') as sleep:
            sqs.sleep()
        sleep.assert_called_once_with(13.0)


class TestCloudStorageAws(_AwsTestCase):

    def test_round_trip(self):
        bucket = boto3.resource('s3', region_name=REGION) \
            .create_bucket(Bucket='slimta-test')
        queue = boto3.resource('sqs', region_name=REGION) \
            .create_queue(QueueName='slimta-test')
        storage = CloudStorage(SimpleStorageService(bucket),
                               SimpleQueueService(queue, poll_pause=0))
        id = storage.write(self.env, 1234.0)
        self.assertEqual([(1234.0, id)], list(storage.wait()))
        self.assertEqual(1, storage.increment_attempts(id))
        self.assertEqual(2, storage.increment_attempts(id))
        storage.set_recipients_delivered(id, [0])
        env, attempts = storage.get(id)
        self.assertEqual(2, attempts)
        self.assertEqual(['rcpt2@example.com'], env.recipients)
        self.assertEqual([(1234.0, id)], list(storage.load()))
        storage.remove(id)
        self.assertEqual([], list(storage.load()))


# vim:et:fdm=marker:sts=4:sw=4:ts=4
