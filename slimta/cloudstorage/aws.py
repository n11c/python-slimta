# Copyright (c) 2013 Ian C. Good
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

"""This module defines the queue storage mechanism specific to the `Amazon Web
Services`_ hosting service. It requires an account as well as the `Simple
Storage Service (S3)`_ and optionally the `Simple Queue Service (SQS)`_
services.

For each queued message, the contents and metadata of the message are written
to *S3*. Upon success, a reference to the S3 object is injected into *SQS* as a
new message.

The *SQS* service is only necessary for alerting separate *slimta* processes
that a new message has been queued. If reception and relaying are happening in
the same process, *SQS* is unnecessary.

**NOTE:** This module uses the `boto3`_ library to communicate with *AWS*. To
avoid performance issues, you must use gevent `monkey patching`_ before using
it!

::

    from gevent import monkey; monkey.patch_all()

    import boto3

    s3_bucket = boto3.resource('s3').Bucket('slimta-queue')
    s3 = SimpleStorageService(s3_bucket)

    sqs_queue = boto3.resource('sqs', region_name='us-west-2') \\
        .get_queue_by_name(QueueName='slimta-queue')
    sqs = SimpleQueueService(sqs_queue)

    queue_storage = CloudStorage(s3, sqs)

.. _Amazon Web Services: http://aws.amazon.com/
.. _Simple Storage Service (S3): http://aws.amazon.com/s3/
.. _Simple Queue Service (SQS): http://aws.amazon.com/sqs/
.. _boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
.. _monkey patching: http://gevent.org/intro.html#monkey-patching

"""

import uuid
import json
import pickle

import gevent
from botocore.exceptions import ClientError

__all__ = ['SimpleStorageService', 'SimpleQueueService']

_NOT_FOUND_CODES = ('404', 'NoSuchKey', 'NotFound')


def _parse_meta(metadata):
    meta = {'timestamp': json.loads(metadata['timestamp'])}
    attempts_raw = metadata.get('attempts')
    if attempts_raw:
        meta['attempts'] = json.loads(attempts_raw)
    delivered_raw = metadata.get('delivered_indexes')
    if delivered_raw:
        meta['delivered_indexes'] = json.loads(delivered_raw)
    return meta


class SimpleStorageService(object):
    """Instances of this class may be passed in to the
    :class:`~slimta.cloudstorage.CloudStorage` constructor for the ``storage``
    parameter to use *S3* as the storage backend.

    Keys added to the bucket are generated with ``prefix + str(uuid.uuid4())``.
    Message metadata is stored as S3 user-defined object metadata.

    :param bucket: The S3 bucket in which all message contents and metadata
                   will be written. Each created S3 object will use a
                   :py:mod:`uuid` string as its key.
    :type bucket: :class:`S3.Bucket` (a boto3 resource)
    :param timeout: Timeout, in seconds, before requests to *S3* will fail and
                    raise an exception.
    :param prefix: The string prefixed to every key added to the bucket.

    """

    def __init__(self, bucket, timeout=None, prefix=''):
        super(SimpleStorageService, self).__init__()
        self.bucket = bucket
        self.timeout = timeout
        self.prefix = prefix

    def _load_object(self, id):
        obj = self.bucket.Object(id)
        try:
            obj.load()
        except ClientError as exc:
            code = exc.response.get('Error', {}).get('Code')
            if code in _NOT_FOUND_CODES:
                raise KeyError(id)
            raise
        return obj

    def write_message(self, envelope, timestamp):
        id = self.prefix+str(uuid.uuid4())
        envelope_raw = pickle.dumps(envelope, pickle.HIGHEST_PROTOCOL)
        metadata = {'timestamp': json.dumps(timestamp),
                    'attempts': '',
                    'delivered_indexes': ''}
        with gevent.Timeout(self.timeout):
            self.bucket.put_object(Key=id, Body=envelope_raw,
                                   Metadata=metadata)
        return id

    def set_message_meta(self, id, timestamp=None, attempts=None,
                         delivered_indexes=None):
        with gevent.Timeout(self.timeout):
            obj = self._load_object(id)
            metadata = dict(obj.metadata)
            if timestamp is not None:
                metadata['timestamp'] = json.dumps(timestamp)
            if attempts is not None:
                metadata['attempts'] = json.dumps(attempts)
            if delivered_indexes is not None:
                metadata['delivered_indexes'] = json.dumps(delivered_indexes)
            # S3 object metadata is immutable: copy the object onto itself
            # with the new metadata.
            obj.copy_from(CopySource={'Bucket': self.bucket.name, 'Key': id},
                          Metadata=metadata, MetadataDirective='REPLACE')

    def delete_message(self, id):
        with gevent.Timeout(self.timeout):
            obj = self._load_object(id)
            obj.delete()

    def get_message(self, id):
        with gevent.Timeout(self.timeout):
            try:
                response = self.bucket.Object(id).get()
            except ClientError as exc:
                code = exc.response.get('Error', {}).get('Code')
                if code in _NOT_FOUND_CODES:
                    raise KeyError(id)
                raise
            envelope_raw = response['Body'].read()
        envelope = pickle.loads(envelope_raw)
        return envelope, _parse_meta(response['Metadata'])

    def get_message_meta(self, id):
        with gevent.Timeout(self.timeout):
            obj = self._load_object(id)
        return _parse_meta(obj.metadata)

    def list_messages(self):
        with gevent.Timeout(self.timeout):
            ids = [obj.key for obj in
                   self.bucket.objects.filter(Prefix=self.prefix)]
        for id in ids:
            meta = self.get_message_meta(id)
            yield (meta['timestamp'], id)


class SimpleQueueService(object):
    """Instances of this class may be passed in to the
    :class:`~slimta.cloudstorage.CloudStorage` constructor for the
    ``message_queue`` parameter to use *SQS* as the message queue backend to
    alert other processes that a new message was stored.

    :param queue: The SQS queue in which each new message corresponds to a new
                  object in storage.
    :type queue: :class:`SQS.Queue` (a boto3 resource)
    :param timeout: Timeout, in seconds, before requests to *SQS* will fail and
                    raise an exception.
    :param poll_pause: The time, in seconds, to idle between attempts to poll
                       the queue for new messages.

    """

    def __init__(self, queue, timeout=None, poll_pause=1.0):
        super(SimpleQueueService, self).__init__()
        self.queue = queue
        self.timeout = timeout
        self.poll_pause = poll_pause

    def queue_message(self, storage_id, timestamp):
        payload = {'timestamp': timestamp, 'storage_id': storage_id}
        with gevent.Timeout(self.timeout):
            self.queue.send_message(MessageBody=json.dumps(payload))

    def poll(self):
        with gevent.Timeout(self.timeout):
            messages = self.queue.receive_messages(MaxNumberOfMessages=10)
        for msg in messages:
            payload = json.loads(msg.body)
            yield (payload['timestamp'], payload['storage_id'], msg)

    def sleep(self):
        gevent.sleep(self.poll_pause)

    def delete(self, msg):
        with gevent.Timeout(self.timeout):
            msg.delete()


# vim:et:fdm=marker:sts=4:sw=4:ts=4
