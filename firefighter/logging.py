""":mod:`firefighter.logging` --- Firehose logging handler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import logging

   from firefighter.logging import FirehoseHandler


   logger = logging.getLogger('foo')
   handler = FirehoseHandler(delivery_stream_name='bar')
   logger.addHandler(handler)
   logger.warning({'message': 'Ahoy'})

"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import base64
import logging
import threading
import time
try:
    import queue
except ImportError:
    import Queue as queue
import warnings

from boto3.session import Session

__all__ = 'FirehoseHandler', 'FirehoseWarning'
KB = 1024


class FirehoseWarning(Warning):
    """Firehose warning class. Every warning raised on
    :class:`FirehoseHanlder` should use this class.

    """
    pass


class FirehoseHandler(logging.Handler):
    """Put logs on `AWS Kinesis Data Firehose`_.

    .. _AWS Kinesis Data Firehose: https://aws.amazon.com/kinesis/data-firehose/

    :param delivery_stream_name: A firehose delivery stream name.
    :param use_queues: Deliver a log data with thread.
    :param send_interval: At least seconds to send a log data.
    :param boto3_session: An instance of :class:`boto3.session.Session`.
    :param boto3_profile_name: A name of boto3 profile name.

    """  # noqa

    END = 1

    FLUSH = 2

    #: A max batch size in bytes.
    MAX_BATCH_SIZE = 5 * KB

    # A max record count of 1 batch operation.
    MAX_BATCH_COUNT = 500

    #: A max record size in bytes.
    MAX_RECORD_SIZE = 1 * KB

    @staticmethod
    def _get_session(boto3_session, boto3_profile_name):
        import boto3
        if boto3_session:
            return boto3_session

        if boto3_profile_name:
            return Session(profile_name=boto3_profile_name)

        return boto3

    def __init__(
        self, delivery_stream_name=None, use_queues=True, send_interval=60,
        boto3_session=None, boto3_profile_name=None,
        *args, **kwargs
    ):
        super(FirehoseHandler, self).__init__(*args, **kwargs)
        self.delivery_stream_name = delivery_stream_name
        self.use_queues = use_queues
        self.send_interval = send_interval
        self.queues = {}
        self.threads = []
        self.shutting_down = False
        self.cwl_client = self._get_session(
            boto3_session, boto3_profile_name
        ).client('firehose')

    def _submit_batch(self, batch, delivery_stream_name, max_retries=5):
        """Make a request to send message to Firehose.

        :param batch: A data to be sent.
        :param delivery_stream_name: A firehose delivery stream name.
        :param max_retries: Max retry count when the making of HTTP request
                            is failed.

        """
        if len(batch) < 1:
            return
        response = None
        kwargs = {
            'DeliveryStreamName': delivery_stream_name,
            'Records': [{'Data': base64.b64encode(data)} for data in batch]
        }
        for retry in range(max_retries):
            try:
                response = self.cwl_client.put_record_batch(**kwargs)
                break
            except Exception as e:
                warnings.warn("Failed to deliver records: {}".format(e),
                              FirehoseWarning)

        if response is None or response['FailedPutCount'] > 0:
            warnings.warn("Failed to deliver records: {}".format(response),
                          FirehoseWarning)

    def emit(self, message, make_thread=threading.Thread):
        """Emit a log message. Queuing a message
        unless :attr:`FirehoseHandler.use_queues` is `false`. So that
        making an HTTP request is executed on another thread.

        :param message: A message to be logged.
        :param make_thread: A callable object to create thread.

        """
        delivery_stream_name = self.delivery_stream_name
        message = self.format(message).encode('utf-8')
        if len(message) >= self.MAX_RECORD_SIZE:
            warnings.warn(
                'message size is to large: {}'.format(len(message)),
                FirehoseWarning
            )
            return
        if self.use_queues:
            if delivery_stream_name not in self.queues:
                self.queues[delivery_stream_name] = queue.Queue()
                thread = make_thread(
                    target=self.batch_sender,
                    args=(
                        self.queues[delivery_stream_name],
                        delivery_stream_name,
                        self.send_interval,
                    )
                )
                self.threads.append(thread)
                thread.daemon = True
                thread.start()
            if self.shutting_down:
                warnings.warn(
                    "Received message after logging system shutdown",
                    FirehoseWarning
                )
            else:
                self.queues[delivery_stream_name].put(message)
        else:
            self._submit_batch([message], delivery_stream_name)

    def batch_sender(
        self,
        batch_queue,
        delivery_stream_name,
        send_interval
    ):
        """Submit queued messages to Firehose. If one of following conditions
        is corresponded.

        - :meth:`FirehoseHandler.close` is called. so
          :attr:`FirehoseHandler.END` is queued in ``batch_queue``.
        - :meth:`FirehoseHandler.flush` is called. so
          :attr:`FirehoseHandler.FLUSH` is queued in ``batch_queue``.
        - The data size of `batch_queue` is greather then
          :attr:`FirehoseHandler.MAX_BATCH_SIZE`.
        - The count of `batch_queue` is greather then
          :attr:`FirehoseHandler.MAX_BATCH_COUNT`.
        - The time of processing of `batch_queue` takes more seconds than
          ``send_interval``.

        :param batch_queue: A queue which is stored a log data to send it.
        :param delivery_stream_name: A firehose delivery stream name.
        :param send_interval: At least seconds to send a log data.

        """
        msg = None
        max_batch_size = self.MAX_BATCH_SIZE
        max_batch_count = self.MAX_BATCH_COUNT
        while msg != self.END:
            cur_batch = [] if msg is None or msg == self.FLUSH else [msg]
            cur_batch_size = sum(map(len, cur_batch))
            cur_batch_msg_count = len(cur_batch)
            cur_batch_deadline = time.time() + send_interval
            while True:
                try:
                    msg = batch_queue.get(
                        block=True,
                        timeout=max(0, cur_batch_deadline - time.time())
                    )
                except queue.Empty:
                    msg = None
                if msg is None or \
                        msg == self.END or \
                        msg == self.FLUSH or \
                        cur_batch_size + len(msg) > max_batch_size or \
                        cur_batch_msg_count >= max_batch_count or \
                        time.time() >= cur_batch_deadline:
                    self._submit_batch(cur_batch, delivery_stream_name)
                    if msg is not None:
                        batch_queue.task_done()
                    break
                elif msg:
                    cur_batch_size += len(msg)
                    cur_batch_msg_count += 1
                    cur_batch.append(msg)
                    batch_queue.task_done()

    def flush(self):
        """Flush messages."""
        if self.shutting_down:
            return
        self.commit(self.FLUSH)

    def close(self):
        """Close handler."""
        if self.shutting_down:
            return
        self.shutting_down = True
        self.commit(self.END)
        super(FirehoseHandler, self).close()

    def commit(self, event):
        for q in self.queues.values():
            q.put(event)
        for q in self.queues.values():
            q.join()
