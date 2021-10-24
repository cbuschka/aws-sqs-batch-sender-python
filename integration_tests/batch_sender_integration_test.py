import os
import json
import unittest
import uuid
from .localstack import Localstack
from aws_sqs_batch_sender import BatchSender

QUEUES = {
    "test-queue": {
        "Attributes": {
            'DelaySeconds': '1',
        }
    }
}


class BatchSenderIntegrationTest(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_DEFAULT_REGION"] = "default"
        os.environ["AWS_ACCESS_KEY_ID"] = "test"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
        self.sqs = Localstack(endpoint_url="http://localhost:4566", use_ssl=False).sqs
        self.sqs.create_queues(config=QUEUES)
        self.queue = self.sqs.get_queue("test-queue")

    def test_send_and_receive(self):
        self.queue.purge()
        with BatchSender(self.queue.url, self.sqs.client) as batch_sender:
            for i in range(10):
                batch_sender.send_message(Id=str(uuid.uuid4()), MessageBody="key{}".format(i))

        messages_keys = set()
        while True:
            messages = self.queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=0)
            if not messages:
                break
            for message in messages:
                messages_keys.add(message.body)
            self.queue.delete_messages(
                Entries=[{'Id': m.message_id, 'ReceiptHandle': m.receipt_handle} for m in messages])
        self.assertEqual(10, len(messages_keys))
