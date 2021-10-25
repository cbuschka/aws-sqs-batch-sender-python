import os
import unittest
from .localstack import Localstack

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
        self.localstack = Localstack(endpoint_url="https://localhost:4566", use_ssl=True)
        self.localstack.create_queues(config=QUEUES)

    def test_nope(self):
        queue = self.localstack.get_queue("test-queue")
