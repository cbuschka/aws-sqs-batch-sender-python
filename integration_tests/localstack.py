import boto3
from botocore.exceptions import ClientError


class Localstack(object):
    def __init__(self, endpoint_url):
        self.endpoint_url = endpoint_url
        self.sqs = boto3.resource('sqs', endpoint_url=self.endpoint_url, region_name='default')

    def get_queue(self, queue_name):
        return self.sqs.get_queue_by_name(QueueName=queue_name)

    def create_queues(self, config):
        for queue_name, queue_config in config.items():
            try:
                return self.sqs.create_queue(QueueName=queue_name,
                                             Attributes=queue_config.get("Attributes", {'DelaySeconds': '5'}))
            except ClientError as e:
                if "AWS.SimpleQueueService.QueueNameExists" in e.response["Error"]["Code"]:
                    return self.get_queue(queue_name)
                else:
                    raise
