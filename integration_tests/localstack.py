import boto3
from botocore.exceptions import ClientError


class SQS(object):
    def __init__(self, endpoint_url, use_ssl=False, region_name=None):
        self.resource = boto3.resource('sqs', endpoint_url=endpoint_url, region_name=region_name, use_ssl=use_ssl)
        self.client = boto3.client('sqs', endpoint_url=endpoint_url, region_name=region_name, use_ssl=use_ssl)

    def get_queue(self, queue_name):
        return self.resource.get_queue_by_name(QueueName=queue_name)

    def create_queues(self, config):
        for queue_name, queue_config in config.items():
            try:
                return self.resource.create_queue(QueueName=queue_name,
                                                  Attributes=queue_config.get("Attributes", {'DelaySeconds': '5'}))
            except ClientError as e:
                if "AWS.SimpleQueueService.QueueNameExists" in e.response["Error"]["Code"]:
                    return self.get_queue(queue_name)
                else:
                    raise

    def receive_message(self, queue_name):
        queue = self.resource.get_queue_by_name(QueueName=queue_name)
        messages = queue.receive_messages(MaxNumberOfMessages=1,
                                          AttributeNames=["ALL"],
                                          WaitTimeSeconds=10)
        if len(messages) == 0:
            return None
        message = messages[0]
        return message.body


class Localstack(object):
    def __init__(self, endpoint_url, use_ssl):
        self.sqs = SQS(endpoint_url=endpoint_url, use_ssl=use_ssl, region_name='default')
