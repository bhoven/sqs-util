#!/usr/bin/env python

from sys import argv
import boto3
import logging

logging.basicConfig(level=logging.INFO)
sqs = boto3.client('sqs')


def queue_transfer(from_queue, to_queue):
    from_queue_url = get_queue_url(from_queue)
    to_queue_url = get_queue_url(to_queue)

    logging.info(f"From URL: {from_queue_url}")
    logging.info(f"To URL: {to_queue_url}")

    response = sqs.receive_message(
        QueueUrl=from_queue_url,
        MaxNumberOfMessages=10
    )
    while 'Messages' in response:
        for message in response['Messages']:
            logging.info("Transferring message...")
            sqs.send_message(
                QueueUrl=to_queue_url,
                MessageBody=message['Body']
            )
            sqs.delete_message(
                QueueUrl=from_queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        response = sqs.receive_message(
            QueueUrl=from_queue_url,
            MaxNumberOfMessages=10
        )


def get_queue_url(queue_name):
    return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]


def main():
    if argv[1] == 'transfer':
        queue_transfer(argv[2], argv[3])
    else:
        print('Command not recognized')


if __name__ == "__main__":
    main()