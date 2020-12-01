#!/usr/bin/env python

from sys import argv
import boto3
import logging

logging.basicConfig(level=logging.INFO)
session = boto3.Session()
sqs = session.client('sqs')
sts = session.client('sts')


def queue_transfer(from_queue, to_queue):
    logging.info(f"Transfer from {from_queue} to {to_queue}")
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


def list_queues():
    account_id = sts.get_caller_identity()["Account"]
    logging.info(f"Listing queues for account {account_id}")
    result = sqs.list_queues()
    for queue_url in result["QueueUrls"]:
        logging.info(f"Queue URL: {queue_url}")


def create_queue(queue_name):
    logging.info(f"Creating queue {queue_name}")
    sqs.create_queue(QueueName=queue_name)


def delete_queue(queue_name):
    logging.info(f"Deleting queue {queue_name}")
    queue_url = get_queue_url(queue_name)
    logging.info(f"Queue URL: {queue_url}")
    sqs.delete_queue(QueueUrl=queue_url)


def purge_queue(queue_name):
    logging.info(f"Purging queue {queue_name}")
    queue_url = get_queue_url(queue_name)
    logging.info(f"Queue URL: {queue_url}")
    sqs.purge_queue(QueueUrl=queue_url)


def main():
    if argv[1] == 'transfer':
        queue_transfer(argv[2], argv[3])
    elif argv[1] == 'list':
        list_queues()
    elif argv[1] == 'create':
        create_queue(argv[2])
    elif argv[1] == 'delete':
        delete_queue(argv[2])
    elif argv[1] == 'purge':
        purge_queue(argv[2])
    else:
        print('Command not recognized')


if __name__ == "__main__":
    main()