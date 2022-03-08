#!/usr/bin/env python

import os
import logging
import sys
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import time
import sqlite3
import smtplib
import json

conn = sqlite3.connect('orders.db')

logger = logging.getLogger()


def sendEmail(body):
    gmail_user = os.environ.get("GMAIL_USER")
    gmail_password = os.environ.get("GMAIL_PASSWORD")

    sent_from = gmail_user
    to = [body['email']]
    body = f"Dear {body['name']}, your order for {body['item']} has been received and is being processed. Thank you for shopping with us"

    email_text = f"""\
    From: {gmail_user}
    Subject: Order received

    {body}
    """

    try:
        smtp_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        smtp_server.ehlo()
        smtp_server.login(gmail_user, gmail_password)
        smtp_server.sendmail(sent_from, to, email_text)
        smtp_server.close()
        return True
    except Exception as ex:
        logger.warning("Something went wrong….", ex)
        return False


if __name__ == '__main__':

    # Wait for broker to come online
    time.sleep(25)
    cur = conn.cursor()

    # Create the orders table in the database
    cur.execute(
        "CREATE TABLE orders(name varchar(255), email varchar(255), item varchar(255))")

    # Parse the configuration.
    config_parser = ConfigParser()

    with open("config.ini") as f:
        config_parser.read_file(f)
        config = dict(config_parser['default'])
        config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if os.environ.get("RESET_OFFSET"):
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "orders"
    consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new orders from Kafka and add them to the database.
    try:
        while True:
            msg = consumer.poll(5.0)
            if msg is None:
                logger.warning("Waiting...")
            elif msg.error():
                logger.error(f"ERROR: {msg.error()}")
            else:
                # Save orders in the database
                data = json.loads(msg.value())
                name, email, item = data["name"], data["email"], data["item"]
                cur.execute("INSERT INTO orders VALUES(?, ?, ?)",
                            (name, email, item))
                conn.commit()
                # Extract the (optional) key and value, and print.
                logger.warning(
                    "Order created successfully....sending email to customer!")
                sentEmail = sendEmail(data)
                if sentEmail:
                    logger.warning("Email sent successfully!")
                else:
                    logger.warning("Email not sent!")

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        conn.close()
