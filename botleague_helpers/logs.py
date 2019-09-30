import time
from collections import defaultdict

from botleague_helpers.config import in_test
from botleague_helpers.crypto import decrypt_db_key
from google.cloud import logging as gcloud_logging
import slack

stackdriver_client = gcloud_logging.Client()

"""
Usage:
# Encrypt SLACK_ERROR_BOT_TOKEN to your secrets DB

from loguru import logger as log
from botleague_helpers.logs import add_slack_error_sink, add_stackdriver_sink

add_stackdriver_sink(log, 'your-log-name')
add_slack_error_sink(log, '#your-channel-name')
"""

"""
Stackdriver severities
DEFAULT	(0) The log entry has no assigned severity level.
DEBUG	(100) Debug or trace information.
INFO	(200) Routine information, such as ongoing status or performance.
NOTICE	(300) Normal but significant events, such as start up, shut down, or a configuration change.
WARNING	(400) Warning events might cause problems.
ERROR	(500) Error events are likely to cause problems.
CRITICAL	(600) Critical events cause more severe problems or outages.
ALERT	(700) A person must take an action immediately.
EMERGENCY	(800) One or more systems are unusable.

LOGURU severities
+----------------------+------------------------+------------------------+
| Level name           | Severity value         | Logger method          |
+======================+========================+========================+
| ``TRACE``            | 5                      | |logger.trace|         |
+----------------------+------------------------+------------------------+
| ``DEBUG``            | 10                     | |logger.debug|         |
+----------------------+------------------------+------------------------+
| ``INFO``             | 20                     | |logger.info|          |
+----------------------+------------------------+------------------------+
| ``SUCCESS``          | 25                     | |logger.success|       |
+----------------------+------------------------+------------------------+
| ``WARNING``          | 30                     | |logger.warning|       |
+----------------------+------------------------+------------------------+
| ``ERROR``            | 40                     | |logger.error|         |
+----------------------+------------------------+------------------------+
| ``CRITICAL``         | 50                     | |logger.critical|      |
+----------------------+------------------------+------------------------+
"""

VALID_STACK_DRIVER_LEVELS = ['DEFAULT', 'DEBUG', 'INFO', 'NOTICE', 'WARNING',
                             'ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY']


def add_stackdriver_sink(loguru_logger, log_name):
    """Google cloud log sink in "Global" i.e.
    https://console.cloud.google.com/logs/viewer?project=silken-impulse-217423&minLogLevel=0&expandAll=false&resource=global
    """
    stackdriver_logger = stackdriver_client.logger(log_name)

    def sink(message):
        record = message.record
        level = str(record['level'])
        if level == 'SUCCESS':
            severity = 'NOTICE'
        elif level == 'TRACE':
            # Nothing lower than DEBUG in stackdriver
            severity = 'DEBUG'
        elif level == 'EXCEPTION':
            severity = 'ERROR'
        elif level in VALID_STACK_DRIVER_LEVELS:
            severity = level
        else:
            severity = 'INFO'
        stackdriver_logger.log_text(message, severity=severity)

    loguru_logger.add(sink)


class SlackMsgHash:
    last_notified: float = None
    count: int = 0

def add_slack_error_sink(loguru_logger, channel):
    client = slack.WebClient(token=decrypt_db_key('SLACK_ERROR_BOT_TOKEN'))

    msg_hashes = defaultdict(SlackMsgHash)

    def sink(message):
        import hashlib
        level = str(message.record['level'])

        def send_message():
            if in_test():
                return
            message_plus_count = f'{message}.\n' \
                f'Message duplicates in this process ' \
                f'{msg_hashes[msg_hash].count}'
            response = client.chat_postMessage(channel=channel,
                                               text=message_plus_count)
            msg_hashes[msg_hash].last_notified = time.time()
            # assert response["ok"]
            # assert response["message"]["text"] == message

        if level in ['ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY']:
            text = message.record['message']
            msg_hash = hashlib.md5(text.encode()).hexdigest()
            if msg_hash in msg_hashes:
                last_notified = msg_hashes[msg_hash].last_notified
                if time.time() - last_notified > 60 * 5:
                    send_message()
            else:
                send_message()

            msg_hashes[msg_hash].count += 1

    loguru_logger.add(sink)