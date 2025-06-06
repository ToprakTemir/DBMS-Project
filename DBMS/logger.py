import os
from utils import LOG_DISK_PATH
import time

def log_message(message: str, success: bool) -> None:
    """
    Log a message to the log file.
    :param message: The message to log.
    """

    log_file_path = os.path.join(LOG_DISK_PATH, 'log.csv')

    # Ensure the log directory exists
    if not os.path.exists(LOG_DISK_PATH):
        os.makedirs(LOG_DISK_PATH)
    if not os.path.exists(log_file_path):
        f = open(log_file_path, 'w')
        f.close()

    # Append the log entry
    with open(log_file_path, 'a') as f:
        status = "success" if success else "failure"
        unix_time = int(time.time())
        log = f"{unix_time}, {message}, {status}\n"
        f.write(log)