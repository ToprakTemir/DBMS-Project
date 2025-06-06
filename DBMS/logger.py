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
        unix_time = int(time.time())
        if success is None: # the log is from the request of command, success is not known yet
            status = "incomplete"
            log = f"{unix_time}, {message}, {status}\n"
            f.write(log)
            return

        # if success is Not None, then last line was likely the incomplete command of the same message

        # delete last line if it was the same command and incomplete
        last_command = f.readlines()[-1] if os.path.getsize(log_file_path) > 0 else ""
        if last_command.strip().split(",")[2] == "incomplete" and last_command.strip().split(",")[1] == message:
            f.seek(0, os.SEEK_END)
            f.seek(f.tell() - len(last_command))
            f.truncate()

        status = "success" if success else "failure"
        log = f"{unix_time}, {message}, {status}\n"
        f.write(log)