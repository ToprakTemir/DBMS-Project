import os
from .utils import LOG_DISK_PATH
import time

def log_command(message: str, success: bool) -> None:
    """
    Log a message to the log file.
    For incomplete commands, the success parameter should be None. When the command is completed and the message is
    logged again with a definite status, the logs will be updated accordingly.

    :param message: The message to log.
    """

    log_file_path = os.path.join(LOG_DISK_PATH, 'log.csv')

    # Ensure the log directory exists
    if not os.path.exists(LOG_DISK_PATH):
        os.makedirs(LOG_DISK_PATH)
    if not os.path.exists(log_file_path):
        f = open(log_file_path, 'w')
        f.close()

    # if success is None, then the log is from the beginning of a command, success is not known yet
    if success is None:
        status = "incomplete"
        log = f"{int(time.time())}, {message}, {status}\n"
        f.write(log)
        return

    # if success is Not None, then last line was likely the incomplete command of the same message
    with open(log_file_path, 'r+') as f:
        lines = f.readlines()
        if lines:
            last_line = lines[-1].strip()
            fields = last_line.split(",")
            if len(fields) >= 3 and fields[1].strip() == message and fields[2].strip() == "incomplete": # remove the last line if it was incomplete and the message matches
                lines = lines[:-1]

        # Write updated log
        f.seek(0)
        f.truncate()
        f.writelines(lines)

        status = "success" if success else "failure"
        log = f"{int(time.time())}, {message}, {status}\n"
        f.write(log)