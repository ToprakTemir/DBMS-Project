import os
import sys
from DBMS.Table import Table
from DBMS.logger import log_command, LogStatus
from DBMS.utils import load_catalog_entry, save_catalog_entry, DISK_PATH, PROJECT_ROOT
from DBMS.exceptions import KeyConstraintViolation

output_file_path = os.path.join(PROJECT_ROOT, 'output.txt')
def print_output(message: str) -> None:
    with open(output_file_path, 'a') as output_file:
        output_file.write(message + '\n')

DEBUG_MODE = True
def print_stdout(message: str) -> None:
    if DEBUG_MODE:
        print(message)

def process_command(input_line):
    """
    Parse a command line input and return the command and its arguments.
    """
    input_line_list = input_line.strip().split()
    command_type = " ".join(input_line_list[:2]) # first two words define the command type
    args = input_line_list[2:] # remaining words are arguments
    table_name = args[0]

    # CREATE COMMAND
    if command_type == "create type":
        if len(args) < 4:
            log_command(input_line, LogStatus.FAILURE)
            return

        # Check if table already exists
        if load_catalog_entry(table_name) is not None:
            log_command(input_line, LogStatus.FAILURE)
            return

        try:
            field_count = int(args[1])
            pk_idx = int(args[2]) - 1 # arguments are 1-indexed, convert to 0-indexed
            fields = args[3:]

            if int(len(fields)/2) != field_count:
                log_command(input_line, LogStatus.FAILURE)
                return

            fields_dict = {}
            i = 0
            while i < len(fields):
                fields_dict[fields[i]] = fields[i + 1]  # field name and type
                i += 2

            Table(table_name, new_table_args=(field_count, pk_idx, fields_dict))
            log_command(input_line, LogStatus.SUCCESS)
        except (ValueError, IndexError) as e:
            log_command(input_line, LogStatus.FAILURE)
        finally:
            return

    # ALL OTHER COMMANDS, GET TABLE SCHEMA FIRST
    table_entry = load_catalog_entry(table_name)
    if not table_entry:
        log_command(input_line, LogStatus.FAILURE)
        return
    table = Table(table_name)

    if command_type == "create record":
        field_values = args[1:]  # all arguments after the table name are field values

        field_count = table_entry["field_count"]
        if len(field_values) != field_count:
            log_command(input_line, LogStatus.FAILURE)
            return

        # create a new record in the table
        try:
            table.add_record(field_values)
            log_command(input_line, LogStatus.SUCCESS)  # Log the command as successful
        except KeyConstraintViolation as e:
            log_command(input_line, LogStatus.FAILURE)

    elif command_type == "search record":
        searched_value = args[1]
        search_result = table.search_record(searched_value)

        if not search_result is None:
            log_command(input_line, LogStatus.SUCCESS)  # Log the command as successful
            found_record, _, _, _ = search_result # ignore the values of internal page and record number
            output_str = ""
            for _, value in found_record.items():
                output_str += f"{value} "
            print_output(output_str)
        else:
            log_command(input_line, LogStatus.FAILURE)

    elif command_type == "delete record":
        # convert pk to int if the pk is an int
        pk_field_type = list(table.fields.values())[table.pk_idx]
        pk_value = args[1]
        if pk_field_type == 'int':
            pk_value = int(pk_value)

        deletion_successful = table.delete_record(pk_value)
        if deletion_successful:
            log_command(input_line, LogStatus.SUCCESS)
        else:
            log_command(input_line, LogStatus.FAILURE)



def main(input_file_path):
    input_file = open(input_file_path, 'r')
    if not os.path.exists(DISK_PATH):
        os.mkdir(DISK_PATH)
    for line in input_file:
        process_command(line)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: python archive.py <full_input_file_path>\n")
        exit(1)

    input_file_path = sys.argv[1]
    if not os.path.isfile(input_file_path):
        sys.stderr.write(f"File {input_file_path} does not exist.\n")
        exit(1)

    main(input_file_path)