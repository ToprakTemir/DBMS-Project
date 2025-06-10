import os
import sys
from DBMS.Table import Table
from DBMS.logger import log_command, LogStatus
from DBMS.utils import load_catalog_entry, save_catalog_entry, DISK_PATH
from DBMS.exceptions import KeyConstraintViolation

output_file_path = os.path.join(DISK_PATH, 'output.txt')
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
    # log_command(input_line, LogStatus.BEGIN) # Log the command as incomplete
    input_line_list = input_line.strip().split()
    command_type = " ".join(input_line_list[:2]) # first two words define the command type
    args = input_line_list[2:] # remaining words are arguments
    table_name = args[0]

    # CREATE COMMAND
    if command_type == "create type":
        if len(args) < 4:
            raise ValueError("Error: 'create table' command requires at least 4 arguments.")

        field_count = int(args[1])
        pk_idx = int(args[2]) - 1 # arguments are 1-indexed, convert to 0-indexed
        fields = args[3:]

        if int(len(fields)/2) != field_count:
            raise ValueError(f"Expected {field_count} field names, but got {len(fields)/2}.")

        fields_dict = {}
        i = 0
        while i < len(fields):
            fields_dict[fields[i]] = fields[i + 1]  # field name and type
            i += 2

        Table(table_name, new_table_args=(field_count, pk_idx, fields_dict))
        log_command(input_line, LogStatus.SUCCESS)
        try:
            pass
        except ValueError as e:
            print_stdout(f"Error: {e}")
            log_command(input_line, LogStatus.FAILURE)
        finally:
            return

    # ALL OTHER COMMANDS, GET TABLE SCHEMA FIRST
    table_entry = load_catalog_entry(table_name)
    if not load_catalog_entry(table_name):
        raise ValueError(f"Error: Table '{table_name}' does not exist. Please create it first.")
    table = Table(table_name)

    if command_type == "create record":
        field_values = args[1:]  # all arguments after the table name are field values

        field_count = table_entry["field_count"]
        if len(field_values) != field_count:
            raise ValueError(f"Error: Expected {field_count} field values, but got {len(field_values)}.")

        # create a new record in the table
        try:
            table.add_record(field_values)
            log_command(input_line, LogStatus.SUCCESS)  # Log the command as successful
        except KeyConstraintViolation as e:
            print_stdout(f"Error: {e}")
            log_command(input_line, LogStatus.FAILURE)

    elif command_type == "search record":
        searched_value = args[1]
        search_result = table.search_record(searched_value)
        log_command(input_line, LogStatus.SUCCESS)  # Log the command as successful

        if not search_result is None:
            found_record, _, _, _ = search_result # ignore the values of internal page and record number
            output_str = ""
            for _, value in found_record.items():
                output_str += f"{value} "
            print_output(output_str)

    elif command_type == "delete record":
        deletion_successful = table.delete_record(args[1])
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
        print_stdout("Usage: python archive.py <full_input_file_path>")
        exit(1)

    input_file_path = sys.argv[1]
    if not os.path.isfile(input_file_path):
        print_stdout(f"File {input_file_path} does not exist.")
        exit(1)

    main(input_file_path)