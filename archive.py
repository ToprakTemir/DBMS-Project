# main application file for reading inputs and calling the necessary DB operations

import os
import sys
from DBMS.Table import Table


def process_command(input_line):
    """
    Parse a command line input and return the command and its arguments.
    """
    input_line = input_line.strip().split()
    command_type = " ".join(input_line[:2]) # first two words define the command type
    args = input_line[2:] # remaining words are arguments

    if command_type == "create table":
        if len(args) < 4:
            raise ValueError("Error: 'create table' command requires at least 4 arguments.")

        table_name = args[0]
        field_count = args[1]
        pk_idx = args[2] - 1 # arguments are 1-indexed, convert to 0-indexed
        field_names = args[3:]

        if len(field_names) != field_count:
            raise ValueError(f"Expected {field_count} field names, but got {len(field_names)}.")

        new_table = Table(table_name, new_table_args=(field_count, pk_idx, field_names))

    elif command_type == "create record":
        table_name = args[0]
        # TODO: validate that the table exists, get the number of fields
        num_fields = 3 # will be taken from the table schema
        if len(args) != num_fields + 1:
            print(f"Error: 'create record {table_name}' command requires {num_fields} field values.")
            return

    elif command_type == "delete record":
        pass
    elif command_type == "search record":
        pass



def main(input_file_path):
    input_file = open(input_file_path, 'r')
    for line in input_file:
        process_command(line)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python archive.py <full_input_file_path>")
        exit(1)

    input_file_path = sys.argv[1]
    if not os.path.isfile(input_file_path):
        print(f"File {input_file_path} does not exist.")
        exit(1)

    main(input_file_path)