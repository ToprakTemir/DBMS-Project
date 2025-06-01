# main application file for reading inputs and calling the necessary DB operations

import os
import sys

def parse_command(input_line):
    """
    Parse a command line input and return the command and its arguments.
    """
    pass


def main(input_file_path):
    input_file = open(input_file_path, 'r')
    for line in input_file:
        parse_command(line)

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: python archive.py <full_input_file_path>")
        exit(1)

    input_file_path = sys.argv[1]
    if not os.path.isfile(input_file_path):
        print(f"File {input_file_path} does not exist.")
        exit(1)

    main(input_file_path)