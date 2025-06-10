import os
import sys
import json
import time
from enum import Enum
from typing import List, Tuple, Dict, Optional

# --- Path definitions ---
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
DISK_PATH = os.path.join(PROJECT_ROOT, 'disk')
LOG_DISK_PATH = os.path.join(PROJECT_ROOT, 'log_disk')
CATALOG_PATH = os.path.join(DISK_PATH, 'catalog.json')

# --- Exceptions ---
class KeyConstraintViolation(Exception):
    """Raised when a key constraint is violated."""
    def __init__(self, message="Key constraint violation occurred."):
        super().__init__(message)

# --- Utils ---
def load_catalog_entry(entry_key: str) -> Optional[dict]:
    if not os.path.exists(CATALOG_PATH):
        return None
    try:
        with open(CATALOG_PATH, 'r') as f:
            content = f.read()
            if not content:
                return None
            catalog = json.loads(content)
    except (json.JSONDecodeError, FileNotFoundError):
        return None
    return catalog.get(entry_key)

def save_catalog_entry(entry_key: str, entry_value: dict):
    catalog = {}
    if os.path.exists(CATALOG_PATH):
        try:
            with open(CATALOG_PATH, 'r') as f:
                content = f.read()
                if content:
                    catalog = json.loads(content)
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    catalog[entry_key] = entry_value
    with open(CATALOG_PATH, 'w') as f:
        json.dump(catalog, f, indent=4)

def delete_catalog_entry(entry_key: str):
    if not os.path.exists(CATALOG_PATH):
        return
    catalog = {}
    try:
        with open(CATALOG_PATH, 'r') as f:
            content = f.read()
            if content:
                catalog = json.loads(content)
            else:
                return
    except (json.JSONDecodeError, FileNotFoundError):
        return
    if entry_key in catalog:
        del catalog[entry_key]
        with open(CATALOG_PATH, 'w') as f:
            json.dump(catalog, f, indent=4)

# --- Logger ---
class LogStatus(Enum):
    BEGIN = "BEGIN"
    SUCCESS = "success"
    FAILURE = "failure"

def log_command(message: str, status: LogStatus) -> None:
    message = message.strip()
    log_file_path = os.path.join(PROJECT_ROOT, 'log.csv')
    if not os.path.exists(log_file_path):
        f = open(log_file_path, 'w')
    else:
        f = open(log_file_path, 'a')
    log = f"{int(time.time())}, {message}, {status.value}\n"
    f.write(log)
    f.close()

# --- Table Class ---
class Table:
    def __init__(self, table_name, new_table_args=None):
        self.PAGE_SLOTS = 8
        self.PAGES_PER_FILE = 256
        self.MAX_TABLE_NAME_LENGTH_ALLOWED = 12
        self.MAX_FIELD_NAME_LENGTH_ALLOWED = 20
        if len(table_name) > self.MAX_TABLE_NAME_LENGTH_ALLOWED:
            raise ValueError(f"Table name '{table_name}' exceeds maximum length of {self.MAX_TABLE_NAME_LENGTH_ALLOWED} characters.")
        if not table_name.isalnum() or table_name.isnumeric():
            raise ValueError(f"Table name '{table_name}' must be alphanumeric and not purely numeric.")
        self.PAGE_HEADER_SIZE = 1
        self.FILE_HEADER_SIZE = 32
        self.table_name = table_name
        self.catalog_entry = load_catalog_entry(table_name)
        if self.catalog_entry is None:
            if new_table_args is None:
                raise ValueError(f"Table '{table_name}' does not exist and no arguments provided to create it.")
            else:
                self._create_table(new_table_args)
                self.catalog_entry = load_catalog_entry(table_name)
        self.field_count = self.catalog_entry["field_count"]
        self.pk_idx = self.catalog_entry["pk_idx"]
        self.fields = self.catalog_entry["fields"]
        self.entry_size = self.catalog_entry["entry_size"]
        self.page_size = self.catalog_entry["page_size"]
        self.file_count = self.catalog_entry["file_count"]
        self.files = [f for f in os.listdir(DISK_PATH) if f.startswith(self.table_name) and f.endswith('.bat')]
        self.files.sort(key=lambda x: int(x.split('_')[-1].split('.')[0]))
        self.files = [os.path.join(DISK_PATH, f) for f in self.files]
    def _create_table(self, args: Tuple[int, int, Dict[str, str]]):
        field_count, pk_idx, fields = args
        if field_count <= 0:
            raise ValueError("Table must have at least one field.")
        if not (0 <= pk_idx < field_count):
            raise ValueError(f"Primary key index {pk_idx} is out of bounds for {field_count} fields.")
        if len(fields) != field_count:
            raise ValueError("Number of fields provided does not match field count (check for duplicate field names).")
        entry_size = 0
        for field_name, field_type in fields.items():
            if len(field_name) > self.MAX_FIELD_NAME_LENGTH_ALLOWED:
                raise ValueError(f"Field name '{field_name}' exceeds maximum length of {self.MAX_FIELD_NAME_LENGTH_ALLOWED} characters.")
            if not field_name.isalnum() or field_name.isnumeric():
                raise ValueError(f"Field name '{field_name}' must be alphanumeric and not purely numeric.")
            if field_type == "int":
                entry_size += 4
            elif field_type == "str":
                entry_size += 256
            else:
                raise ValueError(f"Unsupported field type '{field_type}'.")
        page_size = entry_size * self.PAGE_SLOTS + self.PAGE_HEADER_SIZE
        catalog_key = self.table_name
        catalog_entry = {
            "file_count": 1,
            "field_count": field_count,
            "pk_idx": pk_idx,
            "fields": fields,
            "entry_size": entry_size,
            "page_size": page_size,
        }
        save_catalog_entry(catalog_key, catalog_entry)
        file_path = os.path.join(DISK_PATH, f"{self.table_name}_1.bat")
        open(file_path, 'wb')
    def add_record(self, field_values: Tuple[str|int]) -> None:
        if len(field_values) != self.field_count:
            raise ValueError(f"Expected {self.field_count} field values, but got {len(field_values)}.")
        pk_value = field_values[self.pk_idx]
        record_with_same_pk = self.search_record(key=pk_value)
        if record_with_same_pk is not None:
            raise KeyConstraintViolation(f"Primary key constraint violated: {pk_value} already exists in the table.")
        file_path, page_number = self.search_unfilled_page()
        with open(file_path, 'r+b') as f:
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)
            page_header = f.read(self.PAGE_HEADER_SIZE)
            page_bitmap = int.from_bytes(page_header, 'big')
            slot_idx = 0
            while page_bitmap & (1 << slot_idx) and slot_idx < self.PAGE_SLOTS:
                slot_idx += 1
            if slot_idx >= self.PAGE_SLOTS:
                raise ValueError(f"No available slots in page {page_number} of file {file_path}, even though it is returned as an unfilled page from search_unfilled_page() function.")
            page_bitmap |= (1 << slot_idx)
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size + self.PAGE_HEADER_SIZE + slot_idx * self.entry_size)
            entry_encoded = self.encode_record(field_values)
            assert len(entry_encoded) == self.entry_size, f"Encoded entry size {len(entry_encoded)} does not match expected entry size {self.entry_size}."
            f.write(entry_encoded)
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)
            f.write(page_bitmap.to_bytes(self.PAGE_HEADER_SIZE, 'big'))
            f.seek(0)
            file_header = f.read(self.FILE_HEADER_SIZE)
            file_bitmap = int.from_bytes(file_header, 'big')
            file_bitmap |= (1 << page_number)
            f.seek(0)
            f.write(file_bitmap.to_bytes(self.FILE_HEADER_SIZE, 'big'))
        save_catalog_entry(self.table_name, self.catalog_entry)
    def search_unfilled_page(self) -> Tuple[str, int]:
        for file_path in self.files:
            with open(file_path, 'rb') as f:
                f.seek(0)
                file_header = f.read(self.FILE_HEADER_SIZE)
                file_bitmap = int.from_bytes(file_header, 'big')
                for page_number in range(self.PAGES_PER_FILE):
                    if not file_bitmap & (1 << page_number):
                        return file_path, page_number
                    f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)
                    page_header = f.read(self.PAGE_HEADER_SIZE)
                    page_bitmap = int.from_bytes(page_header, 'big')
                    if page_bitmap != (1 << self.PAGE_SLOTS) - 1:
                        return file_path, page_number
        new_file_index = len(self.files) + 1
        new_file_name = f"{self.table_name}_{new_file_index}.bat"
        new_file_path = os.path.join(DISK_PATH, new_file_name)
        open(new_file_path, 'wb')
        self.files.append(new_file_path)
        self.catalog_entry["file_count"] += 1
        save_catalog_entry(self.table_name, self.catalog_entry)
        return new_file_path, 0
    def search_record(self, key: str | int) -> tuple[dict[str, str | int], str, int, int] | None:
        pk = list(self.fields.keys())[self.pk_idx]
        pk_type = self.fields[pk]
        search_key = key
        if pk_type == 'int':
            if not isinstance(key, int):
                try:
                    search_key = int(key)
                except (ValueError, TypeError):
                    return None
        elif pk_type == 'str':
            if not isinstance(key, str):
                search_key = str(key)
        for file_path in self.files:
            with open(file_path, 'rb') as f:
                f.seek(0)
                file_header = f.read(self.FILE_HEADER_SIZE)
                file_bitmap = int.from_bytes(file_header, 'big')
                for page_number in range(self.PAGES_PER_FILE):
                    if not file_bitmap & (1 << page_number):
                        continue
                    f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)
                    page_header = f.read(self.PAGE_HEADER_SIZE)
                    page_bitmap = int.from_bytes(page_header, 'big')
                    for slot in range(self.PAGE_SLOTS):
                        if not page_bitmap & (1 << slot):
                            continue
                        f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size + self.PAGE_HEADER_SIZE + slot * self.entry_size)
                        entry_encoded = f.read(self.entry_size)
                        entry = self.decode(entry_encoded)
                        if entry[pk] == search_key:
                            return entry, file_path, page_number, slot
        return None
    def encode_record(self, field_values: Tuple[str|int]) -> bytes:
        if len(field_values) != self.field_count:
            raise ValueError(f"Expected {self.field_count} field values, but got {len(field_values)}.")
        entry = bytearray()
        i = 0
        for _, field_type in self.fields.items():
            if field_type == "int":
                entry.extend(int(field_values[i]).to_bytes(4, 'big', signed=True))
                i += 1
            elif field_type == "str":
                field_value = str(field_values[i]).encode('utf-8')
                if len(field_value) > 256:
                    raise ValueError(f"String value '{field_value}' exceeds maximum length of 256 characters.")
                entry.extend(field_value.ljust(256, b'\x00'))
                i += 1
        return entry
    def decode(self, entry: bytes) -> Dict[str, str|int]:
        record = {}
        offset = 0
        for field_name, field_type in self.fields.items():
            if field_type == "int":
                record[field_name] = int.from_bytes(entry[offset:offset + 4], 'big', signed=True)
                offset += 4
            elif field_type == "str":
                record[field_name] = entry[offset:offset + 256].decode('utf-8').rstrip('\x00')
                offset += 256
            else:
                raise ValueError(f"Unsupported field type '{field_type}'.")
        return record
    def delete_record(self, pk_value: str | int) -> bool:
        search_result = self.search_record(pk_value)
        if search_result is None:
            return False
        _, file_path, page_number, slot_idx = search_result
        with open(file_path, 'r+b') as f:
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)
            page_header = f.read(self.PAGE_HEADER_SIZE)
            page_bitmap = int.from_bytes(page_header, 'big')
            page_bitmap &= ~(1 << slot_idx)
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)
            f.write(page_bitmap.to_bytes(self.PAGE_HEADER_SIZE, 'big'))
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size + self.PAGE_HEADER_SIZE + slot_idx * self.entry_size)
            f.write(bytearray(self.entry_size))
            if page_bitmap == 0:
                f.seek(0)
                file_header = f.read(self.FILE_HEADER_SIZE)
                file_bitmap = int.from_bytes(file_header, 'big')
                file_bitmap &= ~(1 << page_number)
                f.seek(0)
                f.write(file_bitmap.to_bytes(self.FILE_HEADER_SIZE, 'big'))
        return True

# --- Output and Command Processing ---
output_file_path = os.path.join(PROJECT_ROOT, 'output.txt')
def print_output(message: str) -> None:
    with open(output_file_path, 'a') as output_file:
        output_file.write(message + '\n')

DEBUG_MODE = True
def print_stdout(message: str) -> None:
    if DEBUG_MODE:
        print(message)

def process_command(input_line):
    try:
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
    except (ValueError, KeyError, IndexError, OverflowError) as e:
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