import os
from typing import List, Tuple, Dict, Optional
from DBMS.utils import load_catalog_entry, save_catalog_entry
from DBMS.utils import DISK_PATH
from DBMS.exceptions import KeyConstraintViolation

class Table:
    def __init__(self, table_name, new_table_args=None):
        """
        Initialize a Table object.
        Tables might be stored in multiple files, each file containing at most PAGES_PER_FILE pages.
        The file naming convention for multiple files of a table is: `<table_name>_1.bat`, `<table_name>_2.bat`, etc.
        :param table_name: Name of the table to be created or loaded.
        :param new_table_args: Arguments required for creating a new table.
        """


        # constants
        self.PAGE_SLOTS = 8
        self.PAGES_PER_FILE = 256
        self.MAX_TABLE_NAME_LENGTH_ALLOWED = 12
        self.MAX_FIELD_NAME_LENGTH_ALLOWED = 20

        if len(table_name) > self.MAX_TABLE_NAME_LENGTH_ALLOWED:
            raise ValueError(f"Table name '{table_name}' exceeds maximum length of {self.MAX_TABLE_NAME_LENGTH_ALLOWED} characters.")

        # Page Structure: [ bitmap | record 0 | record 1 | ... | record 7 ]
        self.PAGE_HEADER_SIZE = 1 # 1 byte for bitmap of 8 bits
        self.FILE_HEADER_SIZE = 32 # 32 bytes for page bitmap of 256 bits

        # check if the table entry exists in the catalog
        self.catalog_entry = load_catalog_entry(table_name)

        # create the table and catalog entry if it does not exist
        if self.catalog_entry is None:
            if new_table_args is None:
                raise ValueError(f"Table '{table_name}' does not exist and no arguments provided to create it.")
            else:
                self._create_table(new_table_args)
                self.catalog_entry = load_catalog_entry(table_name)

        self.table_name = table_name
        self.field_count = self.catalog_entry["field_count"]
        self.pk_idx = self.catalog_entry["pk_idx"]
        self.fields = self.catalog_entry["fields"]
        self.entry_size = self.catalog_entry["entry_size"]
        self.page_size = self.catalog_entry["page_size"]
        self.file_count = self.catalog_entry["file_count"]

        # files this table is stored in, (always named <table_name>_<file_index>.bat), sorted by file index
        self.files = [f for f in os.listdir(DISK_PATH) if f.startswith(self.table_name) and f.endswith('.bat')].sort(key=lambda x: int(x.split('_')[-1].split('.')[0]))

    def _create_table(self, args: Tuple[int, int, Dict[str, str]]):
        field_count, pk_idx, fields = args

        entry_size = 0
        for field_name, field_type in fields.items():
            if len(field_type) > self.MAX_FIELD_NAME_LENGTH_ALLOWED:
                raise ValueError(f"Field type '{field_type}' for field '{field_name}' exceeds maximum length of {self.MAX_FIELD_NAME_LENGTH_ALLOWED} characters.")
            if field_type == "int":
                entry_size += 4 # bytes
            elif field_type == "str":
                entry_size += 256 # bytes, assuming max length of string is 256 characters
            else:
                raise ValueError(f"Unsupported field type '{field_type}'.")
        page_size = entry_size * self.PAGE_SLOTS + self.PAGE_HEADER_SIZE

        catalog_key = self.table_name
        catalog_entry = {
            "file_count": 1,  # Initially one file, file names are always <table_name>_<file_index>.bat
            "field_count": field_count,
            "pk_idx": pk_idx,
            "fields": fields,
            "entry_size": entry_size,
            "page_size": page_size,
        }
        save_catalog_entry(catalog_key, catalog_entry)

        file_path = os.path.join(DISK_PATH, f"{self.table_name}_1.bat")
        if os.path.exists(file_path):
            raise FileExistsError(f"No entry for table '{self.table_name}' exists in catalog, but file '{file_path}' already exists.")

        # write file header
        # file header contains a bitmap of 256 bits, each bit representing a page
        with open(file_path, 'wb') as f:
            f.write(bytearray(32))

    def add_record(self, field_values: List[str]) -> None:
        """
        Add a new record to the table.
        :param field_values: List of field values to be added as a new record.
        :raises KeyConstraintViolation: If the primary key constraint is violated.
        """
        if len(field_values) != self.field_count:
            raise ValueError(f"Expected {self.field_count} field values, but got {len(field_values)}.")

        # Check for primary key constraint violation
        pk_value = field_values[self.pk_idx]
        record_with_same_pk = self.search_record(key=pk_value)

        if record_with_same_pk is not None:
            raise KeyConstraintViolation(f"Primary key constraint violated: {pk_value} already exists in the table.")

        # add the record to first available page of first available file
        # TODO: complete

    def search_unfilled_page(self) -> Tuple[str, int]:
        """
        Search for the first unfilled page in the table.
        :return: A tuple containing the file name and page number of the first unfilled page.
        """
        for file_name in self.files:
            file_path = os.path.join(DISK_PATH, file_name)
            with open(file_path, 'rb') as f:
                # read the file header
                f.seek(0)
                file_header = f.read(self.FILE_HEADER_SIZE)
                file_bitmap = int.from_bytes(file_header, 'big')

                for page_number in range(self.PAGES_PER_FILE):
                    if not file_bitmap & (1 << page_number):
                        return file_name, page_number

                    f.seek(page_number * self.page_size)
                    page_header = f.read(self.PAGE_HEADER_SIZE)
                    page_bitmap = int.from_bytes(page_header, 'big')
                    if page_bitmap != (1 << self.PAGE_SLOTS) - 1:
                        return file_name, page_number

        # if no unfilled page found, all entries in all pages are filled
        # create a new file and return the first page of that file
        new_file_index = len(self.files) + 1
        new_file_name = f"{self.table_name}_{new_file_index}.bat"
        new_file_path = os.path.join(DISK_PATH, new_file_name)
        with open(new_file_path, 'wb') as f:
            f.write(bytearray(self.FILE_HEADER_SIZE))
        self.files.append(new_file_name)
        self.catalog_entry["file_count"] += 1
        save_catalog_entry(self.table_name, self.catalog_entry) # overwrite the catalog entry
        return new_file_name, 0


    def search_record(self, key: str | int) -> Optional[Dict[str, str|int]]:
        """
        Search for a record in the table by the primary key.
        :param key: The primary key value to search for.
        :return: The record if found, None otherwise.
        """
        for file_name in self.files:
            file_path = os.path.join(DISK_PATH, file_name)
            with open(file_path, 'rb') as f:
                # read the file header
                f.seek(0)
                file_header = f.read(self.FILE_HEADER_SIZE)
                file_bitmap = int.from_bytes(file_header, 'big')

                for page_number in range(self.PAGES_PER_FILE):
                    if not file_bitmap & (1 << page_number):
                        continue

                    f.seek(page_number * self.page_size)
                    page_header = f.read(self.PAGE_HEADER_SIZE)
                    page_bitmap = int.from_bytes(page_header, 'big')

                    entries = []
                    for slot in range(self.PAGE_SLOTS):
                        if not page_bitmap & (1 << slot):
                            continue
                        entry_encoded = f.read(self.entry_size)
                        entry = self.decode(entry_encoded)
                        entries.append(entry)

                    for entry in entries:
                        if entry.get(self.pk_idx) == key:
                            return entry


    def encode_record(self):
        raise NotImplementedError()

    def decode(self, entry: bytes) -> Dict[str, str|int]:
        """
        Decode a record from bytes to a dictionary.
        :param entry: The bytes representation of the record.
        :return: A dictionary with field names as keys and field values as values.
        """
        record = {}
        offset = 0
        for field_name, field_type in self.fields.items():
            if field_type == "int":
                record[field_name] = int.from_bytes(entry[offset:offset + 4], 'big')
                offset += 4
            elif field_type == "str":
                record[field_name] = entry[offset:offset + 256].decode('utf-8').strip('\x00')
                offset += 256
            else:
                raise ValueError(f"Unsupported field type '{field_type}'.")
        return record