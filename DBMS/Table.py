import os
from typing import List, Tuple, Dict
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

        # Page Structure: [ page number | bitmap | record 0 | record 1 | ... | record 7 ]
        self.PAGE_HEADER_SIZE = 1 + 1 # 1 byte for page number, 1 byte for bitmap of 8 bits
        self.FILE_HEADER_SIZE = 32 # 256 bits for page bitmap, equal to 32 bytes

        self.table_name = table_name

        # first check if the table entry exists in the catalog
        self.catalog_entry = load_catalog_entry(table_name)

        # create the table and catalog entry if it does not exist
        if self.catalog_entry is None:
            if new_table_args is None:
                raise ValueError(f"Table '{table_name}' does not exist and no arguments provided to create it.")
            else:
                self._create_table(new_table_args)
                self.catalog_entry = load_catalog_entry(table_name)

        self.field_count = self.catalog_entry["field_count"]
        self.pk_idx = self.catalog_entry["pk_idx"]
        self.fields = self.catalog_entry["fields"]
        self.page_size = self.catalog_entry["page_size"]
        self.file_count = self.catalog_entry["file_count"]
        self.file_path = os.path.join(DISK_PATH, f"{self.table_name}_1.bat")


    def _create_table(self, args: Tuple[int, int, Dict[str, str]]):
        field_count, pk_idx, fields = args

        page_size = 0
        for field_name, field_type in fields.items():
            if field_type == "int":
                page_size += 4 # bytes
            if field_type == "str":
                page_size += 256 # bytes, assuming max length of string is 256 characters

        catalog_key = self.table_name
        catalog_entry = {
            "file_count": 1,  # Initially one file, file names are always <table_name>_<file_index>.bat
            "field_count": field_count,
            "pk_idx": pk_idx,
            "fields": fields,
            "page_size": page_size
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

    def encode_record(self):
        raise NotImplementedError()

    def decode_record(self):
        raise NotImplementedError()