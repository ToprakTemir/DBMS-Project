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
        self.table_name = table_name
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
        self.entry_size = self.catalog_entry["entry_size"]
        self.page_size = self.catalog_entry["page_size"]
        self.file_count = self.catalog_entry["file_count"]

        # files this table is stored in, (always named <table_name>_<file_index>.bat), sorted by file index
        self.files = [f for f in os.listdir(DISK_PATH) if f.startswith(self.table_name) and f.endswith('.bat')]
        self.files.sort(key=lambda x: int(x.split('_')[-1].split('.')[0]))
        self.files = [os.path.join(DISK_PATH, f) for f in self.files]  # convert to full paths

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
        open(file_path, 'wb')


    def add_record(self, field_values: Tuple[str|int]) -> None:
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
        file_path, page_number = self.search_unfilled_page()
        with open(file_path, 'r+b') as f:
            # seek to the unfilled page
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)

            page_header = f.read(self.PAGE_HEADER_SIZE)
            page_bitmap = int.from_bytes(page_header, 'big')

            # find the first available slot in the page
            slot_idx = 0
            while page_bitmap & (1 << slot_idx) and slot_idx < self.PAGE_SLOTS:
                slot_idx += 1
            if slot_idx >= self.PAGE_SLOTS: # sanity check
                raise ValueError(f"No available slots in page {page_number} of file {file_path}, even though it is returned as an unfilled page from search_unfilled_page() function.")

            page_bitmap |= (1 << slot_idx) # update the page bitmap to mark the slot as filled
            f.seek(self.FILE_HEADER_SIZE
                   + page_number * self.page_size
                   + self.PAGE_HEADER_SIZE
                   + slot_idx * self.entry_size)

            # encode the record and write it to the slot
            entry_encoded = self.encode_record(field_values)
            assert len(entry_encoded) == self.entry_size, f"Encoded entry size {len(entry_encoded)} does not match expected entry size {self.entry_size}."
            f.write(entry_encoded)

            # write the updated page header
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)
            f.write(page_bitmap.to_bytes(self.PAGE_HEADER_SIZE, 'big'))

            # update the file header to mark the page as nonempty
            f.seek(0)
            file_header = f.read(self.FILE_HEADER_SIZE)
            file_bitmap = int.from_bytes(file_header, 'big')
            file_bitmap |= (1 << page_number)
            f.seek(0)
            f.write(file_bitmap.to_bytes(self.FILE_HEADER_SIZE, 'big'))


    def search_unfilled_page(self) -> Tuple[str, int]:
        """
        Search for the first unfilled page in the table.
        :return: A tuple containing the file path and page number of the first unfilled page.
        """
        for file_path in self.files:
            with open(file_path, 'rb') as f:
                # read the file header
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

        # if no unfilled page found, all entries in all pages are filled
        # create a new file and return the first page of that file
        new_file_index = len(self.files) + 1
        new_file_name = f"{self.table_name}_{new_file_index}.bat"
        new_file_path = os.path.join(DISK_PATH, new_file_name)
        open(new_file_path, 'wb')

        self.files.append(new_file_path)
        self.catalog_entry["file_count"] += 1
        save_catalog_entry(self.table_name, self.catalog_entry) # overwrite the catalog entry
        return new_file_path, 0


    def search_record(self, key: str | int) -> tuple[dict[str, str | int], str, int, int] | None:
        """
        Search for a record in the table by the primary key.
        :param key: The primary key value to search for.
        :return: The record and location in memory if found, None otherwise.
        """
        for file_path in self.files:
            with open(file_path, 'rb') as f:
                # read the file header
                f.seek(0)
                file_header = f.read(self.FILE_HEADER_SIZE)
                file_bitmap = int.from_bytes(file_header, 'big')

                # iterate over pages in file
                for page_number in range(self.PAGES_PER_FILE):
                    if not file_bitmap & (1 << page_number):
                        continue

                    f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)
                    page_header = f.read(self.PAGE_HEADER_SIZE)
                    page_bitmap = int.from_bytes(page_header, 'big')

                    # iterate over slots in page
                    pk = list(self.fields.keys())[self.pk_idx]
                    for slot in range(self.PAGE_SLOTS):
                        if not page_bitmap & (1 << slot):
                            continue
                        f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size + self.PAGE_HEADER_SIZE + slot * self.entry_size)
                        entry_encoded = f.read(self.entry_size)
                        entry = self.decode(entry_encoded)
                        if entry[pk] == key:
                            return entry, file_path, page_number, slot


    def encode_record(self, field_values: Tuple[str|int]) -> bytes:
        """
        Encode a record from a tuple of field values to bytes.
        :param field_values: A tuple containing the field values to be encoded.
        :return: Bytes representation of the record.
        """
        if len(field_values) != self.field_count:
            raise ValueError(f"Expected {self.field_count} field values, but got {len(field_values)}.")

        entry = bytearray()
        i = 0
        for _, field_type in self.fields.items():
            if field_type == "int":
                entry.extend(int(field_values[i]).to_bytes(4, 'big'))
                i += 1
            elif field_type == "str":
                field_value = str(field_values[i]).encode('utf-8')
                if len(field_value) > 256:
                    raise ValueError(f"String value '{field_value}' exceeds maximum length of 256 characters.")
                entry.extend(field_value.ljust(256, b'\x00'))
                i += 1
        return entry

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
                record[field_name] = entry[offset:offset + 256].decode('utf-8').rstrip('\x00')
                offset += 256
            else:
                raise ValueError(f"Unsupported field type '{field_type}'.")
        return record

    def delete_record(self, pk_value: str) -> bool:
        """
        Delete a record from the table by primary key.

        :param pk_value: The primary key value identifying the record to delete.
        :return True if the record was deleted successfully, False if no record with the given primary key exists.
        :raises KeyError: If no record with the given primary key exists.
        """
        pk_field_name = list(self.fields.keys())[self.pk_idx]

        search_result = self.search_record(pk_value)
        if search_result is None:
            return False# No record found with the given primary key, nothing to delete
        entry, file_path, page_number, slot_idx = search_result

        with open(file_path, 'r+b') as f:
            # seek to the page header
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)
            page_header = f.read(self.PAGE_HEADER_SIZE)
            page_bitmap = int.from_bytes(page_header, 'big')

            # clear the slot in the bitmap
            page_bitmap &= ~(1 << slot_idx)

            # write the updated bitmap back to the file
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size)
            f.write(page_bitmap.to_bytes(self.PAGE_HEADER_SIZE, 'big'))

            # clear the entry in the page, isn't strictly necessary after marking the slot as empty but might prevent bugs
            f.seek(self.FILE_HEADER_SIZE + page_number * self.page_size + self.PAGE_HEADER_SIZE + slot_idx * self.entry_size)
            f.write(bytearray(self.entry_size))

            # check if the page is now empty, if so, update the file header
            if page_bitmap == 0:
                f.seek(0)
                file_header = f.read(self.FILE_HEADER_SIZE)
                file_bitmap = int.from_bytes(file_header, 'big')
                file_bitmap &= ~(1 << page_number)
                f.seek(0)
                f.write(file_bitmap.to_bytes(self.FILE_HEADER_SIZE, 'big'))
        return True
