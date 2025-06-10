# Project 4: Design & Implementation of the Dune Archive System

## 1. Introduction

This report details the design and implementation of the Dune Archive system, a simple Database Management System (DBMS) built in Python. The system ensures that records can be created, searched, and deleted efficiently and reliably. It supports creating tables (referred to as "types"), inserting records, searching for records by primary key, and deleting records, all processed from a command input file.

## 2. Design Decisions

This section outlines the core design decisions made during the implementation of the Dune Archive system.

*   **Page and File Size**:
    *   The system uses a page size of **8 records per page**. This choice provides a good balance between I/O efficiency and memory usage.
    *   Each table file (`.bat`) can contain a maximum of **256 pages**, allowing for a large number of records per table.

*   **Page Organization**:
    *   The system employs an **unpacked slotted page format**. Each page contains a header and a series of slots for records.
    *   The page header contains an **8-bit bitmap** where each bit corresponds to a record slot, indicating whether it is occupied (1) or empty (0). This allows for efficient tracking of free space within a page.

*   **Record Structure**:
    *   Records have a **fixed length**, which is determined by the schema defined in the system catalog.
    *   Supported data types are `int` (4 bytes, signed) and `str` (256 bytes, UTF-8 encoded). A fixed size of **256 bytes** is allocated for every string field, regardless of its actual length. This simplifies record management at the cost of storage space.

*   **System Catalog**:
    *   A central catalog file, `disk/catalog.json`, stores the metadata for each type. This includes the schema, field count, primary key index, and physical storage details like record and page size.

*   **Naming Conventions**:
    *   The maximum length for type names is **12 characters**.
    *   The maximum length for field names is **20 characters**.

## 3. Assumptions

The implementation of the Dune Archive system is based on the following assumptions, as outlined in the project specification:

*   All type names, field names, and string field values consist of **alphanumeric characters**.
*   Integer fields contain **signed integer values**.
*   The system only supports **`int`** and **`str`** field types.
*   The term **"type"** is used to refer to a database relation or table.

## 4. Constraints

The system is designed to operate within the following constraints:

*   Each type is stored in one or more separate binary files.
*   Data is organized into pages and records, with the structures detailed in the Data Storage Model section.
*   Each page can hold a maximum of **8 records**.
*   The **primary key** is required for all search and delete operations.
*   All `create` and `delete` operations update the page and file header bitmaps accordingly to reflect space usage.
*   The system avoids loading entire table files into memory, processing pages individually as needed for operations like searching.

## 5. System Architecture

The system is designed with a modular architecture to separate concerns and improve maintainability. While the submission guidelines suggest a single-file script, this modular approach was chosen for clarity during development. The main components are:

- **Command Processor (`archive.py`)**: The main entry point. It reads and parses commands from the input file and orchestrates the required operations.
- **Table Manager (`DBMS/Table.py`)**: The core of the DBMS. It handles all logic for table and record manipulation, including file and page management.
- **Utilities (`DBMS/utils.py`)**: Provides helper functions, primarily for managing the `catalog.json` file.
- **Logger (`DBMS/logger.py`)**: Manages logging of all operations to a CSV file.
- **Exceptions (`DBMS/exceptions.py`)**: Defines custom exceptions for handling database-specific errors.

## 6. File Structure

The project and its runtime artifacts are organized as follows:

- `archive.py`: The main script that drives the DBMS.
- `input.txt`: An example input file containing a sequence of commands.
- `output.txt`: A file located in the project root that contains the results of successful search operations. Each result is written to a new line.
- `log.csv`: The log file where all `SUCCESS` and `FAILURE` operations are recorded with a UNIX timestamp.
- `DBMS/`: A directory containing the core, modular logic of the system.
- `disk/`: A directory created at runtime to store all database files.
    - `catalog.json`: A JSON file acting as the system catalog, storing metadata for all types.
    - `<table_name>_*.bat`: Binary files that store the record data for each type.


## 7. Data Storage Model

The DBMS uses a page-based storage model, implemented within the `Table` class, to organize data on disk.

### 7.1. Physical Storage

- **Files**: Each type's data is stored in one or more binary `.bat` files. Each file can hold up to **256 pages**.
- **Pages**: Each file is divided into pages, the basic unit of I/O. Each page can store up to **8 records**.
- **Records**: Records have a fixed size, determined by the type's schema.

### 7.2. File and Page Headers

- **File Header**: A 32-byte (256-bit) bitmap at the start of each `.bat` file tracks which pages in the file are in use.
- **Page Header**: A 1-byte (8-bit) bitmap at the start of each page tracks which record slots within that page are occupied.

### 7.3. Record Operations

- **Insertion**: A new record is placed in the first available slot in the first available page, and the corresponding file and page header bitmaps are updated.
- **Deletion**: The record's slot is marked as free in the page header bitmap, and the data is cleared. If a page becomes empty, the file header is updated.
- **Search**: The system performs a full scan, reading pages sequentially to find a record by its primary key. This approach avoids loading the entire file into memory.

### 7.4. Catalog

The `disk/catalog.json` file stores all metadata for each type, including field names, types, primary key index, and calculated sizes for records and pages.

## 8. Commands

The system supports the following DDL and DML operations:

### 8.1. `create type` (DDL)

- **Syntax**: `create type <type-name> <#fields> <pk-order> <field1-name> <field1-type> ...`
- **Example**: `create type house 6 1 name str origin str leader str military_strength int wealth int spice_production int`

### 8.2. `create record` (DML)

- **Syntax**: `create record <type-name> <field1-value> ...`
- **Example**: `create record house Atreides Caladan Duke 8000 5000 150`

### 8.3. `search record` (DML)

- **Syntax**: `search record <type-name> <primary-key>`
- **Example**: `search record house Atreides`

### 8.4. `delete record` (DML)

- **Syntax**: `delete record <type-name> <primary-key>`
- **Example**: `delete record house Corrino`

## 9. Example Usage

The provided `input.txt` serves as an example of command execution.

**Input (`input.txt`):**
```
create type house 6 1 name str origin str leader str military_strength int wealth int spice_production int
create record house Atreides Caladan Duke 8000 5000 150
create type fremen 5 1 name str tribe str skill_level int allegiance str age int
create record fremen Stilgar SietchTabr 9 Atreides 45
create record house Harkonnen GiediPrime Baron 12000 3000 200
delete record house Corrino
search record fremen Stilgar
search record house Atreides
```

**Execution:**
```bash
python archive.py input.txt
```

**Output (`disk/output.txt`):**
```
Stilgar SietchTabr 9 Atreides 45 
Atreides Caladan Duke 8000 5000 150 
```

**Log File (`log_disk/log.csv`):**
The log file will contain entries for each operation, recording the timestamp, command, and status (e.g., `success` or `failure`). 