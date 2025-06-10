# Dune Archive System

The Dune Archive System is a simple Database Management System (DBMS) built in Python. It supports creating tables (referred to as "types"), inserting records, searching for records by primary key, and deleting records, all processed from a command input file.

## Requirements

*   Python 3.x

## Installation

It is recommended to use a virtual environment to keep dependencies isolated.

```bash
# Create and activate a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`

# Install dependencies
pip install -r requirements.txt
```

## Usage

The main script for running the DBMS is `archive.py`. It takes a single argument: the path to an input file containing the commands to be executed.

1.  **Prepare an input file** (e.g., `input.txt`) with a sequence of commands. This file can be located anywhere on your system.

    **Example `input.txt`:**
    ```
    create type house 6 1 name str origin str leader str militarystrength int wealth int spiceproduction int
    create record house Atreides Caladan Duke 8000 5000 150
    search record house Atreides
    ```

2.  **Run the script:**
    Provide the  path to your input file.

    ```bash
    python3 2022400180_2022400210/archive.py fullinputfilepath
    ```

3.  **Check the output:**
    -   Successful search results will be printed to `output.txt` in the project's root directory.
    -   All operations (both successful and failed) will be logged in `log.csv` in the project's root directory.
    -   All database files are stored in the `disk/` directory, created in the project's root directory.