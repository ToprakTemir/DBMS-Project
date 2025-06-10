# Individual Contribution Report

**Personal Info**
*   **Name**: Bora Toprak Temir
*   **Student ID**: 2022400180
*   **Group Number**: 15

---

### Contributions

My primary responsibility was the design and implementation of the core database engine, which is encapsulated in the `DBMS/` directory. My work focused on the low-level details of data storage, retrieval, and management.

Key tasks I worked on include:

*   **Data Storage Model (`DBMS/Table.py`)**:
    *   Designed the page-based storage system, with each file containing 256 pages and each page holding 8 records.
    *   Implemented the file and page header bitmaps for tracking free space, enabling efficient record insertion and deletion.
    *   Developed the logic for automatically creating new `.bat` files when the existing ones for a table become full, ensuring the system can scale beyond a single file's capacity.

*   **Record Management (`DBMS/Table.py`)**:
    *   Implemented the `add_record` function, which handles finding an available slot and writing the encoded record to disk.
    *   Developed the `search_record` function to perform a full table scan based on a primary key, reading pages sequentially to avoid loading entire large files into memory.
    *   Created the `delete_record` function, which marks a record's slot as empty and updates the relevant bitmaps.

*   **Data Encoding and Decoding (`DBMS/Table.py`)**:
    *   Wrote the `encode_record` and `decode` methods to handle the serialization and deserialization of records to and from a binary format. This included packing and unpacking `int` and `str` data types according to the specified fixed-length format.

*   **Bug Fixing and Refinements**:
    *   Resolved a critical bug where searches on integer-based primary keys were failing due to incorrect type comparisons.
    *   Fixed an `OverflowError` by modifying the data encoding/decoding methods to correctly handle signed integers, allowing negative numbers to be stored.
    *   Implemented the system catalog (`disk/catalog.json`) and the utility functions in `DBMS/utils.py` to manage it.

### Teamwork

Our team adopted a divide-and-conquer strategy. We held regular check-ins to discuss progress, integrate our components, and resolve any issues that arose at the intersection of our modules, such as ensuring the command processor correctly handled the exceptions thrown by the database engine.

### Self-Reflection

This project was a fantastic learning experience in low-level systems programming. Implementing a database from scratch taught me a great deal about the complexities of data storage and file manipulation in Python. Working with binary data, managing memory with page-based structures, and thinking about I/O efficiency were challenging yet rewarding. I significantly improved my skills in debugging complex systems. This project solidified my understanding of how databases work under the hood. 