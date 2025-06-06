import json
import os
from typing import Optional

# Path definitions
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DISK_PATH = os.path.join(PROJECT_ROOT, 'disk')
LOG_DISK_PATH = os.path.join(PROJECT_ROOT, 'log_disk')

def load_catalog_entry(table_name: str) -> Optional[dict]:
    """
    Load the catalog entry for a given table name.
    :param table_name: Name of the table to load.
    :return: Dictionary containing the catalog entry or None if not found.
    """

    catalog_path = os.path.join(DISK_PATH, 'catalog.json')
    if not os.path.exists(catalog_path):
        print(f"Catalog file does not exist at {os.path.join(DISK_PATH, 'catalog.json')}.")
        return None

    with open(catalog_path, 'r') as f:
        catalog = json.load(f)

    return catalog.get(table_name, None)

def save_catalog_entry(table_name: str, entry: dict) -> None:
    catalog_path = os.path.join(DISK_PATH, 'catalog.json')

    if os.path.exists(catalog_path):
        with open(catalog_path, 'r') as catalog_file:
            catalog = json.load(catalog_file)
    else:
        catalog = {}

    catalog[table_name] = entry

    with open(catalog_path, 'w') as catalog_file:
        json.dump(catalog, catalog_file, indent=4)