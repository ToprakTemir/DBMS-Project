import json
import os
from typing import Optional

# Path definitions
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DISK_PATH = os.path.join(PROJECT_ROOT, 'disk')
LOG_DISK_PATH = os.path.join(PROJECT_ROOT, 'log_disk')
CATALOG_PATH = os.path.join(DISK_PATH, 'catalog.json')

def load_catalog_entry(entry_key: str) -> Optional[dict]:
    """
    Load a specific entry from the catalog.
    :param entry_key: The key of the entry to load.
    :return: The catalog entry if found, otherwise None.
    """
    if not os.path.exists(CATALOG_PATH):
        return None
    try:
        with open(CATALOG_PATH, 'r') as f:
            # handle empty file
            content = f.read()
            if not content:
                return None
            catalog = json.loads(content)
    except (json.JSONDecodeError, FileNotFoundError):
        return None # Return None if catalog is empty or malformed
    return catalog.get(entry_key)

def save_catalog_entry(entry_key: str, entry_value: dict):
    """
    Save a specific entry to the catalog.
    :param entry_key: The key of the entry to save.
    :param entry_value: The value of the entry to save.
    """
    catalog = {}
    if os.path.exists(CATALOG_PATH):
        try:
            with open(CATALOG_PATH, 'r') as f:
                content = f.read()
                if content:
                    catalog = json.loads(content)
        except (json.JSONDecodeError, FileNotFoundError):
            pass # if file doesn't exist or is empty, start with an empty catalog
    catalog[entry_key] = entry_value
    with open(CATALOG_PATH, 'w') as f:
        json.dump(catalog, f, indent=4)

def delete_catalog_entry(entry_key: str):
    """
    Delete a specific entry from the catalog.
    :param entry_key: The key of the entry to delete.
    """
    if not os.path.exists(CATALOG_PATH):
        return

    catalog = {}
    try:
        with open(CATALOG_PATH, 'r') as f:
            content = f.read()
            if content:
                catalog = json.loads(content)
            else:
                return # empty file, nothing to delete
    except (json.JSONDecodeError, FileNotFoundError):
        return # if file is malformed or not found, nothing to delete

    if entry_key in catalog:
        del catalog[entry_key]
        with open(CATALOG_PATH, 'w') as f:
            json.dump(catalog, f, indent=4)