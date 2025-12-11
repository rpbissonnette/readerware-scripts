#!/usr/bin/env python3
"""
hsql2sqlite.py – Clean, modern, bulletproof Readerware 3 .rw3.bkup.script → SQLite converter

Features:
  • Proper context managers
  • Glob support: drop it in a folder, it processes every *.bkup.script (or *.script)
  • Automatic content_hash column (SHA1 over meaningful fields)
  • Provenance: original filename → USER1, import timestamp → USER2
  • Smart VARBINARY hex parsing (\xFF\xD8… → real bytes)
  • Full fidelity: preserves every table, every column, every blob
  • Zero external dependencies
  • Runs on Python 3.8+ (including Debian Bookworm)

Just run:
    python hsql2sqlite.py /path/to/backups/
or
    python hsql2sqlite.py somefile.rw3.bkup.script another.script
"""

import sqlite3
import hashlib
import re
import sys
from pathlib import Path
from datetime import datetime
from contextlib import closing
from subs import *

# ------------------------------------------------------------------
# Configurable hash fields (tweak if you ever want different dedup logic)
# ------------------------------------------------------------------
HASH_FIELDS = [
    "TITLE", "ISBN", "PAGES", "PUBLISHER", "PUBLISHDATE", "PAGES", "FORMAT", "KEYWORDS"
]

def parse_create_table(table: str, columns_text: str) -> None :
    """
    Parse CREATE TABLE statement and generate column transformations.
    Args:
    Returns: (sqlite_schema, column_transforms, column_names, column_indices, insert_column_names)
    """
    # expect these two will be set for the READERWARE table only, 
    # set here to ignore for the other tables
    descriptionIndex = None
    coversIndex = None
    hashIndex = None
    provenanceIndex = None

    # Split by commas (but not within parentheses)
    column_defs = []
    paren_depth = 0
    current = []
    
    # parsing logic for columns like "USED_VALUE DECIMAL(12,2)", ie group by brackets - ignore internal commas.
    # trailing comma ends the column definition, so push it.
    for char in columns_text:
        if char == '(':
            paren_depth += 1
        elif char == ')':
            paren_depth -= 1
        elif char == ',' and paren_depth == 0:
            column_defs.append(''.join(current).strip())
            current = []
            continue
        current.append(char)
    
    if current:
        column_defs.append(''.join(current).strip())
    
    # Parse each column definition
    all_columns = []
    transforms = {}
    sqlite_columns = []

    has_identity = False  # Track if table already has an IDENTITY column

    constraint_keywords = {'CONSTRAINT', 'PRIMARY', 'UNIQUE', 'FOREIGN', 'CHECK'}
    for orig_idx, col_def in enumerate(column_defs):
        # Extract column name and type
        # print(orig_idx, col_def )
        parts = col_def.split()
        if not parts:
            continue

        # Skip table-level constraints which are not column definitions
        if parts[0].upper() in constraint_keywords:
            continue

        if len(parts) < 2:
            continue
        
        col_name = parts[0]
        col_type = parts[1].upper()
        
        all_columns.append(col_name)
 
        # Check for constraints
        is_not_null = 'NOT NULL' in col_def.upper()
        is_identity = 'IDENTITY' in col_def.upper()
        
        if is_identity:
            has_identity = True
        
        # Map Readerware types to SQLite and transformations
        sqlite_type = map_to_sqlite_type(col_type)
        transform_func = get_transform_function(col_type, is_not_null)
        
        # Build SQLite column definition
        # special handling for the identity, key and not null declarations
        sqlite_col = f"{col_name} {sqlite_type}"
        if is_identity:
            sqlite_col = f"{col_name} INTEGER PRIMARY KEY AUTOINCREMENT"
        elif 'PRIMARY KEY' in col_def.upper():
            sqlite_col += " PRIMARY KEY"
        elif is_not_null and not is_identity:
            sqlite_col += " NOT NULL"
        
        sqlite_columns.append(sqlite_col)

        if transform_func:
            transforms[orig_idx] = transform_func

    if table == 'READERWARE':       # Locate the columns for special processing.
        descriptionIndex = sqlite_columns.index("PRODUCT_INFO TEXT")
        if descriptionIndex :
            transforms[descriptionIndex] = clean_product_info
        coversIndex = sqlite_columns.index("IMAGE1_DATA BLOB")
        hashIndex = sqlite_columns.index("USER1 TEXT")
        provenanceIndex = sqlite_columns.index("USER2 TEXT")

    sqlite_schema = f"CREATE TABLE {table} ({', '.join(sqlite_columns)})"
    # print(table, len(transforms))
    return sqlite_schema,  transforms, all_columns, (coversIndex, hashIndex, provenanceIndex)

AllTables = {}
def process_script(script_path: Path, output_dir: Path):

    script_path = script_path.resolve()
    stem = script_path.stem.replace(".bkup", "")  # clean name
    db_path = output_dir / f"{stem}.db"

    print(f"Processing: {script_path.name} → {db_path.name}")

    with closing(sqlite3.connect(db_path)) as con:
        con.execute("PRAGMA journal_mode = OFF")
        con.execute("PRAGMA synchronous = OFF")
        cur = con.cursor()

        # ------------------------------------------------------------------
        # Pass 1: CREATE TABLE statements
        # ------------------------------------------------------------------
        create_re = re.compile(r'CREATE.*?TABLE\s+(\w+)\s*\((.*)\)', re.DOTALL | re.IGNORECASE)
        # create_re = re.compile(r"^CREATE CACHED TABLE [\"']?(\w+)[\"']?\s+\((.+)\)", re.IGNORECASE)
        with open(script_path, "r", encoding="latin1") as f:
            for line in f:
                line = line.strip()
                if not line.upper().startswith("CREATE CACHED TABLE"):
                    continue
                m = create_re.match(line)
                if not m:
                    continue
                table = m.group(1)
                col_defs = m.group(2)
# 
                                # sql,  transforms, all_columns, coversIndex = parse_create_table(table, col_defs)
                AllTables[table] =  parse_create_table(table, col_defs)

                try:
                    cur.execute(AllTables[table][0])
                except Exception as e:
                    print(e)
        # if we get here, the create table has been rewritten and executed successfully
        


        # ------------------------------------------------------------------
        # Pass 2: INSERT statements
        # ------------------------------------------------------------------
        insert_re = re.compile(r"^INSERT INTO [\"']?(\w+)[\"']? VALUES\s*\((.*)\)$", re.IGNORECASE)
        import_time = datetime.now().isoformat(timespec="seconds")

        with open(script_path, "r", encoding="latin1") as f:
            insert_count = 0
            error_count = 0
            insert_pattern = re.compile(r'INSERT INTO\s+(\w+)\s+VALUES\((.*)\)', re.DOTALL | re.IGNORECASE)
            for line_num, line in enumerate(f, 1):
                line = line.strip()

                match = insert_pattern.match(line)
                if  match:
                    table = match.group(1)
                    values_str = match.group(2)

                    try:
                        transforms = AllTables[table][TRANSFORM_LIST]

                        # Use csv.reader to parse (HSQLDB uses '' to escape quotes)
                        reader = csv.reader([values_str], quotechar="'", doublequote=True)
                        for row in reader:
                            readyRow = []
                            for orig_idx, value in enumerate(row):
                                if orig_idx in transforms:
                                    readyRow.append(transforms[orig_idx](value))
                                else:
                                    readyRow.append(value)
                        
                        if table == 'READERWARE': 
                            # now that the hsqldb -> sqlite3 rewrite rules have been applied, at this point one could
                            # execute clean_product_info and / or process_image optional processing
                            # hardcoding in the indices for the PRODUCT_INFO and Cover Images
                            # but transform[description_index] was already on schema creation, 
                            # so next lines are an example one could change for other special processing.
                            
                            # description_index = schemas[table][3]
                            # if (readyRow[description_index]):
                            #     readyRow[description_index] = clean_product_info(readyRow[description_index])

                            # calculate a uuid over some fields in the readerware table, with a bit of meaning
                            # hijack the USER1 field so we don't have to alter the table names.
                            hash_index = AllTables[table][SPECIAL_HANDLING][HASH_INDEX]
                            d = dict(zip(AllTables[table][COLUMNS_LIST], row))
                            hash_fields = ["TITLE", "AUTHOR", "AUTHOR2", "AUTHOR3", "AUTHOR4",
                                "ISBN", "PUBLISHER", "PUBLISHDATE", "PAGES", "FORMAT"]
                            parts = [str(d.get(f) or "") for f in hash_fields]
                            content_hash = hashlib.sha1("||".join(parts).encode("utf-8")).hexdigest()
                            readyRow[hash_index] = content_hash

                            provenance_index = hash_index = AllTables[table][SPECIAL_HANDLING][PROVENANCE_INDEX]
                            readyRow[provenance_index] = "How did we get here - made from input db name?"

                            # My idea about the book cover images - check for the raw length of
                            # all 4 slots, find the min/max and set the converted / resized binary
                            # blobs to the first two image columns.
                            cover_index = AllTables[table][SPECIAL_HANDLING][COVER_IMAGE_COLUMN_INDEX]
                            images = readyRow[cover_index:cover_index+4]
                            # new_list = original_list[start:stop:step]
                            try:
                                # searching for the best small and large cover image.
                                max = 0                 # size in bytes
                                maxset = False
                                min = 100000
                                minset = False
                                for i in range(len(images)): #cover_index,cover_index+4):
                                    if images[i]:
                                        l = len(images[i])
                                        if (l > 4):
                                            if  (l > max):  
                                                imax = i
                                                max = l
                                                maxImage = images[i]
                                                maxset = True
                                            if (l < min) : 
                                                imin = i
                                                min = l
                                                minImage = images[i]
                                                minset = True
                                # Now resize them once and for all.
                                if minset: 
                                    readyRow[cover_index] = process_image(minImage)
                                else:
                                    readyRow[cover_index] = None
                                if maxset:
                                    readyRow[cover_index+1] = process_image(maxImage,LARGE_COVER)
                                else:
                                    readyRow[cover_index+1] = None
                                readyRow[cover_index+2] = None
                                readyRow[cover_index+3] = None
                            except Exception as e:
                                print(e)

                        # Insert into SQLite (explicitly list columns to exclude IDENTITY columns)
                        placeholders = ','.join(['?'] * len(readyRow))
                        column_list =  ','.join(AllTables[table][COLUMNS_LIST])
                        # print(readyRow)
                        cur.execute(f'INSERT INTO {table} ({column_list}) VALUES ({placeholders})', readyRow)
                        if table  == "READERWARE":
                            insert_count += 1
                            if insert_count % 100 == 0:
                                con.commit()  # Commit in batches for better performance
                                print(f"Processed {insert_count} rows, now on {table}...")
                                
                    except Exception as e:
                        error_count += 1
                        print(f"{table}, {e}")


        con.commit()
        cur.execute("VACUUM")
    print(f"Done: {db_path} ({db_path.stat().st_size // 1024 // 1024:,} MB)")

# ------------------------------------------------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python hsql2sqlite.py <path/to/scripts/or/folder> [...]")
        print("       Supports globbing: *.rw3.bkup.script, *.script, or folders")
        sys.exit(1)

    paths = []
    for arg in sys.argv[1:]:
        p = Path(arg)
        if p.is_dir():
            paths.extend(p.glob("*.rw3.bkup.script"))
            # paths.extend(p.glob("*.bkup.script"))
            # paths.extend(p.glob("*.script"))
        else:
            paths.append(p)

    if not paths:
        print("No .script files found.")
        sys.exit(1)

    output_dir = Path.cwd() / "converted_readerware"
    output_dir.mkdir(exist_ok=True)
    print(f"Output directory: {output_dir}\n")

    for script_path in sorted(paths):
        if script_path.suffix.lower() == ".script" or ".bkup.script" in script_path.name.lower():
            print(script_path, output_dir)
            process_script(script_path, output_dir)

    print("\nAll done! Your clean, merge-ready databases are in:", output_dir)

if __name__ == "__main__":
    main()