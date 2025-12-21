import os, sys
import sqlite3
from pathlib import Path

''' Merge any number of Readerware 3 SQLite exports into one master.
    Handles all lookup re-mapping and deduplication.
'''

def merge_books(source_dbs, target_db):
    """
    The main merging function.
    source_dbs: list of source DB file paths
    target_db: target DB file path
    target created from schema.sql, so repeated runs should produce the same result.
    Some failure when source DBs have repeated entries.
    Uses content hash for deduplication.
    Adds provenance info to lookup tables.
    """

    errors = 0

    src_lookups = {}        # should be created once and survives between DBs
    reverse_lookups = {}    # should be one to one, but for checking integrity failures, repeated inside some DBs

    target = sqlite3.connect(target_db)
    target.execute("PRAGMA foreign_keys = OFF")  # speed
    tc = target.cursor()

    # ------------------------------------------------------------------
    # 1. Ensure target has all the lookup tables + a content hash column
    # ------------------------------------------------------------------
    try:
        for table in ["CONTRIBUTOR", "PUBLISHER_LIST", "PUBLICATION_PLACE_LIST", "CATEGORY_LIST", "FORMAT_LIST", "LANGUAGE_LIST"]:
            tc.execute(f"ALTER TABLE {table} ADD COLUMN merge_source TEXT")  # optional provenance

        target.commit()
    except sqlite3.OperationalError:
        pass  # already done
    except Exception as e:
        print("Error altering target DB:", e)
        target.close()
        return

    # Cache dictionaries: source_value → target_ROWKEY
    lookup_maps = {
        "CONTRIBUTOR":    {},   # "Asimov, Isaac" → 42
        "PUBLISHER_LIST": {},
        "PUBLICATION_PLACE_LIST": {},
        "CATEGORY_LIST":  {},
        "FORMAT_LIST":    {},
        "LANGUAGE_LIST":  {},
    }

    def get_or_create_lookup(table, listitem):
        listitem = (listitem or "").strip()
        if not listitem:
            return None
        map_dict = lookup_maps[table]
        if listitem in map_dict:
            return map_dict[listitem]

        # Look in target first
        if table == "CONTRIBUTOR":
            field = "NAME"
        else:
            field = "LISTITEM"
        tc.execute(f"SELECT ROWKEY FROM {table} WHERE {field} = ?", (listitem,))
        row = tc.fetchone()
        if row:
            rowkey = row[0]
        else:
            # Insert new
            try:
                tc.execute(f"INSERT INTO {table} ({field},merge_source ) VALUES (?,?)", (listitem, row_dict['PROVENANCE']))
                rowkey = tc.lastrowid
            except sqlite3.IntegrityError as e:
                print(f"Integrity error inserting ({field}) ({listitem}) into ({table}): {e}")
                return None
            except Exception as e:
                print(e)
                return None
        map_dict[listitem] = rowkey
        return rowkey

    # ------------------------------------------------------------------
    # 2. Process each source database
    # ------------------------------------------------------------------
    for src_path in source_dbs:
        print(f"\nMerging {src_path} → {target_db}")
        src = sqlite3.connect(src_path)
        sc = src.cursor()
        try:
            # Verify integrity of source DB
            sc.execute("PRAGMA integrity_check")
            result = sc.fetchone()
            if result == ('ok',):
                print(f"Database '{src_path}' is valid and connection is solid.")
            else:
                print(f"Database '{src_path}' has integrity issues: {result}")

        except sqlite3.Error as e:
            print(f"Failed to connect or verify database integrity: {e}")


        # Build local lookup caches for this source (speed)
        # if not src_lookups:
        #     src_lookups = {}
        #     reverse_lookups = {}        # should be one to one, but for checking integrity failures

        for table in lookup_maps:
            if table not in src_lookups:
                src_lookups[table] = {}
            if table == "CONTRIBUTOR":
                sc.execute(f"SELECT ROWKEY, NAME FROM {table}") # different text col, sigh
            else:
                sc.execute(f"SELECT ROWKEY, LISTITEM FROM {table}")
            for rowkey, name in sc.fetchall():
                name = (name or "").strip()
                if name:
                    src_lookups[table][rowkey] = name
                    reverse_lookups.setdefault(table, {}).setdefault(name, []).append(rowkey)   

        for table, name_map in reverse_lookups.items():
            for name, keys in name_map.items():
                if len(keys) > 1:
                    print(f"Warning: In source {src_path}, {table} has duplicate entries for '{name}': {keys}")     

        # ------------------------------------------------------------------
        # 3. Iterate every book in source
        # ------------------------------------------------------------------
        sc.execute("SELECT * FROM BOOKS")
        columns = [desc[0] for desc in sc.description]

        inserted = updated = skipped = 0
        for row in sc:
            row_dict = dict(zip(columns, row))

            # 3a. Re-map all foreign keys using the master lookup tables
            for field, table in [
                ("AUTHOR",   "CONTRIBUTOR"),
                ("AUTHOR2",  "CONTRIBUTOR"),
                ("AUTHOR3",  "CONTRIBUTOR"),
                ("PUBLISHER", "PUBLISHER_LIST"),
                ("PUB_PLACE", "PUBLICATION_PLACE_LIST"),
                ("CATEGORY1", "CATEGORY_LIST"),
                ("CATEGORY2", "CATEGORY_LIST"),
                ("CATEGORY3", "CATEGORY_LIST"),
                ("FORMAT",    "FORMAT_LIST"),
                ("CONTENT_LANGUAGE",  "LANGUAGE_LIST"),
            ]:
                ''' Get the old key from the source row, look up its text name,
                    then get or create the corresponding key in the target DB 
                    and update the row_dict field to point to that new key. 
                '''
                old_key = row_dict.get(field)
                if old_key is not None:
                    old_name = src_lookups[table].get(old_key, "").strip()
                    new_key = get_or_create_lookup(table, old_name)
                    row_dict[field] = new_key

            # 3. get the current hash value for deduplication
            hash = row_dict['HASH']
            # 3c. Check if this exact book already exists in target
            tc.execute("SELECT ROWKEY FROM BOOKS WHERE HASH = ?", (hash,))
            exists = tc.fetchone()
            if exists and isinstance(exists, tuple) :
                continue    # No need to reinsert!  I hope.

            # 3d. Insert new book
            cols = ", ".join(f'"{c}"' for c in row_dict.keys())
            placeholders = ", ".join("?" for _ in row_dict)
            row_dict['ROWKEY'] = None
            try:
                tc.execute(f"INSERT INTO BOOKS ({cols}) VALUES ({placeholders})",
                        list(row_dict.values()))
            except Exception as e:
                print(f"{errors} {inserted} {row_dict['TITLE']}")
                errors += 1
                pass
            inserted += 1

        src.close()
        print(f"  → {inserted} inserted, {updated} covers upgraded, {skipped} duplicates skipped")

    target.commit()
    target.execute("VACUUM")
    target.close()
    print("\nAll done. Master database ready.")

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: Hsql2sqlite.py target_db dbSources_directory")
        sys.exit(1)

    target_db = sys.argv[1]
    source_dbs = sys.argv[2]
    if os.path.exists(target_db):
        os.unlink(target_db)
    workDir = os.path.dirname(target_db)

    # Create target DB from schema.sql
    schemaPath = os.path.join(workDir, "schema.sql")
    with open(schemaPath, "r") as f:
        try:
            schema_sql = f.read()
            target = sqlite3.connect(target_db)
            tc = target.cursor()
            tc.executescript(schema_sql)
            target.commit()
            target.close()
        except Exception as e:
            print("Error creating target DB schema:", e)
            sys.exit(1)

    # hardcoded db names for my use, ordered by size/date
    sources = [ 
        "Books To Read Next.db",
        "BorrowedBooks.db" ,
        "MyOwnBooks.db",
        "BookCatalog.db",
        "McCollough.db",
        "Readerware.db",
        "NewMcCollough.db"]
    source_dbs = [os.path.join(source_dbs, s) for s in sources]
  
    merge_books(source_dbs, target_db)
  