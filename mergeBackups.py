import sqlite3
from contextlib import closing
from pathlib import Path

# -------------------------
# Configuration
# -------------------------
TARGET_DB = "mergingTarget.db"
SOURCE_DIR = "rw_converted"  # directory containing all your source DBs
SOURCE_FILES = list(Path(SOURCE_DIR).glob("*.db"))


# -------------------------
# Merge helpers
# -------------------------
def ensure_lookup_unique(cur, table, value_column):
    """
    Ensure the lookup table has a UNIQUE index on the value column.
    """
    index_name = f"idx_{table}_{value_column}"
    try:
        cur.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
            ON {table}({value_column});
        """)
    except Exception as e:
        print(e)


def merge_lookup_table(cur, table, value_column):
    """
    Insert missing values from the source table into the target lookup table.
    Idempotent using INSERT OR IGNORE + UNIQUE index.
    """
    sql = f"""
    INSERT OR IGNORE INTO {table} ({value_column})
    SELECT DISTINCT {value_column} 
    FROM src.{table} 
    WHERE {value_column} IS NOT NULL;
    """
    try:
        cur.execute(sql)
        print(f"{table}: inserted {cur.rowcount} rows")
    except Exception as e:
        print(e)

def merge_books(cur):
    """
    Merge BOOKS from attached source database.
    Resolves AUTHORS and other reference columns to target ROWKEYs.

    """
    sql = """
    INSERT OR IGNORE INTO BOOKS (
        TITLE,
        AUTHOR,
        AUTHOR2,
        AUTHOR3,
        PUBLISHER,
        PUB_PLACE,
        PAGES,
        CONTENT_LANGUAGE,
        DIMENSIONS,
        ISBN,
        FORMAT,
        CATEGORY1,
        CATEGORY2,
        CATEGORY3,
        LOCATION,
        KEYWORDS,
        READ_COUNT,
        DATE_LAST_READ,
        PRODUCT_INFO,
        MY_COMMENTS,
        OWNER,
        IMAGE1_DATA,
        IMAGE2_DATA,
        IMAGE1_LARGE_DATA,
        IMAGE2_LARGE_DATA,
        CONVERTED,
        HASH,
        PROVENANCE
    )
    SELECT
        b.TITLE,
        c1.ROWKEY,
        c2.ROWKEY,
        c3.ROWKEY,
        b.PUBLISHER,
        b.PUB_PLACE,
        b.PAGES,
        b.CONTENT_LANGUAGE,
        b.DIMENSIONS,
        b.ISBN,
        b.FORMAT,
        b.CATEGORY1,
        b.CATEGORY2,
        b.CATEGORY3,
        b.LOCATION,
        b.KEYWORDS,
        b.READ_COUNT,
        b.DATE_LAST_READ,
        b.PRODUCT_INFO,
        b.MY_COMMENTS,
        b.OWNER,
        b.IMAGE1_DATA,
        b.IMAGE2_DATA,
        b.IMAGE1_LARGE_DATA,
        b.IMAGE2_LARGE_DATA,
        b.CONVERTED,
        b.HASH,
        b.PROVENANCE
     FROM src.BOOKS b

    LEFT JOIN src.CONTRIBUTOR sc1 ON sc1.ROWKEY = b.AUTHOR
    LEFT JOIN CONTRIBUTOR c1 ON c1.NAME = sc1.NAME

    LEFT JOIN src.CONTRIBUTOR sc2 ON sc2.ROWKEY = b.AUTHOR2
    LEFT JOIN CONTRIBUTOR c2 ON c2.NAME = sc2.NAME

    LEFT JOIN src.CONTRIBUTOR sc3 ON sc3.ROWKEY = b.AUTHOR3
    LEFT JOIN CONTRIBUTOR c3 ON c3.NAME = sc3.NAME

    WHERE b.TITLE IS NOT NULL;
    """
    try:
        cur.execute(sql)
        print("BOOKS inserted:", cur.rowcount)
    except Exception as e:
        print(e)

# -------------------------
# Main merge loop
# -------------------------
def main():
    with closing(sqlite3.connect(TARGET_DB)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        try:
            cur.execute("BEGIN;")

            for table, column in [
                ("CONTRIBUTOR", "NAME"),
                ("LANGUAGE_LIST", "LISTITEM"),
                ("CATEGORY_LIST", "LISTITEM"),
                ("FORMAT_LIST", "LISTITEM"),
                ("PUBLISHER_LIST", "LISTITEM"),
                ("PUBLICATION_PLACE_LIST", "LISTITEM") ]:
                ensure_lookup_unique(cur, table, column)

            # 2️⃣ Loop over all source DBs
            for src_file in SOURCE_FILES:
                print(f"\nMerging source DB: {src_file}")
                cur.execute(f"ATTACH DATABASE '{src_file}' AS src;")

                # Merge lookup tables first
                merge_lookup_table(cur, "CONTRIBUTOR", "NAME")
                merge_lookup_table(cur, "LANGUAGE_LIST", "LISTITEM")
                merge_lookup_table(cur, "CATEGORY_LIST", "LISTITEM")
                merge_lookup_table(cur, "FORMAT_LIST", "LISTITEM")
                merge_lookup_table(cur, "PUBLISHER_LIST", "LISTITEM")
                merge_lookup_table(cur, "PUBLICATION_PLACE_LIST", "LISTITEM")

                # Merge BOOKS
                merge_books(cur)
                try:
                    conn.commit()
                    cur.execute("DETACH DATABASE src;")
                except Exception as e:
                    print(e)

            conn.commit()
            print("\nAll merges completed successfully.")

        except Exception as e:
            conn.rollback()
            raise RuntimeError("Merge failed, rolled back") from e


if __name__ == "__main__":
    main()
