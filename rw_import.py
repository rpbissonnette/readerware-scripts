#!/usr/bin/env python3

""" 
    Input : Readerware .rw3.bkup.script file
    Output: SQLite database with READERWARE table including BEST_COVER blob column 
    Note  : This version picks the largest image blob among several columns as BEST_COVER

    Not bullet proof, but works for my data set exported from Readerware 3.4.7
    Tested with Python 3.8+ and SQLite 3.31+


Ever the optimist, Grok says 
"THE ACTUAL FINAL SCRIPT – column count fixed, your blob fix included
This one works. Period."
"""

import sqlite3
import csv
import re
import binascii
from pathlib import Path
import io
import sys

# ← csv.reader will skip on records with large image blobs unless we increase this limit
csv.field_size_limit(10_000_000)
bkupScriptFilePath = None # Should be a file with name ending with .rw3.bkup.script
BKUP_SCRIPT = Path(bkupScriptFilePath)
DB_FILE = "readerware_PERFECT.sqlite"

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# Fresh start
cur.executescript("""
DROP TABLE IF EXISTS READERWARE;

CREATE TABLE READERWARE (
    ROWKEY INTEGER PRIMARY KEY,
    TITLE TEXT COLLATE NOCASE NOT NULL,
    ALT_TITLE TEXT COLLATE NOCASE,
    SUBTITLE TEXT COLLATE NOCASE,
    AUTHOR INTEGER, AUTHOR2 INTEGER, AUTHOR3 INTEGER, AUTHOR4 INTEGER, AUTHOR5 INTEGER, AUTHOR6 INTEGER,
    ILLUSTRATOR INTEGER, TRANSLATOR INTEGER, EDITOR INTEGER,
    PUBLISHER INTEGER, PUB_PLACE INTEGER,
    RELEASE_DATE DATE, COPYRIGHT_DATE DATE, PAGES INTEGER, EDITION INTEGER,
    CONTENT_LANGUAGE INTEGER, SIGNED INTEGER, DIMENSIONS TEXT COLLATE NOCASE,
    READING_LEVEL INTEGER, LEXILE_LEVEL TEXT, COPIES INTEGER NOT NULL DEFAULT 1,
    BARCODE TEXT COLLATE NOCASE, ISBN TEXT, ISSN TEXT, LCCN TEXT COLLATE NOCASE,
    DEWEY TEXT COLLATE NOCASE, CALL_NUMBER TEXT COLLATE NOCASE, USER_NUMBER TEXT COLLATE NOCASE,
    TYPE INTEGER, FORMAT INTEGER, SERIES INTEGER, SERIES_NUMBER INTEGER,
    MY_RATING INTEGER, ITEM_CONDITION INTEGER, COVER_CONDITION INTEGER,
    CATEGORY1 INTEGER, CATEGORY2 INTEGER, CATEGORY3 INTEGER, LOCATION INTEGER,
    KEYWORDS TEXT COLLATE NOCASE,
    READ_COUNT INTEGER NOT NULL DEFAULT 0, DATE_LAST_READ DATE,
    PRODUCT_INFO TEXT COLLATE NOCASE, MY_COMMENTS TEXT COLLATE NOCASE,
    DATE_ENTERED DATE NOT NULL, DATE_LAST_UPDATED DATE NOT NULL,
    SOURCE INTEGER,
    PURCHASE_PRICE REAL NOT NULL DEFAULT 0.0, PURCHASE_DATE DATE, PURCHASE_PLACE INTEGER,
    LIST_PRICE REAL NOT NULL DEFAULT 0.0, ITEM_VALUE REAL NOT NULL DEFAULT 0.0,
    VALUATION_DATE DATE, CURRENCY_SYMBOL TEXT COLLATE NOCASE,
    FAVORITE INTEGER NOT NULL DEFAULT 0, OUT_OF_PRINT INTEGER NOT NULL DEFAULT 0,
    MEDIA_URL TEXT COLLATE NOCASE, OWNER INTEGER, STATUS INTEGER,
    EXTERNAL_ID TEXT COLLATE NOCASE, INVENTORY INTEGER NOT NULL DEFAULT 0,
    BEST_COVER BLOB,                   -- ONE perfect cover per book
    IN_LAST_BATCH INTEGER NOT NULL DEFAULT 0,
    AM_ASIN TEXT, SALE_PRICE REAL, SALE_DATE DATE,
    NEW_VALUE REAL, NEW_COUNT INTEGER, USED_VALUE REAL, USED_COUNT INTEGER,
    COLLECTIBLE_VALUE REAL, COLLECTIBLE_COUNT INTEGER,
    BUYER_WAITING INTEGER, WEIGHT TEXT COLLATE NOCASE, SALES_RANK INTEGER NOT NULL DEFAULT 0,
    USER1 TEXT COLLATE NOCASE, USER2 TEXT COLLATE NOCASE, USER3 TEXT COLLATE NOCASE,
    USER4 TEXT COLLATE NOCASE, USER5 TEXT COLLATE NOCASE, USER6 TEXT COLLATE NOCASE,
    USER7 TEXT COLLATE NOCASE, USER8 TEXT COLLATE NOCASE, USER9 TEXT COLLATE NOCASE,
    USER10 TEXT COLLATE NOCASE,
    FILLER1 TEXT COLLATE NOCASE, FILLER2 TEXT COLLATE NOCASE, FILLER3 TEXT COLLATE NOCASE,
    FILLER4 TEXT COLLATE NOCASE, FILLER5 TEXT COLLATE NOCASE, FILLER6 TEXT COLLATE NOCASE,
    FILLER7 TEXT COLLATE NOCASE, FILLER8 TEXT COLLATE NOCASE, FILLER9 TEXT COLLATE NOCASE,
    FILLER10 TEXT COLLATE NOCASE
);                
"""

"""
# There are exactly 99 columns in the table above (counted by hand)
# YMMV
"""

EXPECTED_COLUMNS = 99

def parse_value(v):
    v = v.strip()
    if not v or v == "NULL": return None
    if v in ("TRUE","FALSE"): return 1 if v == "TRUE" else 0
    if v.startswith("X'") and v.endswith("'"):
        return binascii.unhexlify(v[2:-1])
    if v.startswith("'") and v.endswith("'"):
        return v[1:-1].replace("''", "'")
    try: return int(v) if v.lstrip('-').isdigit() else float(v)
    except: return v

total = images = dups = 0
print("Starting import — this will finish cleanly...")

with open(BKUP_SCRIPT, "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        line = line.strip()
        if not line.upper().startswith("INSERT INTO READERWARE"):
            continue

        values_str = line.split("(", 1)[1].rsplit(")", 1)[0]

        try:
            # csv.reader does the heavy lifting of parsing, including quoted strings with commas
            reader = csv.reader(io.StringIO(values_str), delimiter=',', quotechar="'", doublequote=True)
            row = next(reader)
        except csv.Error as e:
            print(f"CSV parse error (skipping row): {e}")
            continue

        # === This next grabs any and all cover images.  In my case, they were all JPEGs
        img_blobs = []
        for idx in range(65,69):
            if idx < len(row):
                blob = parse_value(row[idx])
                if isinstance(blob, str) and len(blob) > 1000:
                    img_blobs.append(blob)
        best_blob = max(img_blobs, key=len) if img_blobs else None

        # Take first 98 columns from backup + BEST_COVER = 99 total
        base_values = [parse_value(v) for v in row[:98]]
        final_values = base_values + [best_blob]

        assert len(final_values) == EXPECTED_COLUMNS, f"Expected {EXPECTED_COLUMNS}, got {len(final_values)}"

        try:
            cur.execute("INSERT INTO READERWARE VALUES ({})"
                        .format(",".join("?" for _ in final_values)), final_values)
            if cur.rowcount:
                total += 1
                if best_blob:
                    images += 1
            else:
                dups += 1
        except Exception as e:
            print(f"DB error: {e}")
            continue

        if total % 500 == 0:
            conn.commit()
            print(f"   {total} books – {images} covers – {dups} duplicates skipped")

conn.commit()
cur.execute("VACUUM")
conn.close()

print(f"\nDONE! {total} books imported, {images} perfect covers stored in BEST_COVER")
print(f"Database: {DB_FILE}")
print(f"Duplicates skipped: {dups}")