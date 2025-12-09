import binascii, csv, io, re, sqlite3
from io import BytesIO, StringIO
from PIL import Image

# Global defines, change for your needs
# Readerware Save file
originalBackupScript = "Readerware.rw3.bkup.script"
# Transliterated sqlite3 output file
output_db = "output.db"

# map schema types
def map_to_sqlite_type(readerware_type):
    """Map Readerware/HSQLDB types to SQLite types"""
    type_upper = readerware_type.upper()
    
    if 'VARCHAR' in type_upper or 'CHAR' in type_upper:
        return 'TEXT'
    elif 'BIGINT' in type_upper:
        return 'INTEGER'
    elif 'INTEGER' in type_upper:
        return 'INTEGER'
    elif 'DECIMAL' in type_upper:
        return 'REAL'
    elif 'DATE' in type_upper:
        return 'TEXT'  # Readerware already a proper ISO date string
    elif 'BOOLEAN' in type_upper:
        return 'INTEGER'  # 0 or 1
    elif 'VARBINARY' in type_upper:
        return 'BLOB'
    else:
        return 'TEXT'


def get_transform_function(col_type, is_not_null):
    """Get the appropriate transformation function for a column type"""
    type_upper = col_type.upper()
    
    if 'VARCHAR' in type_upper or 'CHAR' in type_upper:
        return clean_string
    elif 'BIGINT' in type_upper or 'INTEGER' in type_upper:
        return parse_int if not is_not_null else parse_int_not_null
    elif 'DECIMAL' in type_upper:
        return parse_float
    elif 'DATE' in type_upper:
        return parse_date
    elif 'BOOLEAN' in type_upper:
        return parse_boolean
    elif 'VARBINARY' in type_upper:
        return parse_blob
    else:
        return None


# Transformation functions
def clean_string(value):
    """Clean string values, unescape quotes"""
    if not value or value == 'NULL':
        return None
    return value.replace("''", "'").strip()


def parse_int(value):
    """Convert to integer, handling -1 as NULL"""
    if not value or value == 'NULL':
        return None
    try:
        val = int(value)
        return None if val == -1 else val
    except (ValueError, TypeError):
        return None


def parse_int_not_null(value):
    """Convert to integer, keep -1 values"""
    if not value or value == 'NULL':
        return 0
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def parse_float(value):
    """Convert to float"""
    if not value or value == 'NULL':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_date(value):
    """Readerware already in ISO format"""
    if not value or value == 'NULL':
        return None
    return value.strip() if value else None


def parse_boolean(value):
    """Convert TRUE/FALSE strings to 0/1"""
    if value == 'TRUE':
        return 1
    elif value == 'FALSE':
        return 0
    return None


def parse_blob(value):
    """Convert hex string to binary blob"""
    if not value or value == 'NULL':
        return None
    try:
        return binascii.unhexlify(value)
    except (ValueError, binascii.Error):
        return None


def process_image(binary_data, target_size=(150, 150)):
    """
    Convert hex string to resized JPEG blob.
    Returns None if processing fails.
    """
    if not binary_data or binary_data == 'NULL':
        return 'NULL'
    
    try:
        img = Image.open(io.BytesIO(binary_data))
        # print(f"Original image dimensions: {img.width}x{img.height}")
        # Scale down while preserving aspect ratio
        if img.format != 'JPEG':    # somehow there are 2 .gif files.  Sigh.
            img = img.convert("RGB") 
        img.thumbnail(target_size, Image.Resampling.LANCZOS)
        # print(f"Resized image dimensions: {img.width}x{img.height}")
        # Save to bytes
        output = BytesIO()
        img.save(output, format='JPEG', quality=85)
        data = output.getvalue()
        return data
    except Exception as e:
        print(e)


def clean_product_info(text):
    """Clean up product descriptions"""
    if not text or text == 'NULL':
        return None
    
    text = text.replace("''", "'")  # Unescape quotes
    text = text.replace('\\u000a', '\n')  # Fix newlines
    
    # Remove "Book Description" prefix
    if text.startswith('Book Description\n'):
        text = text[17:]  # len('Book Description\n')
    
    return text.strip()

def parse_create_table(table: str) -> None: 
    """
    Parse CREATE TABLE statement and generate column transformations.
    Args:
    Returns: (sqlite_schema, column_transforms, column_names, column_indices, insert_column_names)
    """
    if table == "READERWARE":
        print("break")
    descriptionIndex = None
    coversIndex = None

    create_sql = originalRWschema[table]
    # Extract column definitions
    match = re.search(r'CREATE.*?TABLE\s+(\w+)\s*\((.*)\)', create_sql, re.DOTALL | re.IGNORECASE)
    if not match:
        raise ValueError("Could not parse CREATE TABLE statement")
    
    table_name = match.group(1)
    columns_text = match.group(2)
    
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

    if table == 'READERWARE':
        descriptionIndex = sqlite_columns.index("PRODUCT_INFO TEXT")
        coversIndex = sqlite_columns.index("IMAGE1_DATA BLOB")
    sqlite_schema = f"CREATE TABLE {table} ({', '.join(sqlite_columns)})"
    
    # return sqlite_schema, transforms, all_columns, column_indices, insert_columns

    return sqlite_schema,  transforms, all_columns, descriptionIndex, coversIndex

def processReaderwareHSQLDBschemas() -> None:
    """Read the backup script file and extract CREATE TABLE statements"""
    content = createLines.read()
        
    # Find all CREATE TABLE statements by matching balanced parentheses
    pos = 0
    while True:
        # Find next CREATE TABLE
        match = re.search(r'CREATE CACHED TABLE\s+(\w+)\s*\(', content[pos:], re.IGNORECASE)
        if not match:
            break
            
        table_name = match.group(1)
        start = pos + match.start()
        paren_start = pos + match.end() - 1  # Position of opening (
        
        # Find matching closing parenthesis
        paren_count = 1
        i = paren_start + 1
        while i < len(content) and paren_count > 0:
            if content[i] == '(':
                paren_count += 1
            elif content[i] == ')':
                paren_count -= 1
            i += 1
        
        if paren_count == 0:
            full_statement = content[start:i]
            originalRWschema[table_name] = full_statement
        
        pos = i
    
    return

def diag():
    sql = """
        SELECT
            r.ROWKEY,
            r.TITLE AS "Title",
            r.SUBTITLE AS "Subtitle",
            r.PAGES AS "Pages",
            a.NAME AS "Author",
            a1.NAME AS "Author1",
            a2.NAME AS "Author2",
            a3.NAME AS "Author3",			
            pp.LISTITEM as PublicationPlace,
            lan.LISTITEM as Lanuage,
            k1.LISTITEM as CATEGORY1,
            k2.LISTITEM as CATEGORY2,
            k3.LISTITEM as CATEGORY3,
            f.LISTITEM as FORMAT,
            p.LISTITEM as publisher,
            l.LISTITEM as ReadingLevel,
            c1.NAME AS Primary_Author,
            c2.NAME AS Co_Author,
            c3.NAME AS "3rd Author",
            c4.NAME AS "4th Author",
            r.ISBN,
            COALESCE(r.IMAGE1_LARGE_DATA,
                r.IMAGE2_LARGE_DATA,
                r.IMAGE1_DATA,
                r.IMAGE2_DATA)      AS cover_blob
            FROM READERWARE r
            LEFT JOIN CONTRIBUTOR a on a.ROWKEY == r.AUTHOR
            LEFT JOIN CONTRIBUTOR a1 on a1.ROWKEY == r.AUTHOR2
            LEFT JOIN CONTRIBUTOR a2 on a1.ROWKEY == r.AUTHOR3
            LEFT JOIN CONTRIBUTOR a3 on a1.ROWKEY == r.AUTHOR4
            LEFT JOIN PUBLICATION_PLACE_LIST pp on r.PUB_PLACE = pp.ROWKEY
            LEFT JOIN LANGUAGE_LIST lan on r.CONTENT_LANGUAGE = lan.ROWKEY
            LEFT JOIN CATEGORY_LIST k1 on r.CATEGORY1 = k1.ROWKEY
            LEFT JOIN CATEGORY_LIST k2 on r.CATEGORY2 = k2.ROWKEY
            LEFT JOIN CATEGORY_LIST k3 on r.CATEGORY3 = k3.ROWKEY
            LEFT JOIN FORMAT_LIST f ON r.FORMAT = f.ROWKEY
            LEFT JOIN PUBLISHER_LIST p ON r.PUBLISHER = p.ROWKEY
            LEFT JOIN READING_LEVEL_LIST l on r.READING_LEVEL = l.ROWKEY
            LEFT JOIN CONTRIBUTOR c1 ON r.AUTHOR  = c1.ROWKEY
            LEFT JOIN CONTRIBUTOR c2 ON r.AUTHOR2 = c2.ROWKEY
            LEFT JOIN CONTRIBUTOR c3 ON r.AUTHOR3 = c3.ROWKEY
            LEFT JOIN CONTRIBUTOR c4 ON r.AUTHOR4 = c4.ROWKEY;
        """
    try:
        cursor.execute(sql)
        all = cursor.fetchall()
        # if len(all) != 0:
        #     print(f"  {all[:4]}")
    except Exception as e:
        print(e)

def convert_readerware_to_sqlite() -> None:
    """
    Convert Readerware backup to SQLite database.
    Args:
        conn: SQLite connection
        cursor: SQLite cursor
        bkupscript_file: Path to Readerware backup script
    """
    # Increase CSV field size limit for large text fields
    csv.field_size_limit(10 * 1024 * 1024)  # 10MB limit
    
    # Process INSERT statements
    insert_count = 0
    error_count = 0
    # carve out the the table name and then the values
    insert_pattern = re.compile(r'INSERT INTO\s+(\w+)\s+VALUES\((.*)\)', re.DOTALL | re.IGNORECASE)
    for line in sortedInserts:
        match = insert_pattern.match(line)
        if  match:
            table = match.group(1)
            values_str = match.group(2)

            try:
                transforms = schemas[table][1]

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
                    description_index = schemas[table][3]
                    cover_index = schemas[table][4]
                    if (readyRow[description_index]):
                        readyRow[description_index] = clean_product_info(readyRow[description_index])
                    try:
                        # searching for the best small and large cover image.
                        max = 0
                        maxset = False
                        min = 100000
                        minset = False
                        for i in range(cover_index,cover_index+4):
                            if readyRow[i]:
                                l = len(readyRow[i])
                                if (l > 4):
                                    if  (l > max):  
                                        imax = i
                                        max = l
                                        maxImage = readyRow[i]
                                        maxset = True
                                    if (l < min) : 
                                        imin = i
                                        min = l
                                        minImage = readyRow[i]
                                        minset = True
                        # Now resize them once and for all.
                        if minset: 
                            readyRow[cover_index] = process_image(minImage)
                        else:
                            readyRow[cover_index] = None
                        if maxset:
                            readyRow[cover_index+1] = process_image(maxImage,(300,300))
                        else:
                            readyRow[cover_index+1] = None
                        readyRow[cover_index+2] = None
                        readyRow[cover_index+3] = None
                    except Exception as e:
                        print(e)

                # Insert into SQLite (explicitly list columns to exclude IDENTITY columns)
                placeholders = ','.join(['?'] * len(readyRow))
                column_list =  ','.join(schemas[table][2])
                cursor.execute(f'INSERT INTO {table} ({column_list}) VALUES ({placeholders})', readyRow)
                insert_count += 1
                rc = cursor.rowcount
                if insert_count % 100 == 0:
                    conn.commit()  # Commit in batches for better performance
                    print(f"Processed {insert_count} rows...")
                    diag()
                    
            except Exception as e:
                error_count += 1
                print(f"{table}, {e}")


def main():
    # resizer = get_image_resizer
    # Fill originalRWschema, a global dictionary with the original RW CREATE TABLE commands.  
    processReaderwareHSQLDBschemas()  # output in originalRWschema


    # next convert to sqlite and then create the simple tables.
    for table in originalRWschema:
            try:
                schemas[table] = parse_create_table(table)
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                cursor.execute(schemas[table][0])
            except Exception as e:
                print(e)
    conn.commit()

    # 2nd pass to insert the actual data
    convert_readerware_to_sqlite()

   
# globals and preliminary input sort since i am lazy
originalRWschema = {}   # The Readerware v 3.4 hsqldb schema
schemas = {}            # The equivalent sqlite3 schema and transforms

# python caches for the original backup script lines
createLines = io.StringIO()             # really anyting Not an Insert Values
readerwareLines = io.StringIO()         # the main table, we want to load it last
otherInsertLines = io.StringIO()        # data for the remaining tables
sortedInserts = io.StringIO()  

# split the original backup into the order to one-shot process
with open(originalBackupScript, "r") as fin:
    for line in fin.readlines():
        if line.startswith("INSERT INTO"):  
            if line.startswith("INSERT INTO READERWARE VALUES"):
                readerwareLines.write(line)
            else:
                otherInsertLines.write(line)
        else:
            createLines.write(line)
    createLines.seek(0)
    otherInsertLines.seek(0)
    readerwareLines.seek(0)
    sortedInserts = io.StringIO(otherInsertLines.getvalue() + readerwareLines.getvalue())

print("Input has been split, ready for the main passes")

# Create SQLite database
conn = sqlite3.connect(output_db)
cursor = conn.cursor()

main()

conn.commit()
conn.close()
print("Conversion complete!")

print("pau hana")

