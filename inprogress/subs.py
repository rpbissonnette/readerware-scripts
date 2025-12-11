import binascii, csv, io, re, sqlite3, sys
from io import BytesIO, StringIO
from PIL import Image

# Default width x height sizes for cover images
SMALL_COVER = (150, 150)
LARGE_COVER = (300, 300)
    # Increase CSV field size limit for large text fields - big cover images!
csv.field_size_limit(10 * 1024 * 1024)  # 10MB limit

# Some symbolic names that will be used to access the create table artifact
# names, numbers correspond to this line:
#     return sqlite_schema,  transforms, all_columns, coversIndex
createSQL = 0           # schema[][0] = STRING of CREATE commands for this schema
TRANSFORM_LIST = 1      # schema[][1] = LIST of transforms for each column
COLUMNS_LIST = 2        # schema[][2] = LIST of Column NAMES
SPECIAL_HANDLING = 3    # column renaming
COVER_IMAGE_COLUMN_INDEX = 0 # saves Where we discovered the IMAGE1 column
HASH_INDEX = 1
PROVENANCE_INDEX = 2


# map schema types      # have already dropped any "CACHED" appearances
# be aware that often a BIGINT will be a foreign key to an other table.
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


def parse_blob(value):          # this has been specialized since we know only cover images apply
    """Convert hex string to binary blob"""
    if not value or value == 'NULL':
        return None
    try:
        return binascii.unhexlify(value)
    except (ValueError, binascii.Error):
        return None


def process_image(binary_data, target_size=SMALL_COVER):
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
