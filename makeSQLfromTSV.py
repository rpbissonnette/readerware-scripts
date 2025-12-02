import csv
import os
import sqlite3
from pathlib import Path
from typing import List, Tuple, Optional
import logging
import hashlib

"""
Improved version of makeDB.py with better binary blob handling and structure.

The big idea is to use the headers in the TSV file to create the database table dynamically,
and to handle images as binary blobs properly.  Depending on what choices you made with
the Readerware export, the resulting SQL table will have different columns.
-- SQL Schema Example --

CREATE TABLE books (
    book_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    author TEXT,
    isbn TEXT,
    publisher TEXT,
    publication_date TEXT,
    pages INTEGER,
    description TEXT,  -- Readerware has a PRODUCT_INFO field, user COMMENTS to play with.
    keywords TEXT,

    -- next entries are created dynamically by the script below.
    
    location TEXT DEFAULT 'Where the book is, or who the ower is, etc.', -- from config, overriding Readerware's idea.
    cover_image BLOB,  -- in whatever format you requested on Image export
    content_hash TEXT UNIQUE,  -- For duplicate detection
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

"""

# Configuration  If you only have one TSV to process, set it here.
CONFIG = {
    'db_name': 'books.db',
    'table_name': 'books',
    'location' : 'nowhere',
    'tsv_file': 'MyBooks/allbooks.tab',
    'images_dir': 'MyBooks/allbooks',
    'image_extension': '.GIF',  # Adjust based on your exported image format
    'image_key_column': 'ROWKEY',  # Column index 5 is the primary key for book entries
    'output_dir': os.path.expanduser('~/whereever/you/want/the/db/'),
}

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BookDatabaseBuilder:
    """Handles the conversion of TSV + images to SQLite database."""
    
    def __init__(self, config: dict):
        self.config = config
        self.db_path = Path(config['output_dir']) / config['db_name']
        self.images_dir = Path(config['images_dir'])
        
    def connect_db(self) -> sqlite3.Connection:
        """Create database connection with proper settings."""
        # Ensure output directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        # Enable foreign keys and other optimizations
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        
        return conn
    
    def create_table_with_blob(self, conn: sqlite3.Connection, header: List[str]) -> None:
        """Create table with proper column types including BLOB for images."""
        cursor = conn.cursor()

        # Check if the table already exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.config['table_name']}';")
        table_exists = cursor.fetchone() is not None
        
        if table_exists:
            logger.info(f"Table '{self.config['table_name']}' already exists, skipping creation")
            return
        
        # Define column types - you can customize this mapping
        column_definitions = []
        for col in header:
            col_upper = col.upper()
            if col_upper in ['PAGES', 'READ_COUNT']:
                column_definitions.append(f'"{col}" INTEGER')
            elif col_upper in ['DATE_LAST_READ']:
                column_definitions.append(f'"{col}" DATE')
            else:
                column_definitions.append(f'"{col}" TEXT')
        
        # Add hash and cover image columns
        column_definitions.append("content_hash TEXT UNIQUE")
        column_definitions.append("Cover_gif BLOB")

        columns_sql = ", ".join(column_definitions)
        create_table_sql = f'CREATE TABLE IF NOT EXISTS {self.config["table_name"]} ({columns_sql})'
        
        logger.info(f"Creating table with SQL: {create_table_sql}")
        cursor.execute(create_table_sql)
        conn.commit()
    
    def load_image_data(self, image_key: str) -> Optional[bytes]:
        """Load image data."""
        if not image_key:
            return None
            
        image_path = self.images_dir / f"{image_key}{self.config['image_extension']}"
        
        try:
            with open(image_path, 'rb') as file:
                image_data = file.read()
                            
            logger.debug(f"Loaded image {image_path} ({len(image_data)} bytes)")
            return image_data
            
        except FileNotFoundError:
            logger.debug(f"Image not found: {image_path}")
            return None
        except Exception as e:
            logger.warning(f"Error loading image {image_path}: {e}")
            return None
    
    def read_tsv_with_images(self, filepath: str) -> Tuple[List[str], List[List]]:
        """Read TSV file and append image data to each row."""
        data = []
        image_key_index = None
        location_index = None
        
        with open(filepath, 'r', newline='', encoding='utf-8-sig') as tsvfile:
            reader = csv.reader(tsvfile, delimiter='\t')
            header = next(reader)
            
            # Find the column that contains the image key
            try:
                image_key_index = header.index(self.config['image_key_column'])
            except ValueError:
                logger.warning(f"Image key column '{self.config['image_key_column']}' not found in header")
                # Fallback to index 5 as in your original code
                image_key_index = 5 if len(header) > 5 else None
            
            # Find the location column
            try:
                location_index = header.index('LOCATION')
            except ValueError:
                try:
                    location_index = header.index('location')
                except ValueError:
                    logger.warning("LOCATION column not found in header")
                    location_index = None
            
            # Remove ROWKEY from header since we don't want it in the final DB
            filtered_header = [col for col in header if col != self.config['image_key_column']]
            
            logger.info(f"Processing TSV with {len(header)} columns, image key at index {image_key_index}, location at index {location_index}")
            logger.info(f"Final header (without ROWKEY): {filtered_header}")
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 because header is row 1
                try:
                    # Get image key from the appropriate column
                    image_key = row[image_key_index] if image_key_index is not None and len(row) > image_key_index else None
                    logger.debug(f"Processing row {row_num}, image key: {image_key}")
                    
                    # Create hash of the original row (before modifying location)
                    # This catches truly identical records, but allows location differences
                    original_row_str = '\t'.join(str(cell) for cell in row if row.index(cell) != image_key_index)
                    content_hash = hashlib.sha256(original_row_str.encode('utf-8')).hexdigest()[:16]  # First 16 chars for readability
                    
                    # Overwrite the location field with our config location
                    if location_index is not None and len(row) > location_index:
                        row[location_index] = self.config['location']
                    
                    # Remove ROWKEY from row data
                    filtered_row = [row[i] for i in range(len(row)) if i != image_key_index]
                    
                    # Load image data
                    image_data = self.load_image_data(image_key)
                    
                    # Append hash and image data to row
                    extended_row = filtered_row + [content_hash, image_data]
                    data.append(extended_row)
                    
                    if row_num % 100 == 0:
                        logger.info(f"Processed {row_num - 1} rows...")
                        
                except Exception as e:
                    logger.error(f"Error processing row {row_num}: {e}")
                    # Add empty hash and image data for failed rows
                    filtered_row = [row[i] for i in range(len(row)) if i != image_key_index] if image_key_index is not None else row
                    # Create a fallback hash for error cases
                    error_hash = hashlib.sha256(f"error_row_{row_num}".encode('utf-8')).hexdigest()[:16]
                    extended_row = filtered_row + [error_hash, None]
                    data.append(extended_row)
        
        logger.info(f"Loaded {len(data)} rows from TSV")
        return filtered_header, data
    
    def insert_data_batch(self, conn: sqlite3.Connection, header: List[str], data: List[List]) -> None:
        """Insert data in batches for better performance."""
        cursor = conn.cursor()
        
        # Create placeholders for all columns (header + content_hash + cover_image)
        num_columns = len(header) + 2  # +1 for content_hash, +1 for cover_image
        placeholders = ", ".join(["?" for _ in range(num_columns)])
        insert_sql = f'INSERT OR IGNORE INTO {self.config["table_name"]} VALUES ({placeholders})'
        
        logger.info(f"Inserting {len(data)} rows with {num_columns} columns...")
        
        # Insert in batches for better performance
        batch_size = 100
        inserted_count = 0
        duplicate_count = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            initial_count = cursor.execute(f"SELECT COUNT(*) FROM {self.config['table_name']}").fetchone()[0]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            final_count = cursor.execute(f"SELECT COUNT(*) FROM {self.config['table_name']}").fetchone()[0]
            
            batch_inserted = final_count - initial_count
            batch_duplicates = len(batch) - batch_inserted
            inserted_count += batch_inserted
            duplicate_count += batch_duplicates
            
            logger.info(f"Batch {i//batch_size + 1}/{(len(data) + batch_size - 1)//batch_size}: {batch_inserted} inserted, {batch_duplicates} duplicates skipped")
        
        logger.info(f"Data insertion completed: {inserted_count} new records, {duplicate_count} duplicates skipped")
    
    def verify_data(self, conn: sqlite3.Connection) -> None:
        """Verify the inserted data."""
        cursor = conn.cursor()
        
        # Get basic statistics
        cursor.execute(f"SELECT COUNT(*) FROM {self.config['table_name']}")
        total_books = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM {self.config['table_name']} WHERE Cover_gif IS NOT NULL")
        books_with_images = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT AVG(LENGTH(Cover_gif)) FROM {self.config['table_name']} WHERE Cover_gif IS NOT NULL")
        avg_image_size = cursor.fetchone()[0]
        
        # Get count by location
        cursor.execute(f"SELECT location, COUNT(*) FROM {self.config['table_name']} GROUP BY location")
        location_counts = cursor.fetchall()
        
        logger.info(f"Database verification:")
        logger.info(f"  Total books: {total_books}")
        logger.info(f"  Books with cover images: {books_with_images}")
        logger.info(f"  Average image size: {avg_image_size:.0f} bytes" if avg_image_size else "  Average image size: N/A")
        logger.info(f"  Books by location: {dict(location_counts)}")
        
        # Show a sample of the data (without binary data)
        cursor.execute(f"SELECT * FROM {self.config['table_name']} LIMIT 3")
        logger.info("Sample data (first 3 rows, excluding binary data):")
        for i, row in enumerate(cursor.fetchall(), 1):
            # Show all columns except the binary image data (last column)
            display_row = list(row[:-1]) + [f"<{len(row[-1])} bytes>" if row[-1] else "None"]
            logger.info(f"  Row {i}: {display_row}")
    
    def build_database(self, append_mode: bool = False) -> None:
        """Main method to build the database."""
        logger.info(f"Starting database build process (append_mode={append_mode})...")
        
        # Only remove existing database if not in append mode
        if not append_mode and self.db_path.exists():
            self.db_path.unlink()
            logger.info(f"Removed existing database: {self.db_path}")
        
        try:
            # Read TSV with images
            header, data = self.read_tsv_with_images(self.config['tsv_file'])
            
            # Connect to database
            conn = self.connect_db()
            
            # Create table (will only create if it doesn't exist due to IF NOT EXISTS)
            self.create_table_with_blob(conn, header)
            
            # Insert data
            self.insert_data_batch(conn, header, data)
            
            # Verify data
            self.verify_data(conn)
            
            # Close connection
            conn.close()
            
            logger.info(f"Database build completed successfully: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Database build failed: {e}")
            raise


def main():
    """Main function."""
    builder = BookDatabaseBuilder(CONFIG)
    builder.build_database()


def merge_multiple_tsvs():
    """Merge multiple TSV files into a single database."""
    
    # Define your TSV files and their locations
    tsv_configs = [
        {
            'location': 'bookshelf1',
            'tsv_file': 'bookshelf1.tab',
            'images_dir': './images/bookshelf1'
        },
        {
            'location': 'borrowed',
            'tsv_file': 'borrowed.tab',
            'images_dir': './images/borrowed'
        },
        {
            'location': 'wishlist',
            'tsv_file': 'wishlist.tab',
            'images_dir': './images/wishlist'
        }
    ]
    
    for i, tsv_config in enumerate(tsv_configs):
        # Update the base config with current TSV settings
        current_config = CONFIG.copy()
        current_config.update(tsv_config)
        
        logger.info(f"Processing {tsv_config['location']} ({i+1}/{len(tsv_configs)})")
        
        builder = BookDatabaseBuilder(current_config)
        # First run creates the database, subsequent runs append
        append_mode = i > 0
        builder.build_database(append_mode=append_mode)
    
    logger.info("All TSV files merged successfully!")


if __name__ == "__main__":
    # Uncomment the line you want to use:
    #main()  # Single TSV file
    merge_multiple_tsvs()  # Multiple TSV files