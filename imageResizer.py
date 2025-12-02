'''
example code for resizing - cover images in this case.

The  number part (image_key) of the image file name refers to primary rowkey 
of the corresponding book from the Readerware database.

'''
from typing import Optional
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from io import BytesIO

logger = logging.getLogger(__name__)

# --- Abstract Base Class ---

class ImageResizer(ABC):
    """Abstract Base Class for image resizing."""

    def __init__(self, images_dir: Path):
        """
        Initializes the resizer with the directory where images are stored.
        """
        if not isinstance(images_dir, Path):
            raise TypeError("images_dir must be a Path object.")
        self.images_dir = images_dir
        logger.info(f"Image directory set to: {self.images_dir}")

    @abstractmethod
    def resize_cover(self, image_key: str, max_size: int = 512) -> Optional[bytes]:
        """
        Abstract method to load an image, resize it, and return it as JPEG bytes.
        Must be implemented by subclasses.
        image_key refers to primary rowkey of the corresponding book from the Readerware database.
        max_size = longest edge in pixels
        """
        pass

# --- Concrete Implementations ---

class QtImageResizer(ImageResizer):
    """Image resizer using PySide6 (Qt)."""

    def __init__(self, images_dir: Path):
        super().__init__(images_dir)
        try:
            from PySide6.QtGui import QImage
            from PySide6.QtCore import Qt, QBuffer, QByteArray, QIODevice
            self.QImage = QImage
            self.Qt = Qt
            self.QBuffer = QBuffer
            self.QByteArray = QByteArray
            self.QIODevice = QIODevice
            logger.info("Successfully imported PySide6 modules for QtImageResizer.")
        except ImportError as e:
            logger.error("Failed to import PySide6 modules for QtImageResizer.")
            raise e

    def resize_cover(self, image_key: str, max_size: int = 512) -> Optional[bytes]:
        if not image_key:
            return None

        path_found = None
        for ext in ('', '.jpg', '.jpeg', '.JPG', '.JPEG', '.gif', '.GIF'):
            path = self.images_dir / f"{image_key}{ext}"
            if path.exists():
                path_found = path
                break

        if not path_found:
            logger.warning(f"Image not found for key: {image_key}")
            return None

        image = self.QImage(str(path_found))
        if image.isNull():
            logger.warning(f"Failed to load image {path_found}")
            return None
        print(f"Original image size: {image.size()} {image.width()}x{image.height()}")

        # Scale down while preserving aspect ratio
        if image.width() > image.height():
            scaled = image.scaledToWidth(max_size, self.Qt.TransformationMode.SmoothTransformation)
        else:
            scaled = image.scaledToHeight(max_size, self.Qt.TransformationMode.SmoothTransformation)
        print(f"Scaled image size: {scaled.size()}  {scaled.width()}x{scaled.height()}")
        # Convert to JPEG bytes
        byte_array = self.QByteArray()
        buffer = self.QBuffer(byte_array)
        buffer.open(self.QIODevice.OpenModeFlag.WriteOnly)
        scaled.save(buffer, "JPG", 85)  # 85 = excellent quality/size trade-off
        buffer.close()

        data = byte_array.data()
        logger.debug(f"Resized {path_found.name}: {image.size()} → {scaled.size()} ({len(data) / 1024:.1f} KB\)")
        return data


class PilImageResizer(ImageResizer):
    """Image resizer using Pillow (PIL)."""

    def __init__(self, images_dir: Path):
        super().__init__(images_dir)
        try:
            from PIL import Image, ImageOps
            self.Image = Image
            self.ImageOps = ImageOps
            logger.info("Successfully imported Pillow modules for PilImageResizer.")
        except ImportError as e:
            logger.error("Failed to import Pillow modules for PilImageResizer.")
            raise e

    def resize_cover(self, image_key: str, max_size: int = 512) -> Optional[bytes]:
        if not image_key:
            return None

        path_found = None
        for ext in ('', '.jpg', '.jpeg', '.JPG', '.JPEG', '.gif', '.GIF'):
            path = self.images_dir / f"{image_key}{ext}"
            if path.exists():
                path_found = path
                break

        if not path_found:
            logger.warning(f"Image not found for key: {image_key}")
            return None

        try:
            with self.Image.open(path_found) as img:
                # Auto-rotate based on EXIF
                img = self.ImageOps.exif_transpose(img)

                # Convert to RGB if it's not, e.g., for GIF files
                if img.mode not in ('RGB', 'L'): # L for grayscale
                    img = img.convert('RGB')

                print(f"Original image dimensions: {img.width}x{img.height}")
                # Scale down while preserving aspect ratio
                img.thumbnail((max_size, max_size), self.Image.Resampling.LANCZOS)

                # Save to bytes
                output = BytesIO()
                img.save(output, format='JPEG', quality=85)
                data = output.getvalue()
                logger.info(f"Resized {path_found.name}: {img.width}x{img.height}  ({len(data) / 1024:.1f} KB ) ")
                return data
        except Exception as e:
            logger.warning(f"Failed to process image {path_found}: {e}")
            return None

# --- Factory Function ---

def get_image_resizer(images_dir: Path) -> Optional[ImageResizer]:
    """
    Factory function to return the best available ImageResizer instance.
    It prefers PySide6 (Qt) over Pillow (PIL).
    """
    try:
        resizer = QtImageResizer(images_dir)
        logger.info("Using PySide6 (Qt) for image resizing.")
        return resizer
    except ImportError:
        logger.info("PySide6 not found, trying Pillow.")
        try:
            resizer = PilImageResizer(images_dir)
            logger.info("Using Pillow (PIL) for image resizing.")
            return resizer
        except ImportError:
            logger.warning("No image resizing library available.")
            logger.warning("Please install PySide6 or Pillow for this functionality.")
            logger.warning("Or roll your own!")
            return None

# --- Main script example ---

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Example Usage
    # NOTE: Update this path to an actual image file on your system.
    image_is_here = None
    try:
        image_path = Path(image_is_here)
        if not image_path.exists():
             raise FileNotFoundError(f"Example image not found at {image_path}. Please update the path in the script.")
        images_dir = image_path.parent
        image_key = image_path.stem

        # Get the best available resizer
        resizer = get_image_resizer(images_dir)

        if resizer:
            # Resize the image by providing its key
            new_blob = resizer.resize_cover(image_key)

            if new_blob:
                print(f"Successfully resized image. New size: {len(new_blob) / 1024:.1f} KB")
                print("new_Blob resized image is about to fall on the floor if you don't interceed quickly!")
            else:
                print("Failed to resize image.")
        else:
            print("No image resizer is available on this system.")

    except (FileNotFoundError, TypeError) as e:
        logger.error(f"Setup error: {e}")
        print(f"Error: {e}")