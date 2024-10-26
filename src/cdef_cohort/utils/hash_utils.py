import glob
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar, cast

from imohash import hashfile

from cdef_cohort.logging_config import logger
from cdef_cohort.utils import config
from cdef_cohort.utils.config import HASH_FILE_PATH

T = TypeVar("T")


def calculate_file_hash(file_path: Path) -> str:
    """
    Calculate the hash of a file using imohash.

    Args:
        file_path (Path): The path to the file to be hashed.

    Returns:
        str: The calculated hash value of the file.
    """
    logger.debug(f"Calculating hash for file: {file_path}")
    hash_value = str(hashfile(str(file_path), hexdigest=True))
    logger.debug(f"Hash calculated: {hash_value}")
    return hash_value


def get_file_hashes(file_paths: list[Path]) -> dict[str, str]:
    """
    Calculate hashes for a list of files.

    Args:
        file_paths (list[Path]): A list of file paths to calculate hashes for.

    Returns:
        dict[str, str]: A dictionary mapping file paths to their hash values.
    """
    logger.debug(f"Calculating hashes for {len(file_paths)} files")
    hashes = {str(file_path): calculate_file_hash(file_path) for file_path in file_paths}
    # logger.debug(f"Hashes calculated: {hashes}")
    logger.debug("Hashes calculated.")
    return hashes


def load_hash_file() -> dict[str, dict[str, str]]:
    """
    Load the hash file if it exists, otherwise return an empty dict.

    Returns:
        dict[str, dict[str, str]]:
            The loaded hash data or an empty dictionary if the file doesn't exist.
    """
    logger.debug(f"Attempting to load hash file from: {HASH_FILE_PATH}")
    if HASH_FILE_PATH.exists():
        with HASH_FILE_PATH.open("r") as f:
            hash_data = cast(dict[str, dict[str, str]], json.load(f))
        logger.debug("Hash file loaded successfully.")
        return hash_data
    logger.debug("Hash file does not exist. Returning empty dict.")
    return {}


def save_hash_file(hash_data: dict[str, dict[str, str]]) -> None:
    """
    Save the hash data to the hash file.

    Args:
        hash_data (dict[str, dict[str, str]]): The hash data to be saved.
    """
    logger.debug(f"Saving hash data to file: {HASH_FILE_PATH}")
    with HASH_FILE_PATH.open("w") as f:
        json.dump(hash_data, f, indent=2)
    logger.debug("Hash data saved successfully")


def process_with_hash_check(process_func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    """
    Wrapper function to handle hash checking and processing.

    This function checks if the input files have changed since the last processing.
    If they haven't, it skips processing.
    Otherwise, it processes the data and updates the hash information.

    Args:
        process_func (Callable[..., Any]):
            The function to call for processing (e.g., process_lpr_adm).
        *args: Additional positional arguments to pass to process_func.
        **kwargs: Additional keyword arguments to pass to process_func.

    Raises:
        ValueError: If input or output file information cannot be found
        for the given process function.
    """
    logger.debug(f"Starting process_with_hash_check for function: {process_func.__name__}")

    # Generate the expected variable names
    register_name = "_".join(process_func.__name__.upper().split("_")[1:])
    input_files_var = f"{register_name}_FILES"
    output_file_var = f"{register_name}_OUT"

    logger.debug(
        f"Generated variable names: "
        f"input_files_var={input_files_var}, "
        f"output_file_var={output_file_var}"
    )

    # Get the input and output file information from the config module
    input_files = getattr(config, input_files_var, None)
    output_file = getattr(config, output_file_var, None)

    logger.debug(f"Retrieved from config: input_files={input_files}, output_file={output_file}")

    if not input_files or not output_file:
        logger.error(f"Could not find input or output file information for {process_func.__name__}")
        raise ValueError(
            f"Could not find input or output file information for {process_func.__name__}"
        )

    # Get list of input files
    file_pattern = str(input_files)
    if not file_pattern.endswith("*.parquet"):
        file_pattern = str(input_files / "*.parquet")
    files = glob.glob(file_pattern)

    logger.debug(f"Found {len(files)} input files matching pattern: {file_pattern}")

    # Calculate hashes for input files
    current_hashes = get_file_hashes([Path(f) for f in files])

    # Load existing hashes
    all_hashes = load_hash_file()

    # Check if processing is needed
    register_name = process_func.__name__
    if register_name in all_hashes and all_hashes[register_name] == current_hashes:
        logger.info(f"Input files for {register_name} haven't changed. Skipping processing.")
        return

    # Process the data
    logger.info(f"Input files have not been processed before {register_name} data...")
    logger.debug(f"Calling process function: {process_func.__name__}")
    process_func(*args, **kwargs)

    # Update and save hashes
    all_hashes[register_name] = current_hashes
    save_hash_file(all_hashes)

    logger.info(f"Processed {register_name} data and saved to {output_file}")
    logger.info(f"Updated hash information in {HASH_FILE_PATH}")
    logger.debug("Finished process_with_hash_check")


# You might want to add a function to test the hash utilities
def test_hash_utils() -> None:
    """
    Test the hash utility functions.

    This function creates a test file, calculates its hash, tests the get_file_hashes function,
    and tests saving and loading the hash file. It then cleans up the test files.
    """
    logger.debug("Starting hash utilities test")

    # Create a test file
    test_file = Path("test_file.txt")
    with test_file.open("w") as f:
        f.write("Test content")

    # Test calculate_file_hash
    hash_value = calculate_file_hash(test_file)
    logger.debug(f"Test file hash: {hash_value}")

    # Test get_file_hashes
    hashes = get_file_hashes([test_file])
    logger.debug(f"Test file hashes: {hashes}")

    # Test save and load hash file
    test_hash_data = {"test_func": {str(test_file): hash_value}}
    save_hash_file(test_hash_data)
    loaded_hash_data = load_hash_file()
    logger.debug(f"Loaded hash data: {loaded_hash_data}")

    # Clean up
    test_file.unlink()
    HASH_FILE_PATH.unlink()

    logger.debug("Finished hash utilities test")


# Example usage of test function
if __name__ == "__main__":
    test_hash_utils()
