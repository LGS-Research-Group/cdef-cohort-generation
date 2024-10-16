import glob
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar, cast

from imohash import hashfile  # type: ignore

from cdef_cohort_builder.utils import config
from cdef_cohort_builder.utils.config import HASH_FILE_PATH

T = TypeVar("T")


def calculate_file_hash(file_path: Path) -> str:
    """Calculate the hash of a file using imohash."""
    return str(hashfile(str(file_path), hexdigest=True))


def get_file_hashes(file_paths: list[Path]) -> dict[str, str]:
    """Calculate hashes for a list of files."""
    return {str(file_path): calculate_file_hash(file_path) for file_path in file_paths}


def load_hash_file() -> dict[str, dict[str, str]]:
    """Load the hash file if it exists, otherwise return an empty dict."""
    if HASH_FILE_PATH.exists():
        with HASH_FILE_PATH.open("r") as f:
            return cast(dict[str, dict[str, str]], json.load(f))
    return {}


def save_hash_file(hash_data: dict[str, dict[str, str]]) -> None:
    """Save the hash data to the hash file."""
    with HASH_FILE_PATH.open("w") as f:
        json.dump(hash_data, f, indent=2)


def process_with_hash_check(process_func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    """
    Wrapper function to handle hash checking and processing.

    Args:
    process_func: The function to call for processing (e.g., process_lpr_adm)
    *args, **kwargs: Additional arguments to pass to process_func
    """
    # Generate the expected variable names
    register_name = "_".join(process_func.__name__.upper().split("_")[1:])
    input_files_var = f"{register_name}_FILES"
    output_file_var = f"{register_name}_OUT"

    # Get the input and output file information from the config module
    input_files = getattr(config, input_files_var, None)
    output_file = getattr(config, output_file_var, None)

    if not input_files or not output_file:
        raise ValueError(
            f"Could not find input or output file information for {process_func.__name__}"
        )

    # Get list of input files
    file_pattern = str(input_files)
    if not file_pattern.endswith("*.parquet"):
        file_pattern = str(input_files / "*.parquet")
    files = glob.glob(file_pattern)

    # Calculate hashes for input files
    current_hashes = get_file_hashes([Path(f) for f in files])

    # Load existing hashes
    all_hashes = load_hash_file()

    # Check if processing is needed
    register_name = process_func.__name__
    if register_name in all_hashes and all_hashes[register_name] == current_hashes:
        print(f"Input files for {register_name} haven't changed. Skipping processing.")
        return

    # Process the data
    process_func(*args, **kwargs)

    # Update and save hashes
    all_hashes[register_name] = current_hashes
    save_hash_file(all_hashes)

    print(f"Processed {register_name} data and saved to {output_file}")
    print(f"Updated hash information in {HASH_FILE_PATH}")
