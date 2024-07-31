from airflow.io.path import ObjectStoragePath
import logging
import pandas as pd

t_log = logging.getLogger("airflow.task")


def get_all_files(path: ObjectStoragePath) -> list[ObjectStoragePath]:
    """Recursively get all files in a directory."""
    return [f for f in path.rglob("*") if f.is_file()]


def get_all_checksums(path: ObjectStoragePath, files: list[ObjectStoragePath]) -> dict:
    """Get all checksums from a list of paths to files."""
    return {file.relative_to(path).path: file.checksum() for file in files}


def compare_checksums(
    src_checksums: dict,
    dst_checksums: dict,
    folder_name_src: str,
    folder_name_dst: str,
) -> None:
    """Compares two dicts of file checksums, raises and error in case of any mismatches"""

    checksum_mismatch = []
    for src_path, src_checksum in src_checksums.items():
        dst_path = src_path.replace(folder_name_src, folder_name_dst)
        dst_checksum = dst_checksums.get(dst_path)
        if dst_checksum is None:
            print(f"File missing in destination: {dst_path}")
            checksum_mismatch.append(f"Missing: {dst_path}")
        elif src_checksum != dst_checksum:
            print(
                f"Checksum mismatch for {dst_path}: src({src_checksum}) != dst({dst_checksum})"
            )
            checksum_mismatch.append(f"Mismatch: {dst_path}")

    if checksum_mismatch:
        raise Exception(
            "Oh no! Something went wrong, checksums did not match, stopping the pipeline!"
        )
    else:
        t_log.info("Copy from ingest to stage successful. All checksums match!")


def read_files_from_path(
    path: ObjectStoragePath, content_type: str, encoding: str
) -> pd.DataFrame:
    """Reads files from remote storage and returns as a dataframe."""

    files = [f for f in path.rglob("*") if f.is_file()]

    titles = []
    contents = []

    for f in files:
        bytes = f.read_block(offset=0, length=None)
        content = bytes.decode(encoding)

        titles.append(f.name)
        contents.append(content)

    df = pd.DataFrame(
        {
            "folder_path": path.path,
            "title": titles,
            content_type: contents,
        }
    )

    return df


def read_and_encode_img(
    path: ObjectStoragePath, content_type: str, encoding: str
) -> pd.DataFrame:
    """Reads and encodes img from remote storage and returns as a dataframe."""
    import base64

    files = [f for f in path.rglob("*") if f.is_file()]

    titles = []
    contents = []

    for f in files:
        bytes = f.read_block(offset=0, length=None)
        encoded_image = base64.b64encode(bytes).decode("utf-8")

        titles.append(f.name)
        contents.append(encoded_image)

    df = pd.DataFrame(
        {
            "folder_path": path.path,
            "title": titles,
            content_type: contents,
        }
    )

    return df