import os
from dataclasses import dataclass


@dataclass
class FileInfo:
    file_path: str
    dir_path: str
    filename: str
    format: str


def extract_file_info(file_path: str) -> FileInfo:
    dir_path = os.path.dirname(file_path)
    filename_with_ext = os.path.basename(file_path)
    filename, file_format = os.path.splitext(filename_with_ext)
    file_format = file_format[1:] if file_format.startswith('.') else file_format
    return FileInfo(
        file_path=file_path,
        dir_path=dir_path,
        filename=filename,
        format=file_format
    )
