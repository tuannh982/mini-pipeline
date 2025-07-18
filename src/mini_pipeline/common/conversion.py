import csv
import json
import io

from mini_pipeline.common.logging import logger


def csv_binary_to_json_binary(binary_csv_data: bytes, encoding='utf-8') -> bytes:
    try:
        decoded = binary_csv_data.decode(encoding)
        csv_text_stream = io.StringIO(decoded)
        csv_reader = csv.DictReader(csv_text_stream)
        data = list(csv_reader)
        json_str = json.dumps(data, indent=4)
        json_binary = json_str.encode(encoding)
        return json_binary
    except Exception as e:
        logger.warning(f"Error occurred while converting CSV to JSON {e}")
        return b''
