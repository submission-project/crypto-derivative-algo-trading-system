import os
from typing import Optional
from common.logging import setup_logger

logger = setup_logger(__name__)


def read_file_if_exists(path: Optional[str]) -> Optional[str]:
    if not path:
        return None

    if not os.path.exists(path):
        logger.warning(f"파일이 존재하지 않습니다: {path}")
        return None

    with open(path, "r") as f:
        return f.read()
