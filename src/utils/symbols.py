"""
Symbols file reader
"""
from typing import List
from pathlib import Path


class SymbolsReader:
    """Read trading symbols from file"""

    @staticmethod
    def read_from_file(file_path: str) -> List[str]:
        """Read symbols from file, one per line"""
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"Symbols file not found: {file_path}")

        symbols = []
        with open(path, 'r') as f:
            for line in f:
                symbol = line.strip()
                if symbol and not symbol.startswith('#'):
                    symbols.append(symbol)

        return symbols
