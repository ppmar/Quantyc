"""
Abstract base for all extractors.

Every extractor implements extract(document_id, pdf_bytes) and returns
a dict of extracted values, or None on failure.
"""

from abc import ABC, abstractmethod


class Extractor(ABC):
    """Base class for document extractors."""

    @abstractmethod
    def extract(self, document_id: int, pdf_bytes: bytes) -> dict | None:
        """
        Extract structured data from a PDF.

        Args:
            document_id: The documents.document_id
            pdf_bytes: Raw PDF bytes (in RAM, never on disk)

        Returns:
            Dict of extracted field→value pairs, or None on failure.
        """
        ...

    @property
    @abstractmethod
    def doc_types(self) -> list[str]:
        """Which doc_types this extractor handles."""
        ...
