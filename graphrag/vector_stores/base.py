# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Base classes for vector stores."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from graphrag.model.community_report import CommunityReport
from graphrag.model.entity import Entity
from graphrag.model.types import TextEmbedder
from graphrag.model import TextUnit

DEFAULT_VECTOR_SIZE: int = 1536


@dataclass
class VectorStoreDocument:
    """A document that is stored in vector storage."""

    id: str | int
    """unique id for the document"""

    text: str | None
    vector: list[float] | None

    attributes: dict[str, Any] = field(default_factory=dict)
    """store any additional metadata, e.g. title, date ranges, etc"""


@dataclass
class VectorStoreSearchResult:
    """A vector storage search result."""

    document: VectorStoreDocument
    """Document that was found."""

    score: float
    """Similarity score between 0 and 1. Higher is more similar."""


class BaseVectorStore(ABC):
    """The base class for vector storage data-access classes."""

    def __init__(
        self,
        collection_name: str,
        vector_name: str,
        reports_name: str,
        text_units_name: str,
        db_connection: Any | None = None,
        document_collection: Any | None = None,
        query_filter: Any | None = None,
        **kwargs: Any,
    ):
        self.collection_name = collection_name
        self.vector_name = vector_name
        self.reports_name = reports_name
        self.text_units_name = text_units_name
        self.db_connection = db_connection
        self.document_collection = document_collection
        self.query_filter = query_filter
        self.kwargs = kwargs

    @abstractmethod
    def connect(self, **kwargs: Any) -> None:
        """Connect to vector storage."""

    @abstractmethod
    def load_documents(
        self, documents: list[VectorStoreDocument], overwrite: bool = True
    ) -> None:
        """Load documents into the vector-store."""

    @abstractmethod
    def similarity_search_by_vector(
        self, query_embedding: list[float], k: int = 10, **kwargs: Any
    ) -> list[VectorStoreSearchResult]:
        """Perform ANN search by vector."""

    @abstractmethod
    def similarity_search_by_text(
        self, text: str, text_embedder: TextEmbedder, k: int = 10, **kwargs: Any
    ) -> list[VectorStoreSearchResult]:
        """Perform ANN search by text."""

    @abstractmethod
    def filter_by_id(self, include_ids: list[str] | list[int]) -> Any:
        """Build a query filter to filter documents by id."""

    @abstractmethod
    def get_extracted_entities(
        self, text: str, text_embedder: TextEmbedder, k: int = 10, **kwargs: Any
    ) -> list[Entity]:
        """From a query, build a subtable of entities which is only matching entities."""

    @abstractmethod
    def load_entities(self, entities: list[Entity], overwrite: bool = True) -> None:
        """Load entities into the vector-store."""

    @abstractmethod
    def load_reports(self, reports: list[CommunityReport], overwrite: bool = True) -> None:
        """Load reports into the vector-store."""

    @abstractmethod
    def get_extracted_reports(
        self, community_ids: list[int], **kwargs: Any
    ) -> list[CommunityReport]:
        """Get reports for a given list of community ids."""

    @abstractmethod
    def setup_entities(self) -> None:
        """Setup the entities in the vector-store."""

    @abstractmethod
    def setup_reports(self) -> None:
        """Setup the reports in the vector-store."""

    @abstractmethod
    def setup_text_units(self) -> None:
        """Setup the reports in the vector-store."""

    @abstractmethod
    def load_text_units(self, units: list[TextUnit], overwrite: bool = True) -> None:
        """Load reports into the vector-store."""

    @abstractmethod
    def unload_entities(self) -> None:
        """Remove context from the databases."""