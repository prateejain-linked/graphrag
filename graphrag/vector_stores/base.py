# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Base classes for vector stores."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from graphrag.model.entity import Entity
from graphrag.model.types import TextEmbedder

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
        db_connection: Any | None = None,
        document_collection: Any | None = None,
        query_filter: Any | None = None,
        **kwargs: Any,
    ):
        self.collection_name = collection_name
        self.vector_name = vector_name
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
    def load_parqs(self, data_path: str, parqs: list[str]) -> Any:
        """Load documents (Parquet files) into the vector-store."""

    #TODO This is temporary until I take out the client from the vector store class
    @abstractmethod
    def execute_query(self, query: str) -> Any:
        """Execute a query in the vector-store."""

    @abstractmethod
    def get_extracted_entities(
        self, text: str, text_embedder: TextEmbedder, k: int = 10, **kwargs: Any
    ) -> list[Entity]:
        """From a query, build a subtable of entities which is only matching entities."""

    @abstractmethod
    def read_parqs(self, data_dir, parq_names) -> Any:
        """Return a dictionary of parquet dataframes of parq_name to data frame."""

    @abstractmethod
    def get_related_entities(self, titles: list[str], **kwargs: Any) -> list[Entity]:
        """Get related entities from the vector store."""