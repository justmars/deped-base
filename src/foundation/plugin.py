from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Mapping

import polars as pl


@dataclass(frozen=True)
class SourcePaths:
    """Paths to all local inputs required by the pipeline."""

    enroll_dir: Path
    psgc_file: Path
    geo_file: Path


@dataclass(frozen=True)
class ExtractionContext:
    """Shared context that is passed to every extractor."""

    paths: SourcePaths


@dataclass
class ExtractorConfig:
    """Configuration that can be customized per extractor."""

    enabled: bool = True
    version: str | None = None
    validate: bool = True


@dataclass
class ExtractionResult:
    """Results emitted by an extractor."""

    tables: Mapping[str, pl.DataFrame]
    metrics: dict[str, object] = field(default_factory=dict)


class BaseExtractor(ABC):
    """Extractor interface that every plugin must implement."""

    name: ClassVar[str]
    version: ClassVar[str] = "0.1.0"
    depends_on: ClassVar[list[str]] = []
    outputs: ClassVar[list[str]] = []
    schema_name: ClassVar[str | None] = None

    def __init__(self, config: ExtractorConfig | None = None):
        self.config = config or ExtractorConfig()

    @abstractmethod
    def extract(
        self,
        context: ExtractionContext,
        dependencies: dict[str, pl.DataFrame],
    ) -> ExtractionResult:
        """Return the tables produced by this extractor."""

    def validate(self, result: ExtractionResult) -> list[str]:
        """Return validation errors (empty if validation passes)."""

        return []
