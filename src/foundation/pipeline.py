from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from environs import EnvError
from rich.console import Console

from .common import env
from .plugin import BaseExtractor, ExtractionContext, SourcePaths
from .registry import PluginRegistry
from .schema import SCHEMAS


@dataclass(frozen=True)
class ExtractedFrames:
    """Legacy container that mirrors the original pipeline outputs."""

    psgc: pl.DataFrame
    enrollment: pl.DataFrame
    geo: pl.DataFrame
    levels: pl.DataFrame
    address: pl.DataFrame


@dataclass
class PipelineOutput:
    """Aggregated plugin output for the new orchestration flow."""

    tables: dict[str, pl.DataFrame]
    metrics: dict[str, object] = field(default_factory=dict)

    def get(self, name: str) -> pl.DataFrame | None:
        return self.tables.get(name)


class PipelineExecutionError(RuntimeError):
    """Raised when the plugin dependency graph cannot be resolved."""


class PluginPipeline:
    """Discover, sort, and execute extractor plugins."""

    def __init__(self, registry: PluginRegistry | None = None):
        self.console = Console()
        self.registry = registry or PluginRegistry()
        self.paths = self._resolve_source_paths()
        self.context = ExtractionContext(paths=self.paths)
        self.plugins = self._load_plugins()
        self.execution_order = self._resolve_execution_order()

    def _resolve_source_paths(self) -> SourcePaths:
        enroll_dir = env.path("ENROLL_DIR")
        psgc_file = env.path("PSGC_FILE")
        geo_file = env.path("GEO_FILE")
        project_root = Path(__file__).resolve().parents[1].parent
        default_region_file = project_root / "data" / "regions.yml"
        default_hr_dir = project_root / "data" / "hr"
        try:
            region_names_file = env.path("REGION_NAMES_FILE")
        except EnvError:
            region_names_file = default_region_file
        try:
            hr_dir = env.path("HR_DIR")
        except EnvError:
            hr_dir = default_hr_dir

        for label, path in (
            ("enroll_dir", enroll_dir),
            ("psgc_file", psgc_file),
            ("geo_file", geo_file),
            ("region_names_file", region_names_file),
            ("hr_dir", hr_dir),
        ):
            self.console.log(f"[bold slate_blue1]{label}[/bold slate_blue1]={path}")

        return SourcePaths(
            enroll_dir=enroll_dir,
            psgc_file=psgc_file,
            geo_file=geo_file,
            region_names_file=region_names_file,
            hr_dir=hr_dir,
        )

    def _load_plugins(self) -> dict[str, BaseExtractor]:
        plugin_classes = self.registry.discover()
        instances: dict[str, BaseExtractor] = {}
        for name, cls in plugin_classes.items():
            instances[name] = cls()
        return instances

    def _map_table_producers(self) -> dict[str, str]:
        producers: dict[str, str] = {}
        for name, plugin in self.plugins.items():
            for table_name in getattr(plugin, "outputs", []):
                if table_name in producers:
                    raise PipelineExecutionError(
                        f"Table '{table_name}' already produced by {producers[table_name]}"
                    )
                producers[table_name] = name
        return producers

    def _build_dependency_graph(self) -> dict[str, set[str]]:
        graph: dict[str, set[str]] = {name: set() for name in self.plugins}
        producers = self._map_table_producers()
        for name, plugin in self.plugins.items():
            for dependency in getattr(plugin, "depends_on", []):
                producer = producers.get(dependency)
                if producer is None:
                    raise PipelineExecutionError(
                        f"{name} requires '{dependency}' but no plugin produces it"
                    )
                graph[name].add(producer)
        return graph

    def _resolve_execution_order(self) -> list[BaseExtractor]:
        graph = self._build_dependency_graph()
        graph_copy = {name: set(deps) for name, deps in graph.items()}
        order: list[BaseExtractor] = []
        ready = [name for name, deps in graph_copy.items() if not deps]

        while ready:
            current = ready.pop(0)
            order.append(self.plugins[current])
            for downstream, deps in graph_copy.items():
                if current in deps:
                    deps.remove(current)
                    if not deps:
                        ready.append(downstream)

        if len(order) != len(self.plugins):
            unresolved = {name for name, deps in graph_copy.items() if deps}
            raise PipelineExecutionError(f"Circular dependency detected: {unresolved}")

        return order

    def execute(self) -> PipelineOutput:
        collected: dict[str, pl.DataFrame] = {}
        metrics: dict[str, object] = {}

        for plugin in self.execution_order:
            with self.console.status(
                f"[bold green]Extracting[/bold green] [cyan]{plugin.name}[/cyan]: ",
                spinner="dots",
            ):
                inputs: dict[str, pl.DataFrame] = {}
                missing: list[str] = []
                for dependency in plugin.depends_on:
                    table = collected.get(dependency)
                    if table is None:
                        missing.append(dependency)
                    else:
                        inputs[dependency] = table
                if missing:
                    raise PipelineExecutionError(
                        f"{plugin.name} cannot resolve inputs: {missing}"
                    )

                result = plugin.extract(context=self.context, dependencies=inputs)
                for table_name, table in result.tables.items():
                    self._validate_table_contract(table_name=table_name, table=table)
                    collected[table_name] = table
                metrics.update(result.metrics)
                self.console.log(
                    f"[green]✓ Completed[/green] extractor [cyan]{plugin.name}[/cyan]"
                )

        return PipelineOutput(tables=collected, metrics=metrics)

    def _validate_table_contract(self, table_name: str, table: pl.DataFrame) -> None:
        schema = SCHEMAS.get(table_name)
        if not schema:
            return

        errors = schema.validate(table)
        if not errors:
            self.console.log(f"[green]Validated schema[/green] {table_name}")
            return

        sample = table.head(3).to_dicts()
        raise PipelineExecutionError(
            f"Schema validation failed for {table_name}: "
            f"{'; '.join(errors)}; sample={sample}"
        )

    def get_output_table(self, output: PipelineOutput, key: str) -> pl.DataFrame | None:
        """Return a specific table emitted by the pipeline (if present)."""

        return output.tables.get(key)


def frames_from_pipeline_output(output: PipelineOutput) -> ExtractedFrames:
    return ExtractedFrames(
        psgc=output.tables["psgc"],
        enrollment=output.tables["enrollment"],
        geo=output.tables["geo"],
        levels=output.tables["school_levels"],
        address=output.tables["address"],
    )


def extract_dataframes() -> ExtractedFrames:
    pipeline = PluginPipeline()
    return frames_from_pipeline_output(pipeline.execute())
