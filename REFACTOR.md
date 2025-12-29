Comprehensive plan to optimize the flow, improve readability, and enable a plugin system for extractors. Use something like pluggy that has a well defined plugin system and determine how to incorporate new extractors. This was a suggested path offered by an engineer, determine viability

### Current State Analysis

**Strengths:**
- Clear separation of concerns (extract_psgc, extract_meta, extract_region, etc.)
- Single-responsibility extraction modules
- Good use of Polars for performance
- Well-structured pipeline orchestration in match_psgc_schools.py

**Pain Points:**
- **Hard-coded pipeline sequence** in `extract_dataframes()` and __main__.py - can't easily add new extractors
- **Implicit dependencies** - unclear which modules depend on which others
- **Scattered configuration** - schema definitions spread across modules
- **Mixed concerns** - extraction, transformation, and schema definition in same files
- **Tuple return types** - `extract_dataframes()` returns 5 unnamed tuples, error-prone
- **No abstraction layer** - each extractor has different patterns for loading, transforming, saving

---

## Plan: Plugin-Based Extraction Architecture

### Phase 1: Create Abstraction Layer (Foundation)

**Goal:** Define interfaces that all extractors follow

**Files to create:**
1. **`src/foundation/plugin.py`** - Base extractor interface
2. **`src/foundation/schema.py`** - Centralized schema definitions
3. **`src/foundation/registry.py`** - Plugin discovery and orchestration

**Key changes:**

```
BaseExtractor (ABC)
├── name: str (unique identifier)
├── version: str
├── depends_on: list[str] (plugin names this depends on)
├── schema: dict (output schema definition)
├── extract(config: Config) → pl.DataFrame
├── validate(df: pl.DataFrame) → bool

PipelineOrchestrator
├── discover_plugins() → list[BaseExtractor]
├── topological_sort(plugins) → ordered list
├── execute(plugins, env) → dict[name -> DataFrame]
├── validate_all_outputs() → bool
```

**Benefits:**
- Clear contracts for new extractors
- Automatic dependency resolution
- Easy to add/remove extractors
- Validation at each stage

---

### Phase 2: Restructure Existing Extractors

**Goal:** Convert current modules to plugin-conforming classes

**Current layout:**
```
extract_psgc.py     → PsgcExtractor(name="psgc")
extract_meta.py     → EnrollmentExtractor(name="enrollment")
extract_geodata.py  → GeoExtractor(name="geo")
extract_region.py   → RegionMatcherExtractor(name="region_matcher")
extract_province.py → ProvinceMatcherExtractor(name="province_matcher")
extract_muni.py     → MuniMatcherExtractor(name="muni_matcher")
extract_brgy.py     → BrgyMatcherExtractor(name="brgy_matcher")
```

**New layout:**
```
plugins/
├── __init__.py
├── base.py          # BaseExtractor class
├── psgc.py          # PsgcExtractor
├── enrollment.py    # EnrollmentExtractor
├── geo.py           # GeoExtractor
├── matching/        # PSGC matching pipeline
│   ├── __init__.py
│   ├── base.py      # BaseMatcherExtractor
│   ├── region.py    # RegionExtractor
│   ├── province.py  # ProvinceExtractor
│   ├── muni.py      # MuniExtractor
│   └── brgy.py      # BrgyExtractor
└── enrichment/      # Future: add custom enrichment plugins
    └── __init__.py
```

**Key improvements:**
- Extractors have `depends_on` lists:
  - `PsgcExtractor`: no dependencies
  - `EnrollmentExtractor`: no dependencies
  - `GeoExtractor`: no dependencies
  - `RegionExtractor`: depends on ["psgc", "enrollment"]
  - `ProvinceExtractor`: depends on ["psgc", "region"]
  - `MuniExtractor`: depends on ["psgc", "province"]
  - `BrgyExtractor`: depends on ["psgc", "muni"]

---

### Phase 3: Create Schema Registry

**Goal:** Centralize all schema definitions for discoverability and validation

**File: `src/foundation/schema.py`**

```python
@dataclass
class ColumnDef:
    name: str
    dtype: pl.DataType
    nullable: bool = True
    description: str = ""

@dataclass
class TableSchema:
    name: str
    columns: dict[str, ColumnDef]
    primary_key: list[str] | None = None

    def to_polars_schema(self) -> dict[str, pl.DataType]:
        ...

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        ...

# Registry of all schemas
SCHEMAS = {
    "psgc": TableSchema(...),
    "enrollment": TableSchema(...),
    "geo": TableSchema(...),
    "region_codes": TableSchema(...),
    "province_codes": TableSchema(...),
    ...
}
```

**Benefits:**
- Single source of truth for all schemas
- Self-documenting code
- Can generate docs, SQL, validation rules from registry
- Easy to track changes across versions

---

### Phase 4: Refactor Pipeline Orchestration

**Goal:** Replace hard-coded pipeline with dynamic orchestration

**Current (extract_dataframes.py):**
```python
def extract_dataframes() -> tuple[pl.DataFrame, ...]:
    psgc_df = set_psgc(...)
    school_year_meta, enroll_df, levels_df = unpack_enroll_data(...)
    meta_psgc = match_psgc_schools(...)
    ...
    return (psgc_df, enroll_df, geo_df, levels_df, addr_df)
```

**New (`pipeline.py`):**
```python
class Pipeline:
    def __init__(self, config: Config):
        self.registry = PluginRegistry()
        self.config = config

    def discover_plugins(self) -> list[BaseExtractor]:
        """Discover all plugins in plugins/ directory"""

    def resolve_dependencies(self) -> list[BaseExtractor]:
        """Topological sort plugins by dependencies"""

    def execute(self) -> PipelineOutput:
        """Run plugins in correct order, passing outputs as inputs"""
        results = {}
        for plugin in self.resolve_dependencies():
            inputs = {dep: results[dep] for dep in plugin.depends_on}
            results[plugin.name] = plugin.extract(
                config=self.config,
                **inputs  # Pass upstream results as kwargs
            )
        return PipelineOutput(results)

    def validate(self) -> ValidationReport:
        """Validate all outputs against schemas"""

class PipelineOutput:
    """Type-safe container for pipeline results"""
    psgc: pl.DataFrame
    enrollment: pl.DataFrame
    geo: pl.DataFrame
    region_codes: pl.DataFrame
    province_codes: pl.DataFrame
    muni_codes: pl.DataFrame
    brgy_codes: pl.DataFrame

    def to_dict(self) -> dict[str, pl.DataFrame]:
        ...
```

**Benefits:**
- Plugins are discovered automatically
- New plugins added to `plugins/` directory work immediately
- Dependencies are explicit and validated at startup
- Easy to compose complex pipelines
- Testable in isolation

---

### Phase 5: Refactor CLI

**Current (__main__.py):**
```python
def build():
    psgc_df, enroll_df, geo_df, levels_df, addr_df = extract_dataframes()
    # ... hard-coded table loading
```

**New approach:**
```python
def build():
    config = Config.from_env()
    pipeline = Pipeline(config)

    # Validate all plugins are available
    available_plugins = pipeline.discover_plugins()
    click.echo(f"Found {len(available_plugins)} extractors")

    # Execute pipeline
    output = pipeline.execute()
    validation = pipeline.validate()

    if not validation.passed:
        click.echo("Validation failed:", err=True)
        for error in validation.errors:
            click.echo(f"  - {error}", err=True)
        sys.exit(1)

    # Load results dynamically into database
    for table_name, df in output.to_dict().items():
        db = add_to(db, df, table_name)

    click.echo("✓ Pipeline completed successfully")
```

---

### Phase 6: Enable Plugin Discovery

**File: __init__.py**

```python
from importlib import import_module
from pathlib import Path
from .base import BaseExtractor

class PluginRegistry:
    @staticmethod
    def discover() -> dict[str, type[BaseExtractor]]:
        """Discover all extractors in subdirectories"""
        plugins = {}
        plugin_dir = Path(__file__).parent

        for plugin_file in plugin_dir.glob("**/*.py"):
            if plugin_file.name.startswith("_"):
                continue

            # Import and register
            module = import_module(f".{plugin_file.stem}", package=__name__)
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (isinstance(attr, type) and
                    issubclass(attr, BaseExtractor) and
                    attr is not BaseExtractor):
                    plugins[attr.name] = attr

        return plugins
```

**Benefits:**
- No manual plugin registration
- Just drop a new file in `plugins/` and it's auto-discovered
- Works with nested directories (e.g., `plugins/enrichment/custom_enricher.py`)

---

### Phase 7: Add Configuration Management

**File: `src/foundation/config.py`**

```python
@dataclass
class ExtractorConfig:
    """Per-extractor configuration"""
    enabled: bool = True
    version: str | None = None  # Filter by version
    cache: bool = False
    validate: bool = True

class Config:
    """Global pipeline configuration"""
    extractors: dict[str, ExtractorConfig]
    output_format: str = "sqlite"
    validation_level: str = "strict"  # strict | warning | none

    @classmethod
    def from_env(cls) -> Config:
        """Load from environment variables"""

    @classmethod
    def from_file(cls, path: Path) -> Config:
        """Load from YAML config file"""
```

**Usage:**
```yaml
# config.yml
pipeline:
  validation_level: strict
  output_format: sqlite

extractors:
  psgc:
    enabled: true
  enrollment:
    enabled: true
  geo:
    enabled: true
  region_matcher:
    enabled: true
  custom_enricher:  # New plugin!
    enabled: true
    version: ">=1.0.0"
```

---

### Phase 8: Add Extensibility Points

**Key interfaces for plugin developers:**

```python
class TransformationStep(ABC):
    """Composable transformation"""
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        pass

class ValidationRule(ABC):
    """Custom validation rules"""
    def validate(self, df: pl.DataFrame) -> bool:
        pass

class OutputWriter(ABC):
    """Custom output targets (sqlite, parquet, etc.)"""
    def write(self, name: str, df: pl.DataFrame) -> None:
        pass
```

---

### Implementation Roadmap

| Phase | Duration | Effort | Priority |
|-------|----------|--------|----------|
| Phase 1: Base abstraction | 3-4 days | Medium | **HIGH** |
| Phase 2: Restructure extractors | 3-4 days | Medium | **HIGH** |
| Phase 3: Schema registry | 2 days | Low | **HIGH** |
| Phase 4: Pipeline orchestration | 2-3 days | Medium | **HIGH** |
| Phase 5: CLI refactor | 1 day | Low | **MEDIUM** |
| Phase 6: Plugin discovery | 1 day | Low | **MEDIUM** |
| Phase 7: Config management | 1-2 days | Low | **MEDIUM** |
| Phase 8: Extensibility points | 2 days | Medium | **LOW** |

**Total: 2-3 weeks for full refactor**

---

### Benefits Summary

✅ **Readability:** Clear interfaces, centralized schemas, explicit dependencies
✅ **Testability:** Each plugin is independently testable
✅ **Maintainability:** Changes to one extractor don't affect others
✅ **Extensibility:** Add new extractors without modifying core code
✅ **Flexibility:** Enable/disable extractors via config
✅ **Discoverability:** Self-documenting plugin system
✅ **Error handling:** Centralized validation and error reporting
✅ **Performance:** Lazy loading, caching, parallel execution possible

---

### Migration Path (Non-Breaking)

1. Keep current modules in foundation
2. Create `src/foundation/plugins/` with new structure
3. New pipeline coexists with old code
4. Gradually migrate extractors to plugins
5. Switch CLI to use new pipeline
6. Remove old modules when all tests pass

Would you like me to proceed with implementing Phase 1 (Base abstraction layer) to show concrete examples of this architecture?
