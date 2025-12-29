# Polars Migration Refactor

**Branch:** `refactor/polars-migration`
**Status:** Planning Phase
**Target Release:** Q1 2026

## Overview

Refactor data processing pipeline from Pandas to Apache Polars for **3-5x performance improvement** on data import/cleaning operations. This addresses the primary bottleneck when processing 540k+ enrollment records, 50k+ PSGC entries, and 48k+ geospatial records with heavy string normalization.

## Motivation

- **Performance:** Current pipeline takes ~22s; Polars should achieve ~5s
- **Memory:** Arrow columnar format reduces memory footprint by 40-50%
- **Workload fit:** String processing, wide→long transforms, multi-step joins are Polars strengths
- **Future-proof:** Apache Arrow standard, active development, growing ecosystem

## Scope

### Phase 1: Core I/O (Target: Week 1)
- [ ] Add Polars to dependencies
- [ ] Refactor `extract_psgc.py` (19 lines) → Polars
- [ ] Refactor `extract_geodata.py` (10 lines) → Polars
- [ ] Benchmark against Pandas equivalents

### Phase 2: Core Processing (Target: Week 2)
- [ ] Refactor `extract_meta.py` (370 lines) → Polars
  - Melt/concat chain
  - String operations
  - Grade/strand/sex parsing (critical)
- [ ] Refactor string cleaners (46+78 lines)
- [ ] Full pipeline benchmark

### Phase 3: Complex Matching (Target: Week 3)
- [ ] Refactor `extract_province.py` (259 lines) → Polars
- [ ] Refactor `extract_muni.py` (69 lines) → Polars
- [ ] Refactor `extract_brgy.py` (38 lines) → Polars
- [ ] Comprehensive testing on full DepEd dataset

### Phase 4: Integration (Target: Week 4)
- [ ] Update all imports and type hints
- [ ] Deprecate Pandas dependencies
- [ ] Documentation updates
- [ ] Performance profiling + validation

## Key Changes

### Dependency Updates
```toml
# Add to pyproject.toml
dependencies = [
    "polars>=0.20.13",  # Main refactor
    "calamine>=0.2.8",  # Rust-based XLSX (faster than openpyxl)
]

# Keep in dev during transition
dev = [
    "pandas>=2.3",  # For equivalence testing
]
```

### Major Code Changes

| Module | Changes | Complexity |
|--------|---------|-----------|
| `extract_psgc.py` | `pd.read_excel()` → `pl.read_excel()` | Low |
| `extract_geodata.py` | CSV read + join | Low |
| `extract_meta.py` | Melt → unpivot, string ops | Medium |
| String cleaners | Regex → `.str` methods | Medium |
| `extract_province.py` | Multi-merge chains + priority | **High** |
| `extract_muni.py` | Join + fallback logic | Medium |
| `extract_brgy.py` | Groupby + match | Low |

### Problem Areas Requiring Refactoring

1. **Grade/Strand/Sex Parsing** (`split_grade_strand_sex()`)
   - Pandas: Index-based column selection on split result
   - Polars: Use `.str.split_exact()` → struct fields
   - Effort: Medium, requires thorough testing

2. **Multi-way Merge with Priority** (`extract_province.py`)
   - Pandas: Sequential `.merge()` + `.fillna()` chain
   - Polars: Chain `.join()` + use `.coalesce()`
   - Effort: High, needs equivalence testing

3. **Location Name Normalization**
   - Pandas: `.apply()` with custom functions
   - Polars: Prefer native `.str.extract()` / `.str.replace()`
   - Effort: Medium, refactor to avoid `.map_elements()`

## Performance Targets

| Operation | Current | Target | Gain |
|-----------|---------|--------|------|
| PSGC XLSX load | 800ms | 250ms | 3.2x |
| CSV load + process | 4.5s | 900ms | 5x |
| String cleaning | 4s | 600ms | 6.7x |
| PSGC matching | ~10s | ~2s | 5x |
| **Full pipeline** | **~22s** | **~5s** | **~4.6x** |

## Testing Strategy

1. **Unit tests:** Polars equivalents for each transformed module
2. **Equivalence tests:** Pandas vs Polars output matching on real data
3. **Benchmarking:** Profiling each module independently
4. **Integration tests:** Full pipeline on complete DepEd dataset

## Migration Checklist

- [ ] Create `polars_*` module versions alongside Pandas (during Phases 1-2)
- [ ] Write equivalence tests (Pandas output == Polars output)
- [ ] Performance benchmark each phase
- [ ] Update `extract_dataframes.py` to call Polars modules
- [ ] Remove Pandas dependencies gradually
- [ ] Update documentation and README
- [ ] Tag release version

## Rollback Plan

If performance goals not met or blockers found:
1. Keep Pandas code in `legacy/` branch
2. Maintain equivalence test suite
3. Return to Pandas with optimizations (e.g., chunked processing)

## Success Criteria

✅ Overall pipeline execution time < 10s on full dataset
✅ Memory usage < 2GB during peak processing
✅ All output data validates against Pandas baseline
✅ Test coverage maintained at ≥80%

## References

- [Polars Documentation](https://docs.pola-rs.com/)
- [Apache Arrow Format](https://arrow.apache.org/)
- [Performance Comparison](https://www.pola-rs.com/posts/polars_lazyness/)

---

**Created:** December 29, 2025
**Branch:** `refactor/polars-migration`
**Lead:** Performance optimization initiative
