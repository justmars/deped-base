# Instructions

## Code Standards

- Use Google-style docstrings
- Prefer single-responsibility functions
  - If not describable in one sentence → decompose
- DRY: extract shared logic
- Remove or consolidate unused / redundant code

## Performance

- Consider indexes for hot paths

## Project Hygiene

- Update `README` for process changes or major refactors
- If `docs/` exists, keep relevant documentation in sync

## Typing

- Prefer `X | None` over `Optional[X]`

## Markdown

- Enforce `MD032`: blank lines around lists
