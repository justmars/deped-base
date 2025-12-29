from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Type

from .plugin import BaseExtractor


class PluginRegistry:
    """Discover extractor subclasses via filesystem scanning."""

    def __init__(
        self, package_root: Path | None = None, package_name: str | None = None
    ):
        self.package_root = package_root or Path(__file__).parent
        self.plugins_dir = self.package_root / "plugins"
        self.package_name = package_name or self._default_package_name()

    def _default_package_name(self) -> str:
        module_name = __name__
        if module_name.endswith(".registry"):
            return module_name[: -len(".registry")]
        return module_name

    def discover(self) -> dict[str, Type[BaseExtractor]]:
        plugins: dict[str, Type[BaseExtractor]] = {}

        for module_path in self.plugins_dir.rglob("*.py"):
            if (
                module_path.name.startswith("_")
                or "__pycache__" in module_path.parts
                or module_path.name == "__init__.py"
            ):
                continue

            relative = module_path.relative_to(self.package_root)
            module_name = ".".join(
                [self.package_name] + list(relative.with_suffix("").parts)
            )
            module = import_module(module_name)
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, BaseExtractor)
                    and attr is not BaseExtractor
                ):
                    key = attr.name
                    if key in plugins:
                        raise RuntimeError(f"Duplicate extractor name: {key}")
                    plugins[key] = attr

        return plugins
