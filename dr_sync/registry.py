"""Sync module registry for extensible sync operations."""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional


@dataclass
class SyncModule:
    """Metadata for a sync module.

    Attributes:
        name: Module name (used for CLI and logging).
        description: Human-readable description.
        resource_types: List of Unity Catalog resource types synced.
        dependencies: List of sync module names that must run first.
        function: Callable that executes the sync.
        requires_source_client: Whether source WorkspaceClient is required.
        requires_target_client: Whether target WorkspaceClient is required.
        requires_spark: Whether Spark session is required (for notebooks).
    """

    name: str
    description: str
    resource_types: List[str]
    dependencies: List[str]
    function: Callable
    requires_source_client: bool = True
    requires_target_client: bool = True
    requires_spark: bool = False


class SyncRegistry:
    """Registry of sync modules with dependency resolution.

    Modules are registered via decorator and executed in dependency order.
    """

    def __init__(self):
        self._modules: Dict[str, SyncModule] = {}

    def register(self, module: SyncModule):
        """Register a sync module.

        Args:
            module: SyncModule metadata.

        Raises:
            ValueError: If module name already registered.
        """
        if module.name in self._modules:
            raise ValueError(f"Sync module '{module.name}' already registered")
        self._modules[module.name] = module

    def get(self, name: str) -> Optional[SyncModule]:
        """Get registered sync module by name.

        Args:
            name: Module name.

        Returns:
            SyncModule if found, None otherwise.
        """
        return self._modules.get(name)

    def list_all(self) -> List[SyncModule]:
        """List all registered sync modules.

        Returns:
            List of SyncModule objects.
        """
        return list(self._modules.values())

    def get_execution_order(self, modules: Optional[List[str]] = None) -> List[SyncModule]:
        """Get modules in dependency-resolved execution order.

        Args:
            modules: List of module names to execute (None = all registered).

        Returns:
            List of SyncModule objects in execution order.

        Raises:
            ValueError: If circular dependency detected or dependency not found.
        """
        if modules is None:
            modules = list(self._modules.keys())

        # Topological sort (Kahn's algorithm)
        in_degree = {name: 0 for name in modules}
        order = []
        queue = []

        # Build graph and calculate in-degrees
        graph: Dict[str, List[str]] = {name: [] for name in modules}

        for name in modules:
            module = self._modules[name]
            for dep in module.dependencies:
                if dep not in modules:
                    raise ValueError(f"Module '{name}' depends on unregistered module '{dep}'")
                if dep in graph:
                    graph[dep].append(name)
                    in_degree[name] += 1

        # Start with modules that have no dependencies
        for name in modules:
            if in_degree[name] == 0:
                queue.append(name)

        # Process nodes
        while queue:
            name = queue.pop(0)
            order.append(self._modules[name])

            for dependent in graph[name]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # Check for cycles
        if len(order) != len(modules):
            raise ValueError("Circular dependency detected in sync modules")

        return order


# Global registry instance
_registry = SyncRegistry()


def register_sync(
    name: str,
    description: str,
    resource_types: List[str],
    dependencies: Optional[List[str]] = None,
    requires_source_client: bool = True,
    requires_target_client: bool = True,
    requires_spark: bool = False,
):
    """Decorator to register a sync function.

    Usage:
        @register_sync(
            name="catalogs",
            description="Sync Unity Catalog catalogs and schemas",
            resource_types=["catalog", "schema"],
            dependencies=["credentials"],
        )
        def sync_catalogs(config, source_client, target_client, logger):
            ...
    """

    def decorator(func: Callable) -> Callable:
        module = SyncModule(
            name=name,
            description=description,
            resource_types=resource_types,
            dependencies=dependencies or [],
            function=func,
            requires_source_client=requires_source_client,
            requires_target_client=requires_target_client,
            requires_spark=requires_spark,
        )
        _registry.register(module)
        return func

    return decorator


def get_registry() -> SyncRegistry:
    """Get the global sync registry."""
    return _registry
