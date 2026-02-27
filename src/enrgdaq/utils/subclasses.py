import importlib
import pkgutil

_imported_packages: set[str] = set()


def _ensure_modules_imported(module_name: str) -> None:
    """
    Recursively import all submodules of a given package to ensure
    all subclasses are registered in memory.
    """
    if not module_name or module_name in _imported_packages:
        return
    _imported_packages.add(module_name)

    try:
        package = importlib.import_module(module_name)
    except Exception:
        return

    if not hasattr(package, "__path__"):
        return

    for _, name, _ in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        try:
            importlib.import_module(name)
        except Exception:
            pass


def all_subclasses(cls: type) -> set[type]:
    """
    Returns a set of all subclasses of a given class, ensuring that
    modules in the same top-level package are imported so all subclasses are found.

    Args:
        cls (type): The class to get subclasses of.

    Returns:
        set[type]: A set of all subclasses of the given class.
    """
    if hasattr(cls, "__module__") and cls.__module__:
        top_pkg = cls.__module__.split(".")[0]
        _ensure_modules_imported(top_pkg)

    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )
