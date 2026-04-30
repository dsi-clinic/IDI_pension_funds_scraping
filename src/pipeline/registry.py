"""Decorator-based registry for scraper functions."""

import importlib
import pkgutil
from collections.abc import Callable

Scraper = Callable[[], None]
_REGISTRY: dict[str, Scraper] = {}


def register(name: str) -> Callable[[Scraper], Scraper]:
    """Return a decorator that registers a scraper under ``name``.

    Args:
        name: Short identifier used to look the scraper up later
            (e.g. ``"amf"``).

    Returns:
        A decorator that records the wrapped function in the registry and
        returns it unchanged.

    Raises:
        ValueError: If ``name`` is already registered.
    """

    def decorator(func: Scraper) -> Scraper:
        if name in _REGISTRY:
            raise ValueError(f"Scraper '{name}' is already registered.")
        _REGISTRY[name] = func
        return func

    return decorator


def list_scrapers() -> list[str]:
    """Return registered scraper names sorted alphabetically.

    Returns:
        Alphabetically sorted list of registered scraper names.
    """
    return sorted(_REGISTRY)


def get_scraper(name: str) -> Scraper:
    """Look up a scraper function by its registered name.

    Args:
        name: Registered scraper name.

    Returns:
        The scraper function previously registered under ``name``.

    Raises:
        KeyError: If no scraper is registered under ``name``.
    """
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"Unknown scraper '{name}'. Available: {', '.join(list_scrapers())}"
        ) from None


def discover() -> None:
    """Import every module under ``pipeline.scrapers`` so decorators fire."""
    from pipeline import scrapers

    for module_info in pkgutil.iter_modules(scrapers.__path__):
        importlib.import_module(f"{scrapers.__name__}.{module_info.name}")
