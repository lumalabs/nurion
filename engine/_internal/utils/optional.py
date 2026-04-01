"""Helpers for optional dependency handling."""


def optional_dependency_placeholder(class_name: str, extra: str):
    """Return a placeholder class that raises ImportError on instantiation or subclassing.

    Used in __init__.py when an optional dependency is not installed, so users get
    a clear error message instead of 'NoneType is not callable'.

    Usage::

        try:
            from mymodule import MyClass
        except ImportError:
            MyClass = optional_dependency_placeholder("MyClass", "myextra")
    """
    msg = (
        f"{class_name} requires the [{extra}] extra. "
        f"Install with: pip install engine[{extra}]"
    )

    class _Placeholder:
        def __init__(self, *args, **kwargs):
            raise ImportError(msg)

        def __init_subclass__(cls, **kwargs):
            raise ImportError(msg)

    _Placeholder.__name__ = class_name
    _Placeholder.__qualname__ = class_name
    return _Placeholder
