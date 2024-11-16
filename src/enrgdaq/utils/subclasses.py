def all_subclasses(cls):
    """
    Returns a set of all subclasses of a given class.

    Args:
        cls (type): The class to get subclasses of.

    Returns:
        set[type]: A set of all subclasses of the given class.
    """

    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )
