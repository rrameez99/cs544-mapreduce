"""The word-count workload used by the MapReduce demos and evaluation."""


def map(key, line):
    """Emit one count for each whitespace-separated word in an input line."""
    for word in line.split():
        yield word.lower(), 1


def combine(key, values):
    """Locally add a mapper's counts for one word before the shuffle."""
    yield key, sum(values)


def reduce(key, values):
    """Add all mapper-side counts for one word."""
    yield key, sum(values)
