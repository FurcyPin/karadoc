import time


class Stopwatch:
    """Provides a timer that can be used with a context manager.

    Example:
    > stopwatch = Stopwatch()
    > time.sleep(0.1)
    > print("%0f seconds" % stopwatch.duration_in_seconds)
    0.1 seconds

    """

    def __init__(self) -> None:
        self.start_time = time.time()

    @property
    def duration_in_seconds(self) -> float:
        return round(time.time() - self.start_time, 3)

    @property
    def duration_str(self) -> str:
        return "%0.3f s" % self.duration_in_seconds
