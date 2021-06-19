class PositionCounter:
    def __init__(self):
        self.added = 0
        self.dropped = 0

    @property
    def monotonic_count(self) -> int:
        return self.added

    @property
    def count(self) -> int:
        return self.added - self.dropped

    def increment(self) -> int:
        self.added += 1
        return self.added

    def decrement(self) -> int:
        self.dropped += 1
        if self.dropped > self.added:
            raise ValueError()
        return self.added - self.dropped
