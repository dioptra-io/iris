from dataclasses import dataclass


@dataclass(frozen=True)
class Round(object):
    number: int

    limit: int
    offset: int

    def encode(self) -> str:
        return f"{self.number}:{self.limit}:{self.offset}"

    @staticmethod
    def decode(encoded: str):
        return Round(*[int(v) for v in encoded.split(":")])

    @staticmethod
    def decode_from_filename(filename: str):
        encoded = filename.split(".")[0].split("_")[-1]
        return Round(*[int(v) for v in encoded.split(":")])

    @property
    def min_ttl(self):
        return (self.limit * self.offset) + 1

    @property
    def max_ttl(self):
        if self.limit == 0:
            return 255
        return self.limit * (self.offset + 1)

    def next_round(self, global_max_ttl=0):
        new_round = Round(self.number + 1, 0, 0)
        if self.number > 1:
            # We are not in round 1
            return new_round
        if self.limit == 0:
            # The round 1 has no limit
            return new_round
        if self.limit * (self.offset + 1) >= global_max_ttl:
            # The round 1 has reached the global max ttl
            return new_round
        return Round(self.number, self.limit, self.offset + 1)
