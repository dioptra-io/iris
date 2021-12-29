import re

from pydantic import NonNegativeInt, PositiveInt

from iris.commons.models.base import BaseModel


class Round(BaseModel):
    number: PositiveInt
    limit: NonNegativeInt
    offset: NonNegativeInt

    def __str__(self):
        return f"Round#{self.number}.{self.offset}"

    def encode(self) -> str:
        return f"{self.number}:{self.limit}:{self.offset}"

    @classmethod
    def decode(cls, encoded: str):
        if m := re.match(r".*?(\d+):(\d+):(\d+).*", encoded):
            number, limit, offset = m.groups()
            return cls(number=int(number), limit=int(limit), offset=int(offset))
        raise ValueError(f"cannot decode {encoded}")

    @property
    def min_ttl(self):
        return (self.limit * self.offset) + 1

    @property
    def max_ttl(self):
        if self.limit == 0:
            return 255
        return self.limit * (self.offset + 1)

    def next_round(self, global_max_ttl=0):
        new_round = Round(number=self.number + 1, limit=0, offset=0)
        if self.number > 1:
            # We are not in round 1
            return new_round
        if self.limit == 0:
            # The round 1 has no limit
            return new_round
        if self.limit * (self.offset + 1) >= global_max_ttl:
            # The round 1 has reached the global max ttl
            return new_round
        return Round(number=self.number, limit=self.limit, offset=self.offset + 1)
