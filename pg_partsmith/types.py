"""Common type aliases for partitioning."""

from typing import Annotated

from pydantic import Field, StringConstraints

PositiveInt = Annotated[int, Field(gt=0)]
NonNegativeInt = Annotated[int, Field(ge=0)]
StrippedNonEmptyStr = Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
