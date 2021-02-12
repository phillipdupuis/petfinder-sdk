import httpx
from typing import Union, Awaitable, TypeVar
from typing_extensions import Literal
from petfinder import enums

T = TypeVar("T")

MaybeAwaitable = Union[T, Awaitable[T]]

HttpClient = TypeVar("HttpClient", httpx.Client, httpx.AsyncClient)

# TODO: Are we sure pycharm can't do type-checking for enums properly?
# Will need to add some tests to make sure the types and the enums are always in sync.

AgeType = Union[enums.Age, Literal["baby", "young", "adult", "senior"]]

CategoryType = Union[
    enums.Category,
    Literal[
        "dog",
        "cat",
        "small-furry",
        "bird",
        "scales-fins-other",
        "barnyard",
        "rabbit",
        "horse",
    ],
]

CoatType = Union[
    enums.Coat, Literal["short", "medium", "long", "wire", "hairless", "curly"]
]

GenderType = Union[enums.Gender, Literal["male", "female", "unknown"]]

SizeType = Union[enums.Size, Literal["small", "medium", "large", "extra-large"]]

SortType = Union[
    enums.Sort, Literal["recent", "random", "distance", "-recent", "-distance"]
]

StatusType = Union[enums.Status, Literal["adoptable", "adopted", "found"]]
