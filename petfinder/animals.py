from math import ceil
from typing import List, Set, TypeVar, Literal, Union, NamedTuple, Dict, ClassVar

import pandas as pd
from pydantic.class_validators import root_validator
from pydantic.fields import Field
from pydantic.types import conint, PositiveInt

from petfinder.enums import Category, Age, Gender, Coat, Status, Size, Sort
from petfinder.pandas_utils import animals_dataframe, photos_dataframe, tags_dataframe
from petfinder.query import Query, QueryParams
from petfinder.schemas import Animal, AnimalsResponse
from petfinder.types import (
    AgeType,
    GenderType,
    CoatType,
    StatusType,
    SizeType,
    SortType,
)

T = TypeVar("T")


class PandasResults(NamedTuple):
    animals: pd.DataFrame
    photos: pd.DataFrame
    tags: pd.DataFrame


Format = Literal["pages", "records", "pandas"]
SearchResults = Union[List[AnimalsResponse], List[Animal], PandasResults]


class InvalidChoice(ValueError):
    def __init__(self, field_name, choice, valid_choices):
        super().__init__(
            f"{choice} is not a valid {field_name}; valid options are {sorted(valid_choices)}"
        )


class MissingDependency(ValueError):
    def __init__(self, attribute, dependency_name):
        super().__init__(
            f"You cannot specify {attribute} without providing a value for {dependency_name}"
        )


class MissingAnimalType(Exception):
    def __init__(self, method_call):
        super().__init__(
            (
                f"{method_call} can not be invoked until an animal type has been defined.\n"
                f"A type can be defined by chaining one of the following:\n"
                f".dogs, .puppies, .cats, .kittens, .small_furry, .birds, .rabbits, .horses, .barnyard, .scales_fins_other"
            )
        )


class AnimalQueryParams(QueryParams):
    type: Category = None
    breed: List[str] = None
    size: List[Size] = None
    gender: List[Gender] = None
    age: List[Age] = None
    color: List[str] = None
    coat: List[Coat] = None
    organization: List[str] = None
    name: str = Field(None, description="Uses partial match for filtering")
    location: str = Field(None, description="zip code, city/state, lat/long")
    distance: PositiveInt = Field(None, description="Maximum miles from given location")
    status: Status = Status.adoptable
    sort: Sort = Sort.recent
    limit: conint(gt=0, le=100) = 100
    page: conint(ge=1) = Field(1, description="Page of results to return")

    @root_validator(pre=True)
    def check_dependencies_and_categoricals(cls, values: dict) -> dict:
        """
        Check through the values for field mismatches that result in a 400 (bad request) from the api.
        """
        if values.get("location") is None:
            if values.get("distance"):
                raise MissingDependency("distance", "location")
            if values.get("sort") in (Sort.distance, Sort.reverse_distance):
                raise MissingDependency(f"'sort = {values['sort']}'", "location")

        query: AnimalsQuery = values["__query__"]

        if values.get("type") and values.get("breed"):
            valid_breeds = query.get_breeds()
            for breed in values["breed"]:
                if breed.lower() not in valid_breeds:
                    raise InvalidChoice("breed", breed, valid_breeds)

        if values.get("type") and values.get("color"):
            valid_colors = query.get_colors()
            for color in values["color"]:
                if color.lower() not in valid_colors:
                    raise InvalidChoice("color", color, valid_colors)

        return values


class AnimalsQuery(Query[AnimalsResponse]):
    params_class = AnimalQueryParams
    # Breeds and colors don't change, so we'll keep them cached as a class variable
    _cached_breeds: ClassVar[Dict[Category, Set[str]]] = {}
    _cached_colors: ClassVar[Dict[Category, Set[str]]] = {}

    @property
    def dogs(self: T) -> T:
        return self._chain(type=Category.dog)

    @property
    def puppies(self: T) -> T:
        return self._chain(type=Category.dog, age=[Age.baby])

    @property
    def cats(self: T) -> T:
        return self._chain(type=Category.cat)

    @property
    def kittens(self: T) -> T:
        return self._chain(type=Category.cat, age=[Age.baby])

    @property
    def small_furry(self: T) -> T:
        return self._chain(type=Category.small_furry)

    @property
    def birds(self: T) -> T:
        return self._chain(type=Category.bird)

    @property
    def rabbits(self: T) -> T:
        return self._chain(type=Category.rabbit)

    @property
    def horses(self: T) -> T:
        return self._chain(type=Category.horse)

    @property
    def barnyard(self: T) -> T:
        return self._chain(type=Category.barnyard)

    @property
    def scales_fins_other(self: T) -> T:
        return self._chain(type=Category.scales_fins_other)

    def filter(
        self: T,
        *,
        status: StatusType = None,
        ages: List[AgeType] = None,
        sizes: List[SizeType] = None,
        genders: List[GenderType] = None,
        breeds: List[str] = None,
        coats: List[CoatType] = None,
        colors: List[str] = None,
        organizations: List[str] = None,
        location: str = None,
        distance: int = None,
        name: str = None,
    ) -> T:
        return self._chain(
            **{
                k: v
                for k, v in (
                    ("status", status),
                    ("age", ages),
                    ("size", sizes),
                    ("gender", genders),
                    ("breed", breeds),
                    ("coat", coats),
                    ("color", colors),
                    ("organization", organizations),
                    ("location", location),
                    ("distance", distance),
                    ("name", name),
                )
                if v is not None
            }
        )

    def limit(self: T, value: int) -> T:
        return self._chain(limit=value)

    def sort_by(self: T, value: SortType) -> T:
        return self._chain(sort=value)

    def page(self: T, number: int) -> T:
        return self._chain(page=number)

    def get_breeds(self) -> Set[str]:
        """
        Returns a set of the breeds for a type of animal
        """
        t = self.params.get("type")
        if not t:
            raise MissingAnimalType(method_call=".breeds()")
        elif t not in self._cached_breeds:
            query: Query[dict] = self.new_query(path=f"types/{t}/breeds")
            self._cached_breeds[t] = set(
                x["name"].lower() for x in query.execute()["breeds"]
            )
        return self._cached_breeds[t]

    def get_colors(self) -> Set[str]:
        """
        Returns a set of the colors for a type of animal
        """
        t = self.params.get("type")
        if not t:
            raise MissingAnimalType(method_call=".colors()")
        elif t not in self._cached_colors:
            query: Query[dict] = self.new_query(path=f"types/{t}")
            self._cached_colors[t] = set(
                x.lower() for x in query.execute()["type"]["colors"]
            )
        return self._cached_colors[t]

    def get_total_count(self) -> int:
        """
        Returns the total number of animals which exist for this query.
        This is only intended to serve as a convenience function for debugging or exploring.
        """
        return self.execute()["pagination"]["total_count"]

    def get_total_pages(self) -> int:
        """
        Returns the total number of pages which exist for this query.
        """
        return self.execute()["pagination"]["total_pages"]

    async def search(
        self, *, format: Format = "records", limit: int = 100, start_page: int = 1,
    ) -> SearchResults:
        """
        Performs a search asynchronously and formats the results.
        """
        num_pages = ceil(limit / 100)
        queries = [self.page(n).limit(100) for n in range(start_page, num_pages + 1)]
        results = await self.async_batch_executor(queries)

        if format == "pages":
            return list(results)
        elif format == "records":
            return [record for page in results for record in page["animals"]][:limit]
        elif format == "pandas":
            records = [record for page in results for record in page["animals"]][:limit]
            return PandasResults(
                animals=animals_dataframe(records),
                photos=photos_dataframe(records),
                tags=tags_dataframe(records),
            )
        else:
            raise Exception(f"{format} is not a valid format")
