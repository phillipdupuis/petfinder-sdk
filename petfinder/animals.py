import json
from typing import List, Callable, Set

from pydantic.class_validators import root_validator
from pydantic.fields import Field
from pydantic.types import conint, PositiveInt
import pandas as pd

from petfinder.enums import Category, Age, Gender, Coat, Status, Size, Sort
from petfinder.query import Query, QueryParams
from petfinder.schemas import Animal, AnimalsResponse
from petfinder.static_data import StaticData
from petfinder.types import (
    MaybeAwaitable,
    AgeType,
    GenderType,
    CoatType,
    StatusType,
    SizeType,
    SortType,
)


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
                f".dogs, .puppies, .cats, .kittens, .birds, .rabbits, .horses, .barnyard, .scales_fins_other"
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

        sd: StaticData = values.pop("__static_data__")

        if values.get("type"):
            type_ = values["type"]

            if values.get("breed"):
                valid_breeds = sd.get_breeds(type_)
                for breed in values["breed"]:
                    if breed.lower() not in valid_breeds:
                        raise InvalidChoice("breed", breed, valid_breeds)

            if values.get("color"):
                valid_colors = sd.get_colors(type_)
                for color in values["color"]:
                    if color.lower() not in valid_colors:
                        raise InvalidChoice("color", color, valid_colors)

        return values


class AnimalsQuery(Query[AnimalsResponse]):
    params_class = AnimalQueryParams
    search: Callable[[], MaybeAwaitable[List[Animal]]]

    def __init__(self, **kwargs):
        kwargs.setdefault("path", "animals")
        super().__init__(**kwargs)
        self.search = self._search_async if self._async else self._search

    @property
    def dogs(self):
        return self._chain(type=Category.dog)

    @property
    def puppies(self):
        return self._chain(type=Category.dog, age=Age.baby)

    @property
    def cats(self):
        return self._chain(type=Category.cat)

    @property
    def kittens(self):
        return self._chain(type=Category.cat, age=Age.baby)

    @property
    def birds(self):
        return self._chain(type=Category.bird)

    @property
    def rabbits(self):
        return self._chain(type=Category.rabbit)

    @property
    def horses(self):
        return self._chain(type=Category.horse)

    @property
    def barnyard(self):
        return self._chain(type=Category.barnyard)

    @property
    def scales_fins_other(self):
        return self._chain(type=Category.scales_fins_other)

    def filter(
        self,
        *,
        status: StatusType = None,
        age: List[AgeType] = None,
        size: List[SizeType] = None,
        gender: List[GenderType] = None,
        breed: List[str] = None,
        coat: List[CoatType] = None,
        color: List[str] = None,
        organization: List[str] = None,
        location: str = None,
        distance: int = None,
        name: str = None,
    ):
        return self._chain(
            **{
                k: v
                for k, v in (
                    ("status", status),
                    ("age", age),
                    ("size", size),
                    ("gender", gender),
                    ("breed", breed),
                    ("coat", coat),
                    ("color", color),
                    ("organization", organization),
                    ("location", location),
                    ("distance", distance),
                    ("name", name),
                )
                if v is not None
            }
        )

    def limit(self, value: int) -> "AnimalsQuery":
        return self._chain(limit=value)

    def sort_by(self, value: SortType) -> "AnimalsQuery":
        return self._chain(sort=value)

    def page(self, number: int) -> "AnimalsQuery":
        return self._chain(page=number)

    def breeds(self) -> Set[str]:
        if not self.params.get("type"):
            raise MissingAnimalType(method_call=".breeds()")
        return self._static_data.get_breeds(self.params["type"])

    def colors(self) -> Set[str]:
        if not self.params.get("type"):
            raise MissingAnimalType(method_call=".colors()")
        return self._static_data.get_colors(self.params["type"])

    def _format_search_results(self, results: AnimalsResponse) -> List[Animal]:
        # animal = results["animals"][0]
        return results["animals"]

    def _search(self) -> List[Animal]:
        return self._format_search_results(self.execute())

    async def _search_async(self) -> List[Animal]:
        return self._format_search_results(await self.execute())

    async def big_dump(self):
        records = []

        for n in range(1, 200):
            print(f"Page {n}")
            data = await self.page(n).execute()
            records.extend(data["animals"])

        path = f"/Users/phillipdupuis/repos/petfinder-client/response_data/BIG_DUMP_2021_02_11.json"
        with open(path, "w") as f:
            json.dump(records, f)
        print("SAVED!")


# class Animal(TypedDict):
# id: int
# organization_id: str
# url: str
# type: str
# species: Optional[str]
# breeds: Breeds
# colors: Colors
# age: Optional[str]
# gender: Optional[str]
# size: Optional[str]
# coat: Optional[str]
# attributes: Attributes
# environment: Environmen
# name: str
# description: Optional[str]

# tags: List[str]
# photos: List[Photo]
# primary_photo_cropped: Optional[Photo]
# contact: Contact
# _links: AnimalLinks


def _animals_dataframe(records: List[Animal]):
    animals = pd.DataFrame(
        [
            {
                "id": x["id"],
                "name": x["name"],
                "type": x["type"],
                "status": x["status"],
                "organization_id": x["organization_id"],
                "species": x.get("species"),
                "age": x.get("age"),
                "gender": x.get("gender"),
                "size": x.get("size"),
                "coat": x.get("coat"),
                "published_at": x.get("published_at"),
                "status_changed_at": x.get("status_changed_at"),
                "breed_primary": x["breeds"].get("primary"),
                "breed_secondary": x["breeds"].get("secondary"),
                "breed_mixed": x["breeds"].get("mixed"),
                "breed_unknown": x["breeds"].get("unknown"),
                "color_primary": x["colors"].get("primary"),
                "color_secondary": x["colors"].get("secondary"),
                "color_tertiary": x["colors"].get("tertiary"),
                "spayed_neutered": x["attributes"]["spayed_neutered"],
                "house_trained": x["attributes"]["house_trained"],
                "special_needs": x["attributes"]["special_needs"],
                "shots_current": x["attributes"]["shots_current"],
                "declawed": x["attributes"].get("declawed"),
                "good_with_children": x["environment"].get("children"),
                "good_with_dogs": x["environment"].get("dogs"),
                "good_with_cats": x["environment"].get("cats"),
                "description": x.get("description"),
                "url": x["url"],
            }
            for x in records
        ]
    )
    animals["id"] = animals["id"].astype(int)
    animals["published_at"] = pd.to_datetime(animals["published_at"])
    animals["status_changed_at"] = pd.to_datetime(animals["status_changed_at"])
    return animals


# def _tags_dataframe(records: List[Animal]):
#     pass
#
#
# def _
