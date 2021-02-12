from typing import Set, Dict

from pydantic.main import BaseModel

from petfinder.enums import Category
from petfinder.types import CategoryType


class CategoryData(BaseModel):
    breeds: Set[str]
    coats: Set[str]
    colors: Set[str]
    genders: Set[str]


class StaticData:
    _store: Dict[Category, CategoryData]

    def __init__(self, data: Dict[Category, CategoryData]):
        self._store = data

    def get_breeds(self, k: CategoryType) -> Set[str]:
        return self._store[k].breeds

    def get_coats(self, k: CategoryType) -> Set[str]:
        return self._store[k].coats

    def get_colors(self, k: CategoryType) -> Set[str]:
        return self._store[k].colors

    def get_genders(self, k: CategoryType) -> Set[str]:
        return self._store[k].genders
