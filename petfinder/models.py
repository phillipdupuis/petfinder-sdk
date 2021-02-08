from enum import Enum
from typing import Optional

from pydantic import BaseModel
from pydantic.class_validators import root_validator
from pydantic.types import conint


class AnimalType(str, Enum):
    dog = "dog"
    cat = "cat"
    small_furry = "small-furry"
    bird = "bird"
    scales_fins_other = "scales-fins-other"
    barnyard = "barnyard"
    rabbit = "rabbit"
    horse = "horse"


class AnimalSize(str, Enum):
    small = "small"
    medium = "medium"
    large = "large"
    extra_large = "extra-large"


class AnimalGender(str, Enum):
    male = "male"
    female = "female"
    unknown = "unknown"


class AnimalCoat(str, Enum):
    short = "short"
    medium = "medium"
    long = "long"
    wire = "wire"
    hairless = "hairless"
    curly = "curly"


class AnimalStatus(str, Enum):
    adoptable = "adoptable"
    adopted = "adopted"
    found = "found"


class AnimalAge(str, Enum):
    baby = "baby"
    young = "young"
    adult = "adult"
    senior = "senior"


class SortType(str, Enum):
    recent = "recent"
    random = "random"


class AnimalQuery(BaseModel):
    type: Optional[AnimalType] = None
    age: Optional[AnimalAge] = None
    size: Optional[AnimalSize] = None
    gender: Optional[AnimalGender] = None
    breed: Optional[str] = None
    name: Optional[str] = None
    zip_code: Optional[str] = None
    distance: Optional[int] = None
    status: AnimalStatus = AnimalStatus.adoptable
    limit: conint(gt=0, le=100) = 100
    sort: SortType = SortType.random

    class Config(BaseModel.Config):
        use_enum_values = True

    def dict(self, *args, **kwargs):
        d = super().dict(exclude_none=True, by_alias=True)
        if "zip_code" in d:
            d["location"] = d.pop("zip_code")
        return d

    @root_validator
    def check_dependencies(cls, values: dict) -> dict:
        """
        Check through the values for field mismatches that result in a 400 (bad request) from the api.
        """
        if values.get("distance") and (values.get("zip_code") is None):
            raise Exception(
                "You cannot specify 'distance' without first providing a value for 'zip_code'"
            )
        return values
