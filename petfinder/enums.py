from enum import Enum


class Age(str, Enum):
    baby = "baby"
    young = "young"
    adult = "adult"
    senior = "senior"


class Category(str, Enum):
    dog = "dog"
    cat = "cat"
    small_furry = "small-furry"
    bird = "bird"
    scales_fins_other = "scales-fins-other"
    barnyard = "barnyard"
    rabbit = "rabbit"
    horse = "horse"


class Coat(str, Enum):
    short = "short"
    medium = "medium"
    long = "long"
    wire = "wire"
    hairless = "hairless"
    curly = "curly"


class Gender(str, Enum):
    male = "male"
    female = "female"
    unknown = "unknown"


class Size(str, Enum):
    small = "small"
    medium = "medium"
    large = "large"
    extra_large = "extra-large"


class Sort(str, Enum):
    recent = "recent"
    random = "random"
    distance = "distance"
    reverse_recent = "-recent"
    reverse_distance = "-distance"


class Status(str, Enum):
    adoptable = "adoptable"
    adopted = "adopted"
    found = "found"
