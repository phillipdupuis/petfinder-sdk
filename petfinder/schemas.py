from typing import Optional, List

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


class Link(TypedDict):
    href: str


class PrevNextLinks(TypedDict):
    previous: Link
    next: Link


class Pagination(TypedDict):
    count_per_page: int
    total_count: int
    current_page: int
    total_pages: int
    _links: Optional[PrevNextLinks]


class Breeds(TypedDict):
    primary: Optional[str]
    secondary: Optional[str]
    mixed: Optional[bool]
    unknown: Optional[bool]


class Colors(TypedDict):
    primary: Optional[str]
    secondary: Optional[str]
    tertiary: Optional[str]


class Attributes(TypedDict):
    spayed_neutered: bool
    house_trained: bool
    declawed: Optional[bool]
    special_needs: bool
    shots_current: bool


class Environment(TypedDict):
    children: Optional[bool]
    dogs: Optional[bool]
    cats: Optional[bool]


class Photo(TypedDict):
    small: str
    medium: str
    large: str
    full: str


class Address(TypedDict):
    address1: Optional[str]
    address2: Optional[str]
    city: Optional[str]
    state: str
    postcode: str
    country: str


class Contact(TypedDict):
    email: Optional[str]
    phone: Optional[str]
    address: Address


class AnimalLinks(TypedDict):
    self: Link
    type: Link
    organization: Link


class Animal(TypedDict):
    id: int
    organization_id: str
    url: str
    type: str
    species: Optional[str]
    breeds: Breeds
    colors: Colors
    age: Optional[str]
    gender: Optional[str]
    size: Optional[str]
    coat: Optional[str]
    attributes: Attributes
    environment: Environment
    tags: List[str]
    name: str
    description: Optional[str]
    photos: List[Photo]
    primary_photo_cropped: Optional[Photo]
    status: str
    published_at: Optional[str]
    status_changed_at: Optional[str]
    contact: Contact
    _links: AnimalLinks


class AnimalsResponse(TypedDict):
    animals: List[Animal]
    pagination: Pagination
