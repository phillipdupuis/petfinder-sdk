from typing import List

import pandas as pd

from petfinder.schemas import Animal


def animals_dataframe(records: List[Animal]):
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
                "number_of_photos": len(x["photos"]),
                "contact_email": x["contact"].get("email"),
                "contact_phone": x["contact"].get("phone"),
                "contact_address1": x["contact"]["address"].get("address1"),
                "contact_address2": x["contact"]["address"].get("address2"),
                "contact_city": x["contact"]["address"].get("city"),
                "contact_state": x["contact"]["address"]["state"],
                "contact_postcode": x["contact"]["address"]["postcode"],
                "contact_country": x["contact"]["address"]["country"],
                "primary_photo_small": (x.get("primary_photo_cropped") or {}).get(
                    "small"
                ),
                "primary_photo_medium": (x.get("primary_photo_cropped") or {}).get(
                    "medium"
                ),
                "primary_photo_large": (x.get("primary_photo_cropped") or {}).get(
                    "large"
                ),
                "primary_photo_full": (x.get("primary_photo_cropped") or {}).get(
                    "full"
                ),
            }
            for x in records
        ]
    )
    animals["published_at"] = pd.to_datetime(animals["published_at"])
    animals["status_changed_at"] = pd.to_datetime(animals["status_changed_at"])
    return animals


def photos_dataframe(records: List[Animal]) -> pd.DataFrame:
    photos = []
    for animal in records:
        animal_id = animal["id"]
        for photo in animal["photos"]:
            for size, url in photo.items():
                photos.append(
                    {"animal_id": animal_id, "photo_size": size, "photo_url": url}
                )
    return pd.DataFrame(photos)


def tags_dataframe(records: List[Animal]) -> pd.DataFrame:
    tags = []
    for animal in records:
        animal_id = animal["id"]
        tags.extend(
            [{"animal_id": animal_id, "tag": tag} for tag in animal.get("tags", [])]
        )
    return pd.DataFrame(tags)
