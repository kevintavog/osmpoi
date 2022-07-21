GeometryCollection
    It's not supported by the wicket library
    "Error adding WKT for Guests of the Great River (relation/12600639)"


Investigating a key
    - https://wiki.openstreetmap.org/wiki/Key:historic?uselang=en
    - https://wiki.openstreetmap.org/wiki/Tag%3Ahistoric%3Dcitywalls
        - The number of nodes/ways/relations using the tag
        - A description of the tag and expected use
        - Some examples
    - https://taginfo.openstreetmap.org/tags/historic=citywalls
        - Tag info
    - https://overpass-turbo.eu/?template=key-value&key=historic&value=citywalls
        - Overpass-turbo query
    

Should accept
    "id": "node/369033357", "name": "Leisureland Airpark",

amenity:fountain
    Doesn't show up now
    If adding all is too much, then add if both this and "tourism:attraction" exist
    And/or add for ways & relations, but not nodes

node/1099460871, "name": "Seattle Institute of Oriental Medicine
    tags: "amenity:college"
    Probably should NOT keep

tourism=attraction
    Positives
    Negatives
        https://www.openstreetmap.org/relation/12431539 (Stonehenge Landscape). No obvious negative tags to filter it out
