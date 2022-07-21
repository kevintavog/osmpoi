# OSM POI
This app parses the OSM POI dataset (as PBF), filtering out anything not considered a POI; see OsmTags for details.
Accepted POI are indexed to ElasticSearch.

A service provides endpoints to retrieve matching POIs


# Useful
A helpful OSM PBF parsing example: 
    https://github.com/topobyte/osm4j/blob/bad1fbcd8d5232ced66db6d1d0bcb026644aa7c1/tbo/core/src/main/java/de/topobyte/osm4j/tbo/access/ReaderUtil.java

Cities?
    https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-populated-places/
    https://batch.openaddresses.io/data
    https://simplemaps.com/data/world-cities
    https://whosonfirst.org/download/ (donwload from https://geocode.earth/data/whosonfirst)
