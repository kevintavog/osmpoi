package osmpoi.indexer

import osmpoi.models.OsmItem

interface OsmItemLookup {
    fun retrieve(id: String): OsmItem?
}
