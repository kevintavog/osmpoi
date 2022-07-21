package osmpoi.indexer

import osmpoi.models.OsmItem

interface ItemStore {
    fun close()
    fun flush()
    fun storeRetained(list: List<String>)
    fun startRetrieveRetained()
    fun nextRetained(): Set<String>
    fun store(list: List<OsmItem>)
    fun retrieve(id: String): OsmItem?
    fun storeNodeIds(list: List<Long>)
    fun containsNodeIds(list: List<Long>): List<Long>
}
