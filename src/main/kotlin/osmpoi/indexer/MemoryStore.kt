package osmpoi.indexer

import osmpoi.models.OsmItem

class MemoryStore: ItemStore {
    private val items = mutableMapOf<String, OsmItem>()
    private val retainedIds = mutableSetOf<String>()
    private var retainedAsArray = mutableListOf<String>()
    private var retainedOffset = 0

    override fun close() {
    }

    override fun flush() {
    }

    override fun storeRetained(list: List<String>) {
        list.forEach { retainedIds.add(it) }
    }

    override fun startRetrieveRetained() {
        retainedOffset = 0
        retainedAsArray = retainedIds.toMutableList()
    }

    override fun nextRetained(): Set<String> {
        val response = mutableSetOf<String>()
        val end = (retainedOffset + OsmItemRepository.maxSeekCount).coerceAtMost(retainedAsArray.size - 1)
        while (retainedOffset <= end) {
            response.add(retainedAsArray[retainedOffset])
            retainedOffset += 1
        }
        return response
    }

    override fun store(list: List<OsmItem>) {
        list.forEach { items[it.id] = it }
    }

    override fun retrieve(id: String): OsmItem? {
        return items[id]
    }

    override fun storeNodeIds(list: List<Long>) {
        TODO("Not yet implemented")
    }

    override fun containsNodeIds(list: List<Long>): List<Long> {
        TODO("Not yet implemented")
    }
}
