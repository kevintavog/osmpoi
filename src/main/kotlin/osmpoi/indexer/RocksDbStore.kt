package osmpoi.indexer

import kotlinx.serialization.json.Json
import maryk.rocksdb.AutoCloseable
import maryk.rocksdb.RocksDB
import maryk.rocksdb.destroyRocksDB
import maryk.rocksdb.openRocksDB
import mu.KotlinLogging
import org.rocksdb.*
import osmpoi.models.OsmItem


private val logger = KotlinLogging.logger {}
class RocksDbStore(private val dbPath: String) : ItemStore, AutoCloseable {
    private val batchSize = 8_000
    private var rocksDb: RocksDB
    private var rocksIterator: RocksIterator? = null
    private val rocksItemPrefix = "item:"
    private val rocksNodeIdPrefix = "id:"
    private val rocksRetainedPrefix = "retained:"
    private val pendingItems = mutableListOf<OsmItem>()
    private val pendingRetainedIds = mutableListOf<String>()
    private val pendingNodeIds = mutableListOf<Long>()


    init {
        val dbOptions = Options()
        destroyRocksDB(dbPath, dbOptions)
        dbOptions.setCreateIfMissing(true)
        rocksDb = openRocksDB(dbOptions, dbPath)
    }

    override fun close() {
        rocksDb.cancelAllBackgroundWork(true)
        rocksDb.closeE()
        destroyRocksDB(dbPath, Options())
    }

    override fun flush() {
        writeItems(pendingItems)
        pendingItems.clear()
        writeRetainedIds(pendingRetainedIds)
        pendingRetainedIds.clear()
        writeNodeIds(pendingNodeIds)
        pendingNodeIds.clear()
    }

    override fun storeRetained(list: List<String>) {
        pendingRetainedIds.addAll(list)
        if (pendingRetainedIds.size >= batchSize) {
            writeRetainedIds(pendingRetainedIds)
            pendingRetainedIds.clear()
        }
    }

    override fun startRetrieveRetained() {
        val readOptions = ReadOptions()
        readOptions.setAutoPrefixMode(true)
        rocksIterator = rocksDb.newIterator(readOptions)
        rocksIterator?.seek(rocksRetainedPrefix.toByteArray())
    }

    override fun nextRetained(): Set<String> {
        val response = mutableSetOf<String>()
        var count = OsmItemRepository.maxSeekCount
        rocksIterator?.let { iterator ->
            while (iterator.isValid && count > 0) {
                response.add(iterator.value().decodeToString())
                iterator.next()
                count -= 1
            }
            if (!iterator.isValid) {
                rocksIterator!!.close()
                rocksIterator = null
            }
        }
        return response
    }

    override fun store(list: List<OsmItem>) {
        pendingItems.addAll(list)
        if (pendingItems.size >= batchSize) {
            writeItems(pendingItems)
            pendingItems.clear()
        }
    }

    override fun storeNodeIds(list: List<Long>) {
        pendingNodeIds.addAll(list)
        if (pendingNodeIds.size >= batchSize) {
            writeNodeIds(pendingNodeIds)
            pendingNodeIds.clear()
        }
    }

    override fun containsNodeIds(list: List<Long>): List<Long> {
        val matches = rocksDb.multiGetAsList(list.map { "${rocksNodeIdPrefix}${it}".toByteArray() })
        val contained = mutableListOf<Long>()
        matches.forEachIndexed { index, bytes ->
            if (bytes != null) {
                contained.add(list[index])
            }
        }
        return contained
    }

    private fun writeNodeIds(list: List<Long>) {
        if (list.isEmpty()) { return }

        val empty = ByteArray(0)
        WriteBatch().use { batch ->
            list.forEach { id ->
                val data = "${rocksNodeIdPrefix}${id}".toByteArray()
                batch.put(data, empty)
            }
            val options = WriteOptions()
            options.setDisableWAL(true)
            options.setSync(false)
            options.setNoSlowdown(false)
            rocksDb.write(options, batch)
        }
    }

    private fun writeItems(items: List<OsmItem>) {
        if (items.isEmpty()) { return }

        WriteBatch().use { batch ->
            items.forEach { item ->
                batch.put("$rocksItemPrefix${item.id}".toByteArray(), Json.encodeToString(OsmItem.serializer(), item).toByteArray())
            }
            val options = WriteOptions()
            options.setDisableWAL(true)
            options.setSync(false)
            options.setNoSlowdown(false)
            rocksDb.write(options, batch)
        }
    }

    private fun writeRetainedIds(ids: List<String>) {
        if (ids.isEmpty()) { return }

        WriteBatch().use { batch ->
            ids.forEach { id ->
                batch.put("$rocksRetainedPrefix${id}".toByteArray(), id.toByteArray())
            }
            val options = WriteOptions()
            options.setDisableWAL(true)
            options.setSync(false)
            options.setNoSlowdown(false)
            rocksDb.write(options, batch)
        }
    }

    override fun retrieve(id: String): OsmItem? {
        if (id.isEmpty()) {
            logger.error { "Retrieving invalid id '$id'" }
        }
        val itemId = "$rocksItemPrefix$id"
        val storedItem = rocksDb.get(itemId.toByteArray())
        if (storedItem != null) {
            return Json.decodeFromString(OsmItem.serializer(), storedItem.decodeToString())
        }
        return null
    }

/*
    private fun serializeItem(item: OsmItem): ByteArray {
        val bStream = ByteArrayOutputStream()
        ObjectOutputStream(bStream).use { oo ->
            oo.writeObject(item)
        }
        return bStream.toByteArray()
    }

    private fun deserializeItem(bytes: ByteArray): OsmItem {
        ObjectInputStream(ByteArrayInputStream(bytes)).use {
            return it.readObject() as OsmItem
        }
    }
*/
}
