package osmpoi.indexer

import com.slimjars.dist.gnu.trove.list.array.TLongArrayList
import de.topobyte.osm4j.core.model.iface.EntityType
import de.topobyte.osm4j.core.model.iface.OsmNode
import de.topobyte.osm4j.core.model.iface.OsmRelation
import de.topobyte.osm4j.core.model.iface.OsmWay
import de.topobyte.osm4j.core.model.impl.Node
import de.topobyte.osm4j.core.model.impl.Relation
import de.topobyte.osm4j.core.model.impl.RelationMember
import de.topobyte.osm4j.core.model.impl.Way
import de.topobyte.osm4j.core.resolve.EntityNotFoundException
import de.topobyte.osm4j.core.resolve.OsmEntityProvider
import osmpoi.models.OsmItem
import java.io.File

object OsmItemRepository: OsmEntityProvider, OsmItemLookup {
    val failedIdFilename = "ItemLookupFailures.log"
    private var store: ItemStore = MemoryStore()
    const val maxSeekCount = 100
    var failedLookupCount = 0L
    var storeNodesInMemory = true
    val nodeIdSet = mutableSetOf<Long>()

    fun initialize(dbPath: String?, storeNodesInMemory: Boolean = true) {
        dbPath?.let { path ->
            store = RocksDbStore(path)
        }
        failedLookupCount = 0L
        nodeIdSet.clear()
        this.storeNodesInMemory = storeNodesInMemory
    }

    fun close() {
        store.close()
        nodeIdSet.clear()
    }

    fun flush() {
        store.flush()
    }

    fun storeRetained(list: List<String>) {
        store.storeRetained(list)
    }

    fun startRetrieveRetained() {
        store.startRetrieveRetained()
    }

    fun nextRetained(): Set<String> {
        return store.nextRetained()
    }

    fun store(list: List<OsmItem>) {
        store.store(list)
    }

    override fun retrieve(id: String): OsmItem? {
        val item = store.retrieve(id)
        if (item == null) {
            failedLookupCount += 1
            File(failedIdFilename).appendText("$id\n")
        }
        return item
    }

    fun storeNodeIds(list: List<Long>) {
        if (storeNodesInMemory) {
            nodeIdSet.addAll(list)
        } else {
            store.storeNodeIds(list)
        }
    }

    // Returns the nodes that are contained in the repository
    fun containsNodeIds(list: List<Long>): List<Long> {
        return if (storeNodesInMemory) {
            list.filter { nodeIdSet.contains(it) }
        } else {
            store.containsNodeIds(list)
        }
    }

    override fun getNode(id: Long): OsmNode {
        retrieve(OsmItem.nodeId(id))?.let { item ->
            return Node(id, item.latitude ?: 0.0, item.longitude ?: 0.0)
        }
        throw EntityNotFoundException("unable to find node: '$id'")
    }

    override fun getWay(id: Long): OsmWay {
        retrieve(OsmItem.wayId(id))?.let { item ->
            val nodeList = TLongArrayList()
            nodeList.addAll(item.nodeIds ?: emptyList<Long>())
            return Way(id, nodeList)
        }
        throw EntityNotFoundException("unable to find way: '$id'")
    }

    override fun getRelation(id: Long): OsmRelation {
        retrieve(OsmItem.relationId(id))?.let { item ->
            val members = mutableListOf<RelationMember>()
//            item.nodeIds?.let { nodeIds ->
//                members.addAll(nodeIds.map { RelationMember(it, EntityType.Node, "") })
//            }
            item.wayIds?.let { wayIds ->
                members.addAll(wayIds.map { RelationMember(it, EntityType.Way, "") })
            }
            item.relationIds?.let { relationIds ->
                members.addAll(relationIds.map { RelationMember(it, EntityType.Relation, "") })
            }
            return Relation(id, members)
        }
        throw EntityNotFoundException("unable to find relation: '$id'")
    }
}
