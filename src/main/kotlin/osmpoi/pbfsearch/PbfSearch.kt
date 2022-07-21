package osmpoi.pbfsearch

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import crosby.binary.Osmformat
import crosby.binary.file.BlockInputStream
import mu.KotlinLogging
import osmpoi.indexer.BasePbfParser
import osmpoi.indexer.OsmTags
import osmpoi.models.OsmItem
import osmpoi.models.OsmItemType
import java.io.File
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

class PbfSearchMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                PbfSearch().main(args)
                exitProcess(0)
            } catch (t: Throwable) {
                t.printStackTrace()
                exitProcess(-1)
            }
        }
    }
}

class PbfSearch: CliktCommand() {
    private val inputFolder: String by option(
        "-p",
        "--pbfFolder",
        help = "The 'osm.pbf' file or the  folder with the those file(s) to search")
        .required()

    private val id: String by option(
        "-s",
        "--search",
        help = "The text to search for (id and name are searched)")
        .required()

    override fun run() {
        logger.info { "Searching $inputFolder" }

        File(inputFolder).walk().sortedBy { it.name.lowercase() }.forEach { file ->
            if (file.isFile && file.extension == "pbf") {
                BlockInputStream(File(file.absolutePath).inputStream(), OsmPbfSearcher(id)).process()
            }
        }
    }
}

class OsmPbfSearcher(val searchText: String): BasePbfParser() {
    private fun printHit(id: String, allTags: Map<String, String>) {
        val filteredTags = OsmTags.filterTags(allTags, OsmItemType.RELATION)
        val name = OsmTags.nameFromTags(allTags)
        println("Found '$searchText': $id, $name \n -> filtered tags=$filteredTags")
    }

    override fun complete() {
    }

    override fun parse(header: Osmformat.HeaderBlock?) {
    }

    override fun parseRelations(rels: MutableList<Osmformat.Relation>?) {
        rels?.let { relationsList ->
            relationsList.forEach { relation ->
                val id = OsmItem.relationId(relation.id)
                val allTags = getTags(relation.keysList, relation.valsList)
                val name = OsmTags.nameFromTags(allTags)
                if (id.contains(searchText) || name.contains(searchText, true)) {
                    printHit(id, allTags)
                }
            }
        }
    }

    override fun parseDense(nodes: Osmformat.DenseNodes?) {
        nodes?.let { dn ->
            var lastId = 0L
            val allDenseTags = getDenseTags(dn.idList, dn.keysValsList)
            for (index in 0 until dn.idCount) {
                lastId += dn.getId(index)
                val id = OsmItem.nodeId(lastId)
                val allTags = allDenseTags[index]
                val name = OsmTags.nameFromTags(allTags)
                if (id.contains(searchText) || name.contains(searchText, true)) {
                    printHit(id, allTags)
                }
            }
        }
    }

    override fun parseNodes(nodes: MutableList<Osmformat.Node>?) {
    }

    override fun parseWays(ways: MutableList<Osmformat.Way>?) {
        ways?.let { waysList ->
            waysList.forEach { way ->
                val id = OsmItem.wayId(way.id)
                val allTags = getTags(way.keysList, way.valsList)
                val name = OsmTags.nameFromTags(allTags)
                if (id.contains(searchText) || name.contains(searchText, true)) {
                    printHit(id, allTags)
                }
            }
        }
    }
}
