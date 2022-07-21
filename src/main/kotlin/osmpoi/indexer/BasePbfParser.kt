package osmpoi.indexer

import crosby.binary.BinaryParser

abstract class BasePbfParser: BinaryParser() {
    fun getTags(keysList: List<Int>, valuesList: List<Int>): Map<String,String> {
        val tags = mutableMapOf<String,String>()
        for (index in keysList.indices) {
            tags[getStringById(keysList[index])] = getStringById(valuesList[index])
        }
        return tags
    }

    // Packed keys & values are a sequence per id; each key is followed by a value.
    // Ids are separated by a key of 0
    fun getDenseTags(ids: List<Long>, packedKV: List<Int>): List<Map<String,String>> {
        val allNodeTags = mutableListOf<Map<String,String>>()

        var lastId = 0L
        var idIndex = 0
        var kvIndex = 0
        var tags = mutableMapOf<String,String>()
        while (kvIndex < packedKV.size && idIndex < ids.size) {
            if (packedKV[kvIndex] == 0) {
                allNodeTags.add(tags)
                tags = mutableMapOf()
                lastId += ids[idIndex]
                idIndex += 1
                kvIndex += 1
            } else {
                tags[getStringById(packedKV[kvIndex])] = getStringById(packedKV[kvIndex + 1])
                // Move past this key & the following value and to the next key
                kvIndex += 2
            }
        }
        return allNodeTags
    }
}
