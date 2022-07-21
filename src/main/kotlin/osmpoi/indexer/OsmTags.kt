package osmpoi.indexer

import osmpoi.models.OsmItemType

enum class PoiLevel {
    ORDINARY,
    NOTABLE,
    ADMIN,
    ZERO
}

object OsmTags {
    fun filterTags(tags: Map<String, String>, type: OsmItemType): Pair<Map<String,String>, PoiLevel> {
        // Filter out undesirable items (those with certain key:value pairs)
        if (tags.filter { kv -> undesirableKeys.contains(kv.key) }.isNotEmpty()) {
            return Pair(emptyMap(), PoiLevel.ZERO)
        }
        if (tags.filter { kv -> undesirableKeysAndValues.contains("${kv.key}:${kv.value}") }.isNotEmpty()) {
            return Pair(emptyMap(), PoiLevel.ZERO)
        }

        var poiLevel = PoiLevel.ZERO
        val acceptedTags = mutableMapOf<String, String>()
        tags.forEach tagLoop@ { tg ->
            if (isAllowed(tg.key, tg.value, type)) {
                acceptedTags[tg.key] = tg.value
                poiLevel = PoiLevel.NOTABLE
            } else {
                // Some tags are acceptable if additional tags exist on the item
                val additionalKey = "${tg.key}:${tg.value}"
                if (poiLevel == PoiLevel.ZERO && ordinaryKeysAndValues.contains(additionalKey)) {
                    poiLevel = PoiLevel.ORDINARY
                }
                keysWithAdditionalTags[additionalKey]?.let { otherKey ->
                    tags[otherKey]?.let {
                        poiLevel = PoiLevel.NOTABLE
                        acceptedTags[tg.key] = tg.value
                        return@tagLoop
                    }
                }

                // Some tags require other keys with a specific value
                keysWithAdditionalKeysAndValues[additionalKey]?.let { additionalList ->
                    additionalList.forEach { dictionary ->
                        var matchCount = 0
                        dictionary.forEach { k, v ->
                            tags[k]?.let { extraTagValue ->
                                if (v == "*" || v == extraTagValue) {
                                    matchCount += 1
                                }
                            }
                        }
                        if (matchCount == dictionary.size) {
                            poiLevel = PoiLevel.NOTABLE
                            acceptedTags[tg.key] = tg.value
                            return@tagLoop
                        }
                    }
                }
            }
        }

        // Filter out undesirable items that have additional key:value pairs
        // Ensure that if another tag matches that the item is NOT thrown out
        var undesirableComboCount = 0
        acceptedTags.forEach { kv ->
            val additionalKey = "${kv.key}:${kv.value}"
            // If the tags match an undesirable key:value, we need to check if other key:value pairs exist
            undesirableKeysWithAdditionalKeysAndValues[additionalKey]?.let { extra ->
                extra.forEach { extraKv ->
                    extraKv.forEach {
                        if (tags[it.key] == it.value) {
                            undesirableComboCount += 1
                        }
                    }
                }
            }
        }

        // If the minimum tags are present, return all except the name related tags - these tags are useful for evaluation
        val filtered = tags.filter ignoredLoop@ { kv ->
            val key = kv.key.lowercase()
            if (ignoredNameKeys.contains(key)) { return@ignoredLoop false }
            val colonOffset = key.lastIndexOf(":")
            if (colonOffset > 0) {
                if (ignoredNameKeys.contains(key.substring(0, colonOffset))) { return@ignoredLoop false }
            }
            true
        }

//        val isAdmin = isAdministrative(tags)
        val notNotable = (acceptedTags.isNotEmpty() && undesirableComboCount == acceptedTags.size) ||
                (poiLevel == PoiLevel.ZERO && acceptedTags.isEmpty())
        if (notNotable) {
//            if (isAdmin) {
//                poiLevel = PoiLevel.ADMIN
//            } else {
                return Pair(emptyMap(), PoiLevel.ZERO)
//            }
        }

        return Pair(filtered, poiLevel)
    }

    fun nameFromTags(tags: Map<String, String>): String {
        var name = ""
        if (tags.containsKey("short_name") && tags["amenity"] == "university") {
            name = tags["short_name"] ?: ""
        }

        if (name.isBlank()) {
            tags["name:en"]?.let {
                name = it
            }
        }

        if (name.isBlank()) {
            tags["name"]?.let {
                name = it
            }
        }

        return name
    }

    private fun isAllowed(key: String, value: String, type: OsmItemType): Boolean {
        allKeysAndValues[key]?.let {
            if (type != OsmItemType.NODE || !keyValueDisallowsNodes.contains("$key:$value")) {
                return it.contains("*") || it.contains(value)
            }
        }
        return false
    }

//    private fun isAdministrative(tags: Map<String,String>): Boolean {
//        return when {
//            tags["boundary"] == "administrative" && tags.containsKey("admin_level") -> true
//            tags["boundary"] == "ceremonial" -> true
//            // Should these return true only if "type=boundary"?
//            //  Holds true for: London, Windsor (England), Yorktown
//            //  Not true for: Coba [place=village], Rocky Point [place=city]
//            tags["place"] == "city" -> true
//            // 'locality' by itself picks up too many small places.
//            // Perhaps it only applies to some countries or to some situations?
//            tags["boundary"] == "administrative" && tags["place"] == "locality" -> true
//            tags["type"] == "boundary" && tags["place"] == "locality" -> true
//            tags["place"] == "town" -> true
//            tags["place"] == "village" -> true
//            else -> false
//        }
//    }

    private val allKeysAndValues = mapOf(
        "aeroway" to setOf("aerodrome"),
        "amenity" to setOf("college", "fountain", "grave_yard", "library", "marketplace",
            "monastery", "public_bath", "research_institute", "university"),
        "attraction" to setOf("*"),
        "boundary" to setOf("national_park"),
        "bridge" to setOf("aqueduct"),
        "building" to setOf("train_station"),
        "geological" to setOf("palaeontological_site"),
        "historic" to setOf("aqueduct", "archaeological_site", "battlefield", "bunker", "castle",
            "cemetery", "church", "city_gate", "folly", "fort", "fountain", "landmark", "lighthouse", "manor",
            "memorial", "monastery", "monument", "palace", "ruins", "ship", "tomb", "tower", "windmill", "wreck"),
        "landuse" to setOf("cemetery"),
        "leisure" to setOf("amusement_arcade", "bowling_alley", "dog_park", "garden", "marina", "park",
            "sports_centre", "stadium", "water_park"),
        "man_made" to setOf("lighthouse", "obelisk", "windmill"),
        "natural" to setOf("volcano"),
        "public_transport" to setOf("station", "stop_position"),
        "railway" to setOf("station", "stop"),
        "tourism" to setOf("aquarium", "historical", "museum", "theme_park", "viewpoint", "zoo"),
    )

    private val keysWithAdditionalTags = mapOf(
        "amenity:place_of_worship" to "wikipedia",
        "building:cathedral" to "wikipedia",
        "man_made:bridge" to "wikipedia",
        "man_made:tower" to "wikipedia",
        "leisure:playground" to "wikipedia",
        "place:square" to "wikipedia",
        "ruins:yes" to "wikipedia",
        "tourism:artwork" to "wikipedia",
        "tourism:attraction" to "wikipedia",
        "tourism:monument" to "wikipedia",
    )

    private val keysWithAdditionalKeysAndValues = mapOf(
        "artwork_type:sculpture" to arrayOf(
            mapOf("landmark" to "*"),
        ),
        "highway:pedestrian" to arrayOf(
            mapOf("place" to "square"),
        ),
        "leisure:playground" to arrayOf(
            mapOf("tourism" to "attraction"),
        ),
        "man_made:bridge" to arrayOf(
            mapOf("tourism" to "attraction"),
        ),
        "man_made:tower" to arrayOf(
            mapOf("tower:type" to "defensive"),
        ),
        "tourism:artwork" to arrayOf(
            mapOf("artwork_type" to "architecture"),
            mapOf("artwork_type" to "statue", "wikipedia" to "*"),
            mapOf("artwork_type" to "sculpture", "wikipedia" to "*"),
        ),
        "tourism:attraction" to arrayOf(
            mapOf("building" to "temple"),
            mapOf("man_made" to "pier"),
            mapOf("office" to "government"),
        ),
    )

    // Some tags don't make sense for a node, get rid of those
    private val keyValueDisallowsNodes = setOf(
        "amenity:fountain"
    )

    private val ordinaryKeysAndValues = setOf(
        "tourism:aquarium",
        "tourism:artwork",
        "tourism:attraction",
        "tourism:giant_furniture",
        "tourism:landmark",
        "tourism:memorial",
        "tourism:monument",
        "tourism:museum",
        "tourism:observatory",
        "tourism:park",
        "tourism:ruins",
        "tourism:theme_park",
        "tourism:tower",
        "tourism:viewpoint",
        "tourism:winery",
        "tourism:zoo",
    )

    private val undesirableKeysWithAdditionalKeysAndValues = mapOf(
        "attraction:train" to arrayOf(
            mapOf("type" to "route")
        ),
        "boundary:protected_area" to arrayOf(
            mapOf("type" to "boundary"),
        ),
        "leisure:garden" to arrayOf(
            mapOf("garden:type" to "roof_garden"),
        )
    )

    private val undesirableKeys = setOf(
        "admin_level"
    )
    private val undesirableKeysAndValues = setOf(
        "amenity:restaurant",
        "bus:yes",
        "tourism:roller_coaster",
        "tourism:theme_area",
    )

    // Only the English name is useful for this application; other applications likely want various language support
    // NOTE: Not only do exact key matches get filtered out, so do keys that start with these values followed by a
    // colon, such as 'name:'
    private val ignoredNameKeys = setOf(
        "name",
        "alt_name",
        "alt_short_name",
        "int_name",
        "long_name",
        "not_official_name",
        "not:official_name",
        "official_name",
        "old_name",
        "old_proposed_name",
        "old_short_name",
        "short_name",
    )
}
