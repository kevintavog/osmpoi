package osmpoi.service

import osmpoi.indexer.PoiLevel
import osmpoi.models.OsmPoi

data class FilterResponse(
    val poiList: List<OsmPoi>,
    val countryCode: String? = null,
    val countryName: String? = null,
    val stateName: String? = null,
    val cityName: String? = null
)

object PoiFilter {
    // These apply primarily to the UK/England
    private val ignoredDesignations = setOf("civil_parish", "non_metropolitan_district", "outer_london_borough", "sui_generis")
    private val ignoredKeyValues = mapOf("waterway" to "*")

    private val criticalTags = setOf("amenity", "building")

    fun run(poiList: List<OsmPoi>): List<OsmPoi> {
        if (poiList.isEmpty()) {
            return poiList
        }

        // POIs with some (critical) matching tags and/or matching names need to be filtered out.
        // This is common with a multi-polygon relation that shares a name/tags with a single polygon
        val skipList = mutableSetOf<String>()
//        val duplicateNames = mutableListOf<Pair<OsmPoi,OsmPoi>>()
//        val duplicateTags = mutableListOf<Pair<OsmPoi,OsmPoi>>()
//        val nameMap = mutableMapOf<String,OsmPoi>()
        val tagsMap = mutableMapOf<String,OsmPoi>()
        poiList.forEach { poi ->
//            nameMap[poi.name]?.let { existingPoi ->
//                duplicateNames.add(Pair(existingPoi, poi))
//            } ?: run {
//                nameMap[poi.name] = poi
//            }
            poi.tags.forEach { tv ->
                if (criticalTags.contains(tv.key)) {
                    val combined = "${tv.key}=${tv.value}"
                    tagsMap[combined]?.let { existingPoi ->
//                        duplicateTags.add(Pair(existingPoi, poi))
                        skipList.add(if (existingPoi.area > poi.area) poi.id else existingPoi.id)
                    } ?: let {
                        tagsMap[combined] = poi
                    }
                }
            }
        }

        return if (skipList.isNotEmpty()) {
            val response = mutableListOf<OsmPoi>()
    //println("skip=$skipList")
            poiList.forEach {
                if (!skipList.contains(it.id)) {
                    response.add(it)
                }
            }
            response
        } else {
            poiList
        }
    }

    fun apply(poiList: List<OsmPoi>, nearbyPoiList: List<OsmPoi> = emptyList()): FilterResponse {
        val poiAndTagsList = poiAndTagsMap(poiList)
        // A helpful list of admin_level values used by different countries:
        // https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative#10_admin_level_values_for_specific_countries
        val countryPair = getCountry(poiAndTagsList)
        val countryCode = countryPair?.second?.get("ISO3166-1:alpha3") ?: ""
        val statePair = getState(countryCode, poiAndTagsList)
        val stateCode = getStateName(countryCode, statePair?.first, statePair?.second) ?: ""
        val cityPair = getCity(countryCode, stateCode, poiAndTagsList, nearbyPoiList)

        val locationIds = listOfNotNull(countryPair?.first?.id, statePair?.first?.id, cityPair?.first?.id)
        val filtered = poiAndTagsList
            .filter filter@ { pair ->
                if (locationIds.contains(pair.first.id)) {
                    return@filter false
                }
                if (pair.first.poiLevel == PoiLevel.ADMIN) {
                    return@filter false
                }
                true
            }
            .map { it.first }
        return FilterResponse(
            filtered,
            countryCode,
            countryPair?.first?.name,
            stateCode.ifEmpty { null },
            cityPair?.first?.name
        )
    }

    private fun getCity(
        countryCode: String,
        stateCode: String,
        poiAndTagsList: List<Pair<OsmPoi,Map<String,String>>>,
        nearbyPoiList: List<OsmPoi>): Pair<OsmPoi, Map<String,String>>? {

        val filteredList = poiAndTagsList.filter { pair ->
            var keep = true
            if (ignoredDesignations.contains(pair.second["designation"])) { keep = false }
            ignoredKeyValues.forEach { kv ->
                if (kv.value == "*" && pair.second.containsKey(kv.key)) { keep = false }
                if (pair.second[kv.key] == kv.value) { keep = false }
            }
            keep
        }

        // There are a few best first matches to check for
        val allowedCapitalValues = setOf("yes", "4")
        filteredList.forEach { pair ->
            // place=city (& permutations) is obvious
            if (pair.second["place"] == "city") {
                return pair
            }
            if (pair.second["de:place"] == "city") {
                return pair
            }
            if (pair.second["council_style"] == "city") {
                return pair
            }

            // If the state admin level indicates it's the capitol, use it
            if (allowedCapitalValues.contains(pair.second["capital"])) {
                return pair
            }
        }

        val backupCityAdminLevels = mutableListOf<String>()
        val directPlaceTypes = mutableSetOf("city", "town")
        val cityAdminLevels = when (countryCode) {
            "BEL" -> { listOf("9") }
            "CAN" -> { listOf("8", "10", "6", "5") }
            "DEU" -> { listOf("9", "8") }
            "GBR" -> {
                if (stateCode == "Scotland") {
                    listOf("8", "10")
                } else {
                    backupCityAdminLevels.add("10")
                    listOf("8")
                }
            }
            "ISL" -> { listOf("6") }
            "MEX" -> {
                directPlaceTypes.add("village")
                listOf("8", "10")
            }
            "USA" -> {
                backupCityAdminLevels.add("8")
                listOf("8", "10", "6", "5")
            }
            else -> { listOf("8") }
        }

        val placeTypesByLevel = when (countryCode) {
            "USA" -> {
                val m = mutableMapOf<String,List<String>>()
                cityAdminLevels.forEach { m[it] = listOf("city", "town") }
                m
            }
            else -> emptyMap()
        }

        val borderTypes = when (countryCode) {
//            "GBR",
            "USA" -> { listOf("city", "locality", "town") }
            else -> emptyList()
        }

        val cityPairList = mutableListOf<Pair<OsmPoi, Map<String,String>>>()
        filteredList.forEach { pair ->
            if (overriddenCityIds.contains(pair.first.id)) {
                cityPairList.add(pair)
//                return pair
            }
        }

        cityAdminLevels.forEach adminFor@ { adminLevel ->
            filteredList.forEach { pair ->
                val adminPlaceTypes = placeTypesByLevel[adminLevel] ?: emptyList()
//                pair.second["place"]?.let {
//                    if (adminPlaceTypes.contains(it)) {
//                        cityPairList.add(pair)
////                        return pair
//                    }
//                }
                if (pair.second["admin_level"] == adminLevel) {
                    if (adminPlaceTypes.isEmpty()) {
                        cityPairList.add(pair)
//                        return pair
                    } else if (pair.second["place"] != null) {
                        if (adminPlaceTypes.contains(pair.second["place"])) {
                            cityPairList.add(pair)
//                            return pair
                        }
                    }
                    pair.second["border_type"]?.let {
                        if (borderTypes.contains(it)) {
                            cityPairList.add(pair)
//                            return pair
                        }
                    }
                }
            }
        }

        // Certain types of boundaries
        val boundaryPlaceTypes = directPlaceTypes + setOf("locality")
        val cityBoundaries = setOf("administrative", "census")
        filteredList.forEach { pair ->
            if (cityBoundaries.contains(pair.second["boundary"])) {
                if (boundaryPlaceTypes.contains(pair.second["place"])) {
                    cityPairList.add(pair)
//                    return pair
                }
            }
        }

        // Place
        filteredList.forEach { pair ->
            if (directPlaceTypes.contains(pair.second["place"] ?: "")) {
                cityPairList.add(pair)
//                return pair
            }
        }

        // No matches with 'border_type' or 'place', check if there are any fallback items
        backupCityAdminLevels.forEach { adminLevel ->
            filteredList.forEach { pair ->
                if (pair.second["admin_level"] == adminLevel) {
                    cityPairList.add(pair)
//                    return pair
                }
            }
        }

println("city candidates: ${cityPairList
//    .sortedBy { it.first.area } 
    .map { "${it.first.id} ${it.first.name} area=${it.first.area} tags=${it.first.tagsAsMap()}"} }")
        if (cityPairList.isNotEmpty()) {
            return cityPairList
//                .sortedBy { it.first.area }
                .first()
        }

        // No matches - what about a nearby city?
        if (nearbyPoiList.isNotEmpty()) {
//println("nearby city check: $nearbyPoiList")
            getCity(countryCode, stateCode, poiAndTagsMap(nearbyPoiList), emptyList())?.let { pair ->
                return pair
            }
//            nearbyPoiList.forEach { item ->
//println("nearby city check: ${item.id} ${item.name}: ${item.tagsAsMap()}")
//                getCity(countryCode, stateCode, poiAndTagsMap(listOf(item)), emptyList())?.let { pair ->
//                    return pair
//                }
//            }
        }

        return null
    }

    private fun getStateName(countryCode: String, poi: OsmPoi?, tags: Map<String,String>?): String? {
        when (countryCode) {
            "USA" -> {
                val stateCode = tags?.get("ref") ?: ""
                if (stateCode.isNotEmpty()) {
                    poi?.name = stateCode
                }
            }
            else -> { }
        }
        return poi?.name
    }

    private fun getState(countryCode: String, poiAndTagsList: List<Pair<OsmPoi,Map<String,String>>>): Pair<OsmPoi, Map<String,String>>? {
        val adminLevels = when (countryCode) {
            "CAN", "GBR", "USA" -> { listOf("4")}
            else -> { emptyList() }
        }

        adminLevels.forEach { adminLevel ->
            for (pair in poiAndTagsList) {
                if (pair.second["boundary"] == "administrative" && pair.second["admin_level"] == adminLevel) {
                    return pair
                }
            }
        }

        return null
    }

    private fun getCountry(poiAndTagsList: List<Pair<OsmPoi,Map<String,String>>>): Pair<OsmPoi, Map<String,String>>? {
        for (pair in poiAndTagsList) {
            if (pair.second["boundary"] == "administrative" && pair.second["admin_level"] == "2") {
                return pair
            }
        }

        return null
    }

    private fun poiAndTagsMap(poiList: List<OsmPoi>) = poiList.map { Pair(it, it.tagsAsMap()) }

    // I don't understand how some cities are identified, this is a backup to ensure they show up correctly.
    // 2022-06-16: The admin node has "capital=yes", and when the code is in to merge that tag to the relation, this
    // will be resolved so this override can go away
    private val overriddenCityIds = setOf(
        "relation/1376330"  // Mexico City
    )
}
