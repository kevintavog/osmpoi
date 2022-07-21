package osmpoi.service

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import osmpoi.indexer.PoiLevel
import osmpoi.models.OsmPoi
import osmpoi.models.OsmTagValue
import kotlin.math.exp

class PoiFilterTests {
    @ParameterizedTest
    @ValueSource(strings = [
        "Harvard University@" +
                "name=Harvard University,amenity=university,type=multipolygon|" +
                "name=Harvard University,amenity=university,type=multipolygon,wikidata=Q13371,wikipedia=en:Harvard University" +
                "@",
        "Court of the Lindaraja,Palacios Nazaríes,Alhambra@" +
                "name=Court of the Lindaraja,leisure=garden,wikipedia=en:Space Needle|" +
                "name=Palacios Nazaríes,castle_type=palace,historic=castle,wikipedia=es:Palacios nazaríes|" +
                "name=Alhambra,historic=castle,tourism=attraction,wikipedia=es:Alhambra" +
                "@",
        "Berlin Central Station@" +
                "name=Berlin Hauptbahnhof,building=train_station|" +
                "name=Berlin Central Station,building=train_station,wikipedia=de:Berlin Hauptbahnhof" +
                "@",
        "Space Needle,Seattle Center@" +
                "name=Space Needle,tourism=attraction,wikipedia=en:Space Needle|" +
                "name=Seattle Center,leisure=park,tourism=attraction,wikipedia=en:Seattle Center" +
                "@",
    ])
    fun `The correct POIs are returned`(testValue: String) {
        val triple = toPoiAndNearbyList(testValue)
        val response = PoiFilter.run(triple.second)
        val actual = response.map { it.name }
        assertEquals(triple.first, actual)
    }

    @Test
    fun `One off - the correct city, state & country names are returned`() {
        val testValue = "GBR,England,Molesey@" +
                "name=England,ref=WA,boundary=administrative,admin_level=4|" +
                "name=GBR,boundary=administrative,admin_level=2,ISO3166-1:alpha3=GBR" +
                "@" +
                "name=Thames,boundary=administrative,admin_level=8,waterway=river|" +
                "name=Molesey,boundary=administrative,admin_level=11,place=town"
        val triple = toPoiAndNearbyList(testValue)
        val response = PoiFilter.apply(triple.second, triple.third)
        val actual = listOfNotNull(response.countryName, response.stateName, response.cityName).toMutableList()
        actual += response.poiList.map { it.name }
        assertEquals(triple.first, actual)
    }

    @Test
    fun `one off validation`() {
        val testValue = "usa;name=USA,boundary=administrative,admin_level=2,ISO3166-1:alpha3=USA|" +
                "california;name=California,boundary=administrative,admin_level=4,ref=CA|" +
                "santa barbara county;name=Santa Barbara County,boundary=administrative,admin_level=6|" +
                "santa barbara;name=Santa Barbara,boundary=administrative,admin_level=8,border_type=city" +
                "@USA,CA,Santa Barbara"
        val poiList = toPoiList(testValue)
        val expected = toExpected(testValue)
        val response = PoiFilter.apply(poiList)
        val actual = listOfNotNull(response.countryName, response.stateName, response.cityName).toMutableList()
        actual += response.poiList.map { it.name }
        assertEquals(expected, actual)
    }

    @Test
    fun `nearby cities are returned`() {
        val insideValue = "usa;name=USA,boundary=administrative,admin_level=2,ISO3166-1:alpha3=USA|" +
                "california;name=California,boundary=administrative,admin_level=4,ref=CA|" +
                "santa barbara county;name=Santa Barbara County,boundary=administrative,admin_level=6" +
                "@na"
        val nearbyValue = "santa barbara;name=Santa Barbara,boundary=administrative,admin_level=8,border_type=city" +
                "@USA,CA,Santa Barbara"
        val insideList = toPoiList(insideValue)
        val nearbyList = toPoiList(nearbyValue)
        val expected = toExpected(nearbyValue)
        val response = PoiFilter.apply(insideList, nearbyList)
        val actual = listOfNotNull(response.countryName, response.stateName, response.cityName).toMutableList()
        actual += response.poiList.map { it.name }
        assertEquals(expected, actual)
    }

    private fun toExpected(testValue: String): List<String> {
        val testTokens = testValue.split("@")
        return testTokens[1].split(",")
    }

    private fun toPoiList(testValue: String): List<OsmPoi> {
        val testTokens = testValue.split("@")
        val poiTokens = testTokens[0].split("|")
        return poiTokens.map { item ->
            val tokens = item.split(";")
            var name = ""
            var poiLevel = PoiLevel.NOTABLE
            val tags = tokens[1].split(",").map { kv ->
                val kvTokens = kv.split("=")
                if (kvTokens[0] == "name") { name = kvTokens[1] }
                if (kvTokens[0] == "admin_level") { poiLevel = PoiLevel.ADMIN }
                if (kvTokens[0] == "boundary" && kvTokens[1] == "ceremonial") { poiLevel = PoiLevel.ADMIN }
                OsmTagValue(kvTokens[0], kvTokens[1])
            }
            OsmPoi(id=tokens[0], name=name, tags=tags, poiLevel = poiLevel)
        }
    }

    // Expected value (comma-separated)
    // @
    // inside list (| separated)
    //   tag list (comma-separated): name=value,two=three,four=five
    // @
    // nearby list: (same format as inside list)
    //
    // Example: USA,CA,Novato@name=Novato|name=CA|name=USA@name=Petaluma
    //   expected = "USA,CA,Novato"
    //   inside list =
    //      name=Novato
    //      name=CA
    //      name=USA
    //   nearby list =
    //      name=Petaluma
    private fun toPoiAndNearbyList(testValue: String): Triple<List<String>,List<OsmPoi>,List<OsmPoi>> {
        val testTokens = testValue.split("@")
        val expected = testTokens[0].split(",")
        val inside = poiList(testTokens[1])
        val nearby = poiList(testTokens[2])
        return Triple(expected, inside, nearby)
    }

    private fun poiList(testValue: String): List<OsmPoi> {
        if (testValue.isEmpty()) { return emptyList() }
        var count = 1
        return testValue.split("|").map { item ->
            var name = ""
            var poiLevel = PoiLevel.NOTABLE
            val tags = item.split(",").map { kv ->
                val kvTokens = kv.split("=")
                if (kvTokens[0] == "name") { name = kvTokens[1] }
                if (kvTokens[0] == "admin_level") { poiLevel = PoiLevel.ADMIN }
                if (kvTokens[0] == "boundary" && kvTokens[1] == "ceremonial") { poiLevel = PoiLevel.ADMIN }
                if (kvTokens[0] == "boundary" && kvTokens[1] == "administrative") { poiLevel = PoiLevel.ADMIN }
                OsmTagValue(kvTokens[0], kvTokens[1])
            }
            val id = "$name-$count"
            count += 1
            OsmPoi(id=id, name=name, tags=tags, poiLevel = poiLevel)
        }
    }
}
