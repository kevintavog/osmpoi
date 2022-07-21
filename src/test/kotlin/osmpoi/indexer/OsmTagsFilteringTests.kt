package osmpoi.indexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import osmpoi.models.OsmItemType


// Should probably include Relation: Monticello (11595350) tourism=attraction,historic=yes

class OsmTagsFilteringTests {
    @ParameterizedTest
    @ValueSource(strings = [
        "aeroway=aerodrome",
        "amenity=college",
        "amenity=fountain",         // At least for ways/regions, such as: way/35352476
        "amenity=library",
        "amenity=university",
        "artwork_type=sculpture,landmark=statue",
        "building=fort,castle_type=fortress,historic=castle",
        "building=temple,tourism=attraction",
        "building=train_station",
        "highway=pedestrian,place=square",
        "historic=bunker",
        "historic=castle",
        "historic=memorial",
        "historic=monument",
        "historic=wreck",
        "landuse=retail,tourism=attraction,wikipedia=w",
        "leisure=garden",
        "leisure=marina",
        "leisure=park",
        "leisure=park,name=Seattle Center,tourism=attraction,wikipedia=en:Seattle Center",
        "leisure=playground,tourism=attraction",
        "leisure=stadium",
        "man_made=bridge,tourism=attraction",
        "office=government,tourism=attraction",     // Hiram M. Chittenden Locks
        "place=square,wikipedia=foo",
        "public_transport=station,type=public_transport",
        "public_transport=stop_position",
        "railway=stop",
        "ruins=yes,wikipedia=yes",
        "tourism=attraction,attraction=big_wheel",
        "tourism=attraction,leisure=park",
        "tourism=attraction,wikipedia=wkp",
        "tourism=historical",
        "tourism=museum",
        "tourism=zoo",
    ])
    fun acceptsNotableWayTags(flatTags: String) {
        val tags = toMap(flatTags)
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assert(filtered.first.isNotEmpty())
        assertEquals(filtered.second, PoiLevel.NOTABLE)
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "admin_level=2,boundary=administrative,name=Iceland",   // A country
        "boundary=ceremonial,type=boundary,place=city,name=London",
        "boundary=administrative,place=locality,name=Monte Rio",
        "place=city",
        "place=village",
        "type=boundary,place=city",
        "type=boundary,place=locality",
        "type=boundary,place=town",
    ])
    fun `does not accept admin tags`(flatTags: String) {
        val tags = toMap(flatTags)
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assert(filtered.first.isEmpty())
        assertEquals(filtered.second, PoiLevel.ZERO)
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "building=yes,tourism=attraction",  // way/234810726 Argosy Cruises
        "tourism=attraction",
    ])
    fun acceptsOrdinaryWayTags(flatTags: String) {
        val tags = toMap(flatTags)
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assert(filtered.first.isNotEmpty())
        assertEquals(filtered.second, PoiLevel.ORDINARY)
    }

    // Some of these should discard nodes; but do consider keeping ways & relations
    @ParameterizedTest
    @ValueSource(strings = [
        "admin_level=2,name=Iceland",   // A country
        "admin_level=10,historic=castle",
        "amenity=arts_centre",
        "amenity=shelter",
        "artwork_type=community_center",
        "amenity=graveyard",
        "artwork_type=sculpture",
        "attraction=train,route=bus,type=route",
        "boundary=protected_area",
        "boundary=protected_area,leisure=nature_reserve,protected=perpetuity,wikidata=Q1502633,wikipedia=en:Tongass National Forest",
        "building=university",
        "landuse=allotments",           // For Seattle, at least some of these are p-patches
        "landuse=recreation_ground",
        "leisure=garden,garden:type=roof_garden",
        "leisure=nature_reserve",
        "man_made=bridge",
        "man_made=tower",
        "man_made=tower,historic=yes",
        "man_made=water_tower",
        "man_made=water_works",
        "natural=bay",
        "natural=beach",
        "natural=peak",
        "natural=spring",
        "place=square",
        "public_transport=stop_area",
        "public_transport=stop_position,bus=yes",
        "ruins=yes",
        "tourism=gallery",
        "tourism=roller_coaster,wikipedia=yes",
        "tourism=theme_area,wikipedia=yes",
        "waterway=dam",
    ])
    fun doesNotAcceptWayTags(flatTags: String) {
        val tags = toMap(flatTags)
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assert(filtered.first.isEmpty())
        assertEquals(filtered.second, PoiLevel.ZERO)
    }

    // Some of these should discard nodes; but do consider keeping ways & relations
    @ParameterizedTest
    @ValueSource(strings = [
        "amenity=fountain",
    ])
    fun doesNotAcceptNodeTags(flatTags: String) {
        val tags = toMap(flatTags)
        val filtered = OsmTags.filterTags(tags, OsmItemType.NODE)
        assert(filtered.first.isEmpty())
        assertEquals(filtered.second, PoiLevel.ZERO)
    }

    @Test
    fun `one off validation`() {
        val tags = toMap("amenity=shelter")
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assertEquals(PoiLevel.ZERO, filtered.second)
        assert(filtered.first.isEmpty())
    }

    @Test
    fun `removes all name tags`() {
        val tags = mapOf(
            "name" to "trail", "name:en" to "trail", "name:gb" to "trail",
            "alt_name" to "trail", "alt_name:en" to "trail",
            "alt_short_name" to "trail", "alt_short_name:en" to "trail",
            "int_name" to "trail", "int_name:en" to "trail",
            "long_name" to "trail", "long_name:en" to "trail",
            "not_official_name" to "trail", "not_official_name:en" to "trail",
            "not:official_name" to "trail", "not:official_name:en" to "trail",
            "official_name" to "trail", "official_name:en" to "trail",
            "old_name" to "trail", "old_name:en" to "trail", "old_name:be" to "trail",
            "old_short_name" to "trail", "old_short_name:en" to "trail",
            "short_name" to "trail", "short_name:en" to "trail",
            "tourism" to "artwork", "artwork_type" to "sculpture", "wikipedia" to "yup")
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assertEquals(Pair(mapOf("tourism" to "artwork", "artwork_type" to "sculpture", "wikipedia" to "yup"), PoiLevel.NOTABLE), filtered)
    }

    @Test
    fun `accepts an airport`() {
        val tags = mapOf("aeroway" to "aerodrome")
        val actual = OsmTags.filterTags(tags, OsmItemType.WAY)
        val expected = Pair(mapOf("aeroway" to "aerodrome"), PoiLevel.NOTABLE)
        assertEquals(expected, actual)
    }

    @Test
    fun `accepts a notable tower`() {
        val tags = mapOf("fubar" to "some tower", "man_made" to "tower", "wikipedia" to "yup")
        val actual = OsmTags.filterTags(tags, OsmItemType.WAY)
        val expected = Pair(mapOf("wikipedia" to "yup", "man_made" to "tower", "fubar" to "some tower"), PoiLevel.NOTABLE)
        assertEquals(expected, actual)
    }

    @Test
    fun `accepts a notable attraction`() {
        val tags = mapOf("name" to "some tower", "tourism" to "attraction", "wikipedia" to "yup")
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assertEquals(Pair(mapOf("wikipedia" to "yup", "tourism" to "attraction"), PoiLevel.NOTABLE), filtered)
    }

    @Test
    fun `accepts a notable statue`() {
        val tags = mapOf("name" to "some statue", "tourism" to "artwork", "artwork_type" to "statue", "wikipedia" to "yup")
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assertEquals(Pair(mapOf("wikipedia" to "yup", "tourism" to "artwork", "artwork_type" to "statue"), PoiLevel.NOTABLE), filtered)
    }

    @Test
    fun `accepts a notable sculpture`() {
        val tags = mapOf("name" to "some statue", "tourism" to "artwork", "artwork_type" to "sculpture", "wikipedia" to "yup")
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assertEquals(Pair(mapOf("wikipedia" to "yup", "tourism" to "artwork", "artwork_type" to "sculpture"), PoiLevel.NOTABLE), filtered)
    }

    @Test
    fun `does not accept a restaurant`() {
        val tags = mapOf("leisure" to "amusement_arcade", "amenity" to "restaurant")
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assertEquals(Pair(mapOf<String, String>(), PoiLevel.ZERO), filtered)
    }

    @Test
    fun `a sculpture is ordinary`() {
        val tags = mapOf("name" to "some statue", "tourism" to "artwork", "artwork_type" to "sculpture")
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assertEquals(Pair(mapOf("tourism" to "artwork", "artwork_type" to "sculpture"), PoiLevel.ORDINARY), filtered)
    }

    @Test
    fun `does not accept a route`() {
        val tags = mapOf("name" to "trail", "route" to "hiking", "highway" to "path", "type" to "route")
        val filtered = OsmTags.filterTags(tags, OsmItemType.WAY)
        assertEquals(Pair(mapOf<String,String>(), PoiLevel.ZERO), filtered)
    }

    private fun toMap(flat: String): Map<String,String> {
        val map = mutableMapOf<String, String>()
        flat.split(",").forEach { kv ->
            val tokens = kv.split("=")
            assert(tokens.size == 2)
            map[tokens[0].trim()] = tokens[1].trim()
        }
        return map
    }
}
