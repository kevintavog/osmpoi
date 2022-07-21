package osmpoi.indexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class OsmTagsNameTests {
    @Test
    fun `uses the native name if no  other names exist`() {
        val tags = mapOf("name" to "native")
        val actual = OsmTags.nameFromTags(tags)
        assertEquals("native", actual)
    }

    @Test
    fun `uses the english name rather than the native name`() {
        val tags = mapOf("name" to "native", "name:en" to "english")
        val actual = OsmTags.nameFromTags(tags)
        assertEquals("english", actual)
    }

    @Test
    fun `uses the short name rather than the native or english name`() {
        val tags = mapOf("name" to "native", "name:en" to "english", "short_name" to "short", "amenity" to "university")
        val actual = OsmTags.nameFromTags(tags)
        assertEquals("short", actual)
    }

    @Test
    fun `uses the english name rather than the short name for most items`() {
        val tags = mapOf("name" to "native", "name:en" to "english", "short_name" to "short")
        val actual = OsmTags.nameFromTags(tags)
        assertEquals("english", actual)
    }
}
