package osmpoi.indexer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import osmpoi.models.GeoPoint
import osmpoi.models.WofEntity
import osmpoi.service.CityAndDistance
import osmpoi.service.CitySelector

class NearbyCitySelectorTests {
    @Test
    fun `one off selection`() {
        val nearby = listOf(
            CityAndDistance(
                WofEntity(101730169, "Napavine", "locality", GeoPoint(0.0, 0.0), 6169906.374775, 1766, 3),
                1.0),
            CityAndDistance(
                WofEntity(101730175, "Chehalis", "locality", GeoPoint(0.0, 0.0), 14354228.44771, 7259, 5),
                1.0),
        )

        val city = CitySelector.nearbyCity(null, null, nearby)
        assertEquals("Napavine", city?.name)
    }

    @Test
    fun `Salisbury is returned when closer to Mere`() {
        val nearby = listOf(
            CityAndDistance(
                WofEntity(404432357, "Mere", "localadmin", GeoPoint(0.0, 0.0), 24433052.417819, null, null),
                1.0),
            CityAndDistance(
                WofEntity(101852713, "Salisbury", "locality", GeoPoint(0.0, 0.0), 18429466.289431, 45600, 7),
                1.0),
        )

        val city = CitySelector.nearbyCity(null, null, nearby)
        assertEquals("Salisbury", city?.name)
    }

    @Test
    fun `Napavine is returned when close to Napavine`() {
        val nearby = listOf(
            CityAndDistance(
                WofEntity(101730169, "Napavine", "locality", GeoPoint(0.0, 0.0), 6169906.374775, 1766, 3),
                1234.0),
            CityAndDistance(
                WofEntity(101730175, "Chehalis", "locality", GeoPoint(0.0, 0.0), 14354228.44771, 7259, 5),
                9112.0),
            CityAndDistance(
                WofEntity(101730179, "Centralia", "locality", GeoPoint(0.0, 0.0), 14354228.44771, 16336, 6),
                14980.0),
        )

        val city = CitySelector.nearbyCity(null, null, nearby)
        assertEquals("Napavine", city?.name)
    }

    @Test
    fun `Walton-on-thames is returned when just outside London`() {
        val nearby = listOf(
            CityAndDistance(
                WofEntity(1126060415, "Walton-on-thames", "locality", GeoPoint(0.0, 0.0), 61910142.0, 22834, 7),
                5274.0),
            CityAndDistance(
                WofEntity(1126029899, "Epsom", "locality", GeoPoint(0.0, 0.0), 33967590.0, 27065, 7),
                8700.0),
            CityAndDistance(
                WofEntity(1125930819, "Wisley", "locality", GeoPoint(0.0, 0.0), 4014673.0, 0, 0),
                12471.0),
            CityAndDistance(
                WofEntity(1125901469, "Redhill", "locality", GeoPoint(0.0, 0.0), 38437268.0, 51559, 8),
                18985.0),
            CityAndDistance(
                WofEntity(101750367, "London", "locality", GeoPoint(0.0, 0.0), 1589261266.0, 7556900, 13),
                19366.0),
        )

        val city = CitySelector.nearbyCity(null, null, nearby)
        assertEquals("Walton-on-thames", city?.name)
    }

    @Test
    fun `Redmond is returned when in Marymoor Park`() {
        val nearby = listOf(
            CityAndDistance(
                WofEntity(101730409, "Redmond", "locality", GeoPoint(0.0, 0.0), 1.0, 54144, 8),
                1184.0),
            CityAndDistance(
                WofEntity(101730427, "Kirkland", "locality", GeoPoint(0.0, 0.0), 1.0, 48787, 7),
                6149.0),
            CityAndDistance(
                WofEntity(101730457, "Bellevue", "locality", GeoPoint(0.0, 0.0), 1.0, 122363, 9),
                8252.0),
            CityAndDistance(
                WofEntity(101730401, "Seattle", "locality", GeoPoint(0.0, 0.0), 1.0, 608660, 11),
                17145.0),
        )

        val city = CitySelector.nearbyCity(null, null, nearby)
        assertEquals("Redmond", city?.name)
    }

    @Test
    fun `Friday Harbor is returned`() {
        val nearby = listOf(
            CityAndDistance(
                WofEntity(101730169, "Friday Harbor", "locality", GeoPoint(0.0, 0.0),
                    6169906.374775, 2162, 4, countryCode = "USA", stateCode = "WA"),
                6510.0),
            CityAndDistance(
                WofEntity(101730175, "Oak Bay", "locality", GeoPoint(0.0, 0.0),
                    14354228.44771, 18015, 6, countryCode = "CAN", stateCode = "BC"),
                15010.0),
        )

        val city = CitySelector.nearbyCity("USA", "WA", nearby)
        assertEquals("Friday Harbor", city?.name)
    }

    @Test
    fun `The country is  used to differentiate similar choices`() {
        val nearby = listOf(
            CityAndDistance(
                WofEntity(101730169, "Friday Harbor", "locality", GeoPoint(0.0, 0.0),
                    6169906.374775, 2162, 4, countryCode = "USA", stateCode = "WA"),
                6510.0),
            CityAndDistance(
                WofEntity(101730175, "Oak Bay", "locality", GeoPoint(0.0, 0.0),
                    14354228.44771, 2162, 6, countryCode = "CAN", stateCode = "BC"),
                15010.0),
        )

        assertEquals("Friday Harbor", CitySelector.nearbyCity("USA", "WA", nearby)?.name)
        assertEquals("Oak Bay", CitySelector.nearbyCity("CAN", "BC", nearby)?.name)
    }

    @Test
    fun `null is returned when there are no items in list`() {
        val city = CitySelector.nearbyCity(null, null, emptyList())
        assertEquals(null, city?.name)
    }
}
