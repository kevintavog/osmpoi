package osmpoi.service

import osmpoi.models.WofEntity
import java.util.*
import kotlin.math.PI
import kotlin.math.sqrt

data class CityAndDistance(
    val entity: WofEntity,
    val distance: Double
)

data class CityResponse(
    val countryCode: String? = null,
    val countryName: String? = null,
    val stateName: String? = null,
    val cityName: String? = null
)

object CitySelector {
    private val countryCodeToName: Map<String,String> by lazy {
        val map = mutableMapOf<String,String>()
        Locale.getISOCountries().forEach {
           val countryLocale = Locale("", it)
            map[countryLocale.isO3Country] = countryLocale.displayCountry
        }
        map
    }

    private val overiddenCityNames = mapOf(
        "Urban Honolulu" to "Honolulu"
    )

    fun apply(wofList: List<WofEntity>, nearbyWofList: List<CityAndDistance> = emptyList()): CityResponse {
        var country: WofEntity? = null
        var stateRegion: WofEntity? = null
        var stateMacroRegion: WofEntity? = null
        var city: WofEntity? = null

//println("inside=${wofList.filter { it.placeType == "locality" }.map { "${it.id} ${it.name}" } } nearby=${nearbyWofList.map { "${it.entity.id} ${it.entity.name} ${it.distance.toInt()}" } }")
        wofList.forEach { item ->
            when(item.placeType) {
                "country" -> { country = item }
                "region" -> { stateRegion = item }
                "macroregion" -> { stateMacroRegion = item }
                "locality" -> { city = item }
                else -> { println("Unexpected Wof placeType: ${item.placeType} (${item.id} ${item.name})")}
            }
        }

        var countryCode = countryCode(country, city)
        var countryName = countryName(country, countryCode)

        val state = when(countryCode) {
            "GBR" -> {
                stateMacroRegion
            }
            else -> {
                stateRegion
            }
        }
        var stateName = stateName(countryCode, state, city)

        val cityName = overrideCityName(when(city) {
            null -> {
                val nearbyCity = nearbyCity(country?.countryCode, state?.stateCode, nearbyWofList)
                // A lookup outside a country but near a city will have no matches for 'country' or 'state'
                // Fill on those value here
                if (countryCode == null) {
                    countryCode = countryCode(country, nearbyCity)
                    countryName = countryName(country, countryCode)
                    stateName = stateName(countryCode, state, nearbyCity)
                }
                nearbyCity?.name
            }
            else -> {
                city?.name
            }
        })

        return CityResponse(
            countryCode,
            countryName,
            stateName,
            cityName
        )
    }

    private fun overrideCityName(name: String?): String? {
        name?.let {
            return overiddenCityNames[name] ?: name
        }
        return name
    }

    private fun stateName(countryCode: String?, state: WofEntity?, city: WofEntity?): String? {
        return when(countryCode) {
            "GBR" -> {
                state?.name
            }
            "CAN", "USA" -> {
                state?.stateCode ?: city?.stateCode
            }
            else -> {
                null
            }
        }
    }

    private fun countryCode(country: WofEntity?, city: WofEntity?): String? {
        return country?.countryCode ?: city?.countryCode
    }

    private fun countryName(country: WofEntity?, countryCode: String?): String? {
        return when(country) {
            null -> {
                countryCodeToName[countryCode]
            }
            else -> { country.name }
        }
    }

    fun nearbyCity(countryCode: String?, stateCode: String?, items: List<CityAndDistance>): WofEntity? {
        // Potential ways to determine which nearby locality to choose
        //  1. The largest population
        //  2. The largest area
        //  3. The closest city (the shortest distance)
        //  4. A weighted rank based off of population & distance from point to city
        //  5. Find the closest distance to a shape (currently, it's sorted by distance to center point)
        //  6. Calculate radius based off of area & assuming a circle - choose the closest item
        //  7. A combination of these

//        items.forEach {
//            val radius = sqrt(it.entity.areaMeters / PI).toInt()
//            println("${it.entity.id} ${it.entity.name} ${it.entity.population}/${it.entity.populationRank} ${it.distance} radius=$radius")
//        }

        // Considerations:
        //  A. Select matches from the same country/state

        val sameRegionItems = when(countryCode) {
            null -> {
                items
            }
            else -> {
                when(stateCode) {
                    null -> { items.filter { it.entity.countryCode == countryCode } }
                    else -> { items.filter { it.entity.countryCode == countryCode && it.entity.stateCode == stateCode }}
                }
            }
        }

        // Prefer the closest place with an acceptable population first. This will have inferior matches when
        // the population data is missing, of course.
        val hasPopulationItems = sameRegionItems.filter { (it.entity.populationRank ?: 0) > 1 }
        hasPopulationItems.minByOrNull { it.distance  }?.entity?.let {
            return it
        }

        return sameRegionItems.minByOrNull { it.distance }?.entity
//        .maxByOrNull { it.entity.population ?: 0 }?.entity?.name
    }
}
