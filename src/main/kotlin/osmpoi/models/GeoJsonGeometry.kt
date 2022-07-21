import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonContentPolymorphicSerializer
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject

object GeoJsonGeometrySerializer: JsonContentPolymorphicSerializer<GeoJsonGeometry>(GeoJsonGeometry::class) {
    override fun selectDeserializer(element: JsonElement): DeserializationStrategy<out GeoJsonGeometry> {
        return when ((element.jsonObject.get("type") as JsonPrimitive).content) {
            "Point" -> GeoJsonGeometryPoint.serializer()
            "Polygon" -> GeoJsonGeometryPolygon.serializer()
            "MultiPolygon" -> GeoJsonGeometryMultiPolygon.serializer()
            else -> { throw Exception("Unhandled GeoJson type: '${element.jsonObject["type"]}'")}
        }
    }
}

@Serializable(GeoJsonGeometrySerializer::class)
abstract class GeoJsonGeometry {
    abstract val type: String
}

@Serializable
class GeoJsonGeometryPoint(
    override val type: String = "Point",
    val coordinates: Array<Double>
): GeoJsonGeometry()

@Serializable
class GeoJsonGeometryPolygon(
    override val type: String = "Polygon",
    val coordinates: Array<Array<Array<Double>>>
): GeoJsonGeometry()

@Serializable
class GeoJsonGeometryMultiPolygon(
    override val type: String = "MultiPolygon",
    val coordinates: Array<Array<Array<Array<Double>>>>
): GeoJsonGeometry()

