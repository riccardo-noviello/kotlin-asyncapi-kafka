package com.asyncapi.kafka

import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant
import java.time.LocalDate
import java.util.Date
import java.util.UUID
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties

/**
 * This files generates OpenApi yaml from code.
 *
 *  Example:
 *
 *     val senders = mapOf("invoices" to InvoiceTopic::class)
 *     val receivers = mapOf("orders" to OrderTopic::class)
 *     val yamlOutput = generateYaml(senders, receivers)
 *
 *     val outputPath = Paths.get("src", "main", "resources", "asyncapi.yaml").toFile()
 *     outputPath.parentFile.mkdirs()
 *     outputPath.writeText(yamlOutput)
 *
 */

fun generateYaml(senders: Map<String, KClass<*>>?, receivers: Map<String, KClass<*>>?): String {
    val schemaMaps = (senders ?: emptyMap()) + (receivers ?: emptyMap())
    val yamlData = mutableMapOf(
        "asyncapi" to "3.0.0",
        "info" to mapOf(
            "title" to "Kafka Topics",
            "version" to "1.0.0"
        ),
        "channels" to generateChannels(senders, receivers),
        "operations" to generateOperations(senders, receivers),
        "components" to mapOf(
            "schemas" to generateSchemas(schemaMaps)
        )
    )

    val options = DumperOptions()
    options.defaultFlowStyle = DumperOptions.FlowStyle.BLOCK
    options.isPrettyFlow = true
    val yaml = Yaml(options)

    return yaml.dump(yamlData)
}

fun generateChannels(senders: Map<String, KClass<*>>?, receivers: Map<String, KClass<*>>?): Map<String, Any> {
    val channels = mutableMapOf<String, Any>()
    senders?.forEach { (channel, clazz) ->
        channels[channel] = mapOf(
            "description" to "Channel for ${clazz.simpleName} events.",
            "address" to channel,
            "messages" to mapOf(
                clazz.simpleName to mapOf(
                    "name" to clazz.simpleName,
                    "payload" to mapOf("\$ref" to "#/components/schemas/${clazz.simpleName}")
                )
            )
        )
    }
    receivers?.forEach { (channel, clazz) ->
        channels[channel] = mapOf(
            "description" to "Channel for ${clazz.simpleName} events.",
            "address" to channel,
            "messages" to mapOf(
                clazz.simpleName to mapOf(
                    "name" to clazz.simpleName,
                    "payload" to mapOf("\$ref" to "#/components/schemas/${clazz.simpleName}")
                )
            )
        )
    }
    return channels
}

fun generateOperations(senders: Map<String, KClass<*>>?, receivers: Map<String, KClass<*>>?): Map<String, Any> {
    val operations = mutableMapOf<String, Any>()
    senders?.forEach { (channel, clazz) ->
        operations["produce${clazz.simpleName}"] = mapOf(
            "action" to "send",
            "channel" to mapOf("\$ref" to "#/channels/$channel")
        )
    }
    receivers?.forEach { (channel, clazz) ->
        operations["consume${clazz.simpleName}"] = mapOf(
            "action" to "receive",
            "channel" to mapOf("\$ref" to "#/channels/$channel")
        )
    }
    return operations
}

fun generateSchemas(classes: Map<String, KClass<*>>): Map<String, Any> {
    val schemaMap = mutableMapOf<String, Any>()
    classes.values.forEach { clazz ->
        processClass(clazz, schemaMap)
    }
    return schemaMap
}

private val kotlinTypes = listOf(
    "Byte",
    "Short",
    "Int",
    "Long",
    "Float",
    "Double",
    "Char",
    "Boolean",
    "String",
    "List",
    "Set",
    "Map",
    "LocalDate",
    "Instant",
    "Date",
    "Map"
)

// Recursive function to process a data class and add its schema to the map
fun processClass(clazz: KClass<*>, schemaMap: MutableMap<String, Any>) {
    if (schemaMap.containsKey(clazz.simpleName) || clazz.simpleName in kotlinTypes) return

    schemaMap[clazz.simpleName!!] = mapOf(
        "type" to "object",
        "properties" to clazz.memberProperties.associate { property ->
            val propertyType = property.returnType
            val propertyTypeName = propertyType.classifier as? KClass<*> ?: return

            property.name to getTypeMap(propertyType, propertyTypeName, schemaMap)
        }
    )
}

// See: https://www.asyncapi.com/docs/reference/specification/v3.0.0#data-type-formats
fun getTypeMap(propertyType: KType, propertyTypeName: KClass<*>, schemaMap: MutableMap<String, Any>): Map<String, Any> {
    return when (propertyTypeName) {
        List::class -> {
            val itemType = propertyType.arguments.firstOrNull()?.type
            val itemKClass = itemType?.classifier as? KClass<*>
            mapOf(
                "type" to "array",
                "items" to if (itemKClass != null) getTypeMap(
                    itemType,
                    itemKClass,
                    schemaMap
                ) else mapOf("type" to "object")
            )
        }

        Map::class -> {
            val keyItemType = propertyType.arguments[0].type
            val keyItemKClass = keyItemType?.classifier as? KClass<*>

            val valueItemType = propertyType.arguments[1].type
            val valueItemKClass = valueItemType?.classifier as? KClass<*>

            mapOf(
                "type" to "object",
                "properties" to mapOf(
                    "key" to getTypeMap(keyItemType!!, keyItemKClass!!, schemaMap),
                    "value" to getTypeMap(valueItemType!!, valueItemKClass!!, schemaMap),
                )
            )
        }

        in listOf(Date::class, LocalDate::class) -> mapOf(
            "type" to checkNullable(propertyType, "string"),
            "format" to "date"
        )

        Instant::class -> mapOf(
            "type" to checkNullable(propertyType, "string"),
            "format" to "date-time"
        )

        Boolean::class -> mapOf("type" to checkNullable(propertyType, "boolean"))
        in listOf(String::class, Byte::class) -> mapOf("type" to checkNullable(propertyType, "string"))
        UUID::class -> mapOf(
            "type" to checkNullable(propertyType, "string"),
            "format" to "uuid"
        )
        in listOf(Int::class, Long::class, BigInteger::class) -> mapOf("type" to checkNullable(propertyType, "integer"))
        in listOf(Float::class, Double::class, BigDecimal::class) -> mapOf("type" to checkNullable(propertyType, "number"))
        else -> {
            processClass(propertyTypeName, schemaMap)
            mapOf("\$ref" to "#/components/schemas/${propertyTypeName.simpleName}")
        }
    }
}

fun checkNullable(nullableType: KType, type: String): Any {
    return if (nullableType.isMarkedNullable) {
        listOf("null", type)
    } else {
        type
    }
}
