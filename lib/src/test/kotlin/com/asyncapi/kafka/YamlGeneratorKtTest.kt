package com.asyncapi.kafka

import com.asyncapi.kafka.TestUtils.Companion.assertApproved
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.yaml.snakeyaml.Yaml
import java.math.BigDecimal
import java.time.LocalDate
import java.util.*
import kotlin.reflect.KClass

class YamlGeneratorKtTest {

    data class ExampleTopic1(
        val code: Long,
        val amount: BigDecimal,
        val name: String,
        val age: Int,
        val birthDate: LocalDate?
    )

    data class ExampleTopic2(val id: UUID, val description: String, val active: Boolean)
    data class NestedTopic(val orgCode: String, val contacts: List<Contact>)
    data class MapTopic(val orgCode: String, val contacts: Map<String, Contact>)
    data class Contact(val name: String, val lastname: String, val deleted: Boolean)

    private lateinit var senders: Map<String, KClass<*>>
    private lateinit var receivers: Map<String, KClass<*>>

    @Nested
    inner class YamlGeneratorApprovalTest {

        @Test
        fun `generateYaml with 1 producer and consumer produces asyncApi document`() {
            val senders = mapOf("test_topic1" to ExampleTopic1::class)
            val receivers = mapOf("test_topic2" to ExampleTopic2::class)
            val yamlOutput = generateYaml(senders, receivers)
            TestUtils.approver("asyncapi1").assertApproved(yamlOutput)
        }

        @Test
        fun `generateYaml with 2 producers and no consumers produces asyncApi document`() {
            val senders =
                mapOf("example_topic1" to ExampleTopic1::class, "example_topic2" to ExampleTopic2::class)
            val yamlOutput = generateYaml(senders, null)
            TestUtils.approver("asyncapi2").assertApproved(yamlOutput)
        }

        @Test
        fun `generateYaml with 2 consumers and no producers produces asyncApi document`() {
            val receivers =
                mapOf("example_topic1" to ExampleTopic1::class, "example_topic2" to ExampleTopic2::class)
            val yamlOutput = generateYaml(null, receivers)
            TestUtils.approver("asyncapi3").assertApproved(yamlOutput)
        }

        @Test
        fun `generateYaml with 1 consumer that has a map structure produces asyncApi document`() {
            val receivers =
                mapOf("example_topic1" to MapTopic::class)
            val yamlOutput = generateYaml(null, receivers)
            TestUtils.approver("asyncapi4").assertApproved(yamlOutput)
        }
    }

    @BeforeEach
    fun setUp() {
        senders = mapOf("ExampleTopic1Channel" to ExampleTopic1::class)
        receivers = mapOf("ExampleTopic2Channel" to ExampleTopic2::class)
    }

    @Test
    fun `generateYaml generates valid YAML`() {
        val yamlString = generateYaml(senders, receivers)

        val yaml = Yaml()
        val parsedData = yaml.load<Map<String, Any>>(yamlString)

        assertEquals("3.0.0", parsedData["asyncapi"])
        val info = parsedData["info"] as Map<*, *>
        assertEquals("Kafka Topics", info["title"])
        assertEquals("1.0.0", info["version"])

        val channels = parsedData["channels"] as Map<*, *>
        assertEquals(2, channels.size)
        assert(channels.containsKey("ExampleTopic1Channel"))
        assert(channels.containsKey("ExampleTopic2Channel"))

        val operations = parsedData["operations"] as Map<*, *>
        assertEquals(2, operations.size)
        assert(operations.containsKey("produceExampleTopic1"))
        assert(operations.containsKey("consumeExampleTopic2"))

        val components = parsedData["components"] as Map<*, *>
        val schemas = components["schemas"] as Map<*, *>
        assertEquals(2, schemas.size)
        assert(schemas.containsKey("ExampleTopic1"))
        assert(schemas.containsKey("ExampleTopic2"))
    }

    @Test
    fun `generateChannels generates correct channel structure`() {
        val channels = generateChannels(senders, receivers)

        assertEquals(2, channels.size)
        val senderChannel = channels["ExampleTopic1Channel"] as Map<*, *>
        assertEquals("Channel for ExampleTopic1 events.", senderChannel["description"])

        val receiverChannel = channels["ExampleTopic2Channel"] as Map<*, *>
        assertEquals("Channel for ExampleTopic2 events.", receiverChannel["description"])
    }

    @Test
    fun `generateOperations generates correct operations`() {
        val operations = generateOperations(senders, receivers)

        assertEquals(2, operations.size)
        val produceOperation = operations["produceExampleTopic1"] as Map<*, *>
        assertEquals("send", produceOperation["action"])
        val produceChannel = produceOperation["channel"] as Map<*, *>
        assertEquals("#/channels/ExampleTopic1Channel", produceChannel["\$ref"])

        val consumeOperation = operations["consumeExampleTopic2"] as Map<*, *>
        assertEquals("receive", consumeOperation["action"])
        val consumeChannel = consumeOperation["channel"] as Map<*, *>
        assertEquals("#/channels/ExampleTopic2Channel", consumeChannel["\$ref"])
    }

    @Test
    fun `generateSchemas correctly processes classes`() {
        val schemas = generateSchemas(senders + receivers)

        assertEquals(2, schemas.size)
        val senderSchema = schemas["ExampleTopic1"] as Map<*, *>
        assertEquals("object", senderSchema["type"])

        val senderProperties = senderSchema["properties"] as Map<*, *>
        assertEquals(5, senderProperties.size)
        assert(senderProperties.containsKey("name"))
        assert(senderProperties.containsKey("age"))
        assert(senderProperties.containsKey("birthDate"))

        val receiverSchema = schemas["ExampleTopic2"] as Map<*, *>
        assertEquals("object", receiverSchema["type"])

        val receiverProperties = receiverSchema["properties"] as Map<*, *>
        assertEquals(3, receiverProperties.size)
        assert(receiverProperties.containsKey("id"))
        assert(receiverProperties.containsKey("description"))
        assert(receiverProperties.containsKey("active"))
    }

    @Test
    fun `processClass correctly handles nested classes`() {
        val nestedClass = NestedTopic::class

        val schemaMap = mutableMapOf<String, Any>()
        processClass(nestedClass, schemaMap)

        assertEquals(2, schemaMap.size)
        assert(schemaMap.containsKey("NestedTopic"))
        assert(schemaMap.containsKey("Contact"))
    }

    @Test
    fun `processClass correctly handles Maps`() {
        val mapClass = MapTopic::class

        val schemaMap = mutableMapOf<String, Any>()
        processClass(mapClass, schemaMap)

        assertEquals(2, schemaMap.size)
        assert(schemaMap.containsKey("MapTopic"))
        assert(schemaMap.containsKey("Contact"))
    }

    @Test
    fun `processClass correctly ignores native classes`() {
        val ignoredClass = BigDecimal::class

        val schemaMap = mutableMapOf<String, Any>()
        processClass(ignoredClass, schemaMap)

        assertEquals(1, schemaMap.size)
        assert(schemaMap.containsKey("BigDecimal"))
    }
}
