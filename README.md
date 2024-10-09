# About
The Kotlin AsyncAPI Kafka project is a Code-first tools for generating AsyncAPI documentation from Kotlin classes.
This is specifically built for Kafka Topics. 

# Usage
Declare your senders and receivers by specifying the topic name and the class used for the topic payload:

Example:
```
val senders = mapOf("invoices" to InvoiceTopic::class)
val receivers = mapOf("orders" to OrderTopic::class)
val yamlOutput = generateYaml(senders, receivers)
println(yamlOutput)
```

