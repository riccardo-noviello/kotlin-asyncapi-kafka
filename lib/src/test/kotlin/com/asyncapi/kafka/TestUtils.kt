package com.asyncapi.kafka

import java.nio.file.Paths
import kotlin.test.fail

class TestUtils {
    data class Approver(val fileName: String, val content: String)

    companion object {
        fun approver(fileName: String): Approver {
            val resource = this::class.java.classLoader.getResource("$fileName.approved")
            val content = resource?.readText(Charsets.UTF_8)!!
            return Approver(fileName, content)
        }
        fun Approver.assertApproved(actual: String) {
            val outputPath = Paths.get("src", "test", "resources", "${this.fileName}.actual").toFile()
            if (this.content != actual) {
                outputPath.parentFile.mkdirs()
                outputPath.writeText(actual)
                fail("Expected $actual \n but got: \n$this ")
            } else {
                outputPath.delete()
            }
        }
    }
}

