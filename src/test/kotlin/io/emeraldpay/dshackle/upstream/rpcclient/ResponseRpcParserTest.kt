package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

class ResponseRpcParserTest : ShouldSpec({

    val parser = ResponseRpcParser()

    should("Parse string response") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": "Hello world!"}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "\"Hello world!\""
    }

    should("Parse string response when result starts first") {
        //            0       8       16                32  35
        val json = """{"result": "Hello world!", "jsonrpc": "2.0", "id": 1}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "\"Hello world!\""
    }

    should("Parse bool response") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": false}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "false"
    }

    should("Parse bool response when id is last") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "result": false, "id": 1}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "false"
    }

    should("Parse int response") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": 100}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "100"
    }

    should("Parse null response") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": null}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "null"
    }

    should("Parse object response") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": {"hash": "0x00000", "foo": false, "bar": 1}}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "{\"hash\": \"0x00000\", \"foo\": false, \"bar\": 1}"
    }

    should("Parse object response with null error") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": {"hash": "0x00000", "foo": false, "bar": 1}, "error": null}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "{\"hash\": \"0x00000\", \"foo\": false, \"bar\": 1}"
    }

    should("Parse object response if null error comes first") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "error": null, "result": {"hash": "0x00000", "foo": false, "bar": 1}}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "{\"hash\": \"0x00000\", \"foo\": false, \"bar\": 1}"
    }

    should("Parse object response with extra spaces") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "result"  :   {"hash": "0x00000", "foo": false , "bar":1}  , "id": 1}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "{\"hash\": \"0x00000\", \"foo\": false , \"bar\":1}"
    }

    should("Parse complex object response") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": {"hash": "0x00000", "foo": {"bar": 1, "baz": 2}}}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "{\"hash\": \"0x00000\", \"foo\": {\"bar\": 1, \"baz\": 2}}"
    }

    should("Parse array response") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": [1, 2, false]}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "[1, 2, false]"
    }

    should("Parse error") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": null, "error": {"code": -1111, "message": "test"}}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldNotBe null
        act.hasError shouldBe true
        act.hasResult shouldBe false

        act.error!!.code shouldBe -1111
        act.error!!.message shouldBe "test"
    }

    should("Parse error with no result field") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "error": {"code": -1111, "message": "test"}}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldNotBe null
        act.hasError shouldBe true
        act.hasResult shouldBe false

        act.error!!.code shouldBe -1111
        act.error!!.message shouldBe "test"
    }

    should("Parse error with data") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": null, "error": {"code": -1111, "message": "test", "data": "just data"}}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldNotBe null
        act.hasError shouldBe true
        act.hasResult shouldBe false

        act.error!!.code shouldBe -1111
        act.error!!.message shouldBe "test"
        act.error!!.details shouldBe "just data"
    }

    should("Parse error with data struct") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1, "result": null, "error": {"code": -1111, "message": "test", "data": {"foo": "just data", "bar": 1}}}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldNotBe null
        act.hasError shouldBe true
        act.hasResult shouldBe false

        act.error!!.code shouldBe -1111
        act.error!!.message shouldBe "test"
        act.error!!.details shouldBe mapOf("foo" to "just data", "bar" to 1)
    }

    should("Handle non-json with producing an error response") {
        //            0       8       16                32  35
        val json = """NOT JSON"""

        val act = parser.parse(json.toByteArray())

        act.error shouldNotBe null
        act.hasError shouldBe true
        act.hasResult shouldBe false

        act.error!!.code shouldBe RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE

        val json2 = """\nNOT JSON"""

        val act2 = parser.parse(json2.toByteArray())

        act2.error shouldNotBe null
        act2.hasError shouldBe true
        act2.hasResult shouldBe false

        act2.error!!.code shouldBe RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE
    }

    should("Keep provided ID even if JSON is not full") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 1}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldNotBe null
        act.hasError shouldBe true
        act.hasResult shouldBe false

        act.id.asNumber() shouldBe 1
    }

    should("Keep provided ID even if JSON is broken") {
        //            0       8       16                32  35
        val json = """{"jsonrpc": "2.0", "id": 101, "resu'"""

        val act = parser.parse(json.toByteArray())

        act.error shouldNotBe null
        act.hasError shouldBe true
        act.hasResult shouldBe false

        act.id.asNumber() shouldBe 101
    }

    should("Parse JSON with spaces in front") {
        //            0       8       16                32  35
        val json = """  {"result": "Hello world!", "jsonrpc": "2.0", "id": 1}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "\"Hello world!\""
    }

    should("Parse JSON with new line in front") {
        val json = "\n" + """{"result": "Hello world!", "jsonrpc": "2.0", "id": 1}"""

        val act = parser.parse(json.toByteArray())

        act.error shouldBe null
        act.resultAsRawString shouldBe "\"Hello world!\""
    }
})
