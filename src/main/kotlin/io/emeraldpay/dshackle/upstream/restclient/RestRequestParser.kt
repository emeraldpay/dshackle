package io.emeraldpay.dshackle.upstream.restclient

import java.util.ArrayDeque
import java.util.Queue

object RestRequestParser {

    fun transformPathParams(path: String, pathParams: List<String>): String {
        if (pathParams.isEmpty()) {
            return path
        }
        val params: Queue<String> = ArrayDeque(pathParams)
        val paramPlaceholder = '*'
        var occurrenceIndex = path.indexOf(paramPlaceholder)

        if (occurrenceIndex < 0) {
            return path
        }

        val builder = StringBuilder()

        var i = 0
        while (occurrenceIndex >= 0 && params.isNotEmpty()) {
            val param = params.poll()
            builder.append(path, i, occurrenceIndex).append(param)
            i = occurrenceIndex + 1
            occurrenceIndex = path.indexOf(paramPlaceholder, occurrenceIndex + 1)
        }

        return builder.append(path, i, path.length).toString()
    }

    fun transformQueryParams(queryParams: List<Pair<String, String>>): String {
        if (queryParams.isEmpty()) {
            return ""
        }
        return "?".plus(queryParams.joinToString("&") { "${it.first}=${it.second}" })
    }
}
