package testing

import com.fasterxml.jackson.databind.ObjectMapper

class ResourceResponse {

    ObjectMapper objectMapper

    ResourceResponse(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper
    }

    Object getResource(String name) {
        String json = BlocksHandler.class.getResourceAsStream("/" + name)?.text
        if (json == null) {
            return null
        }
        return objectMapper.readValue(json, Map)
    }

    CallHandler.Result respondWith(String name) {
        return CallHandler.Result.ok(getResource(name))
    }

}
