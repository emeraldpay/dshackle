package testing

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

class InternalHandler implements CallHandler {

    private AtomicInteger ids = new AtomicInteger()
    private List<Item> requests = new ArrayList()

    def record(String json) {
        requests << new Item(ids.getAndIncrement(), json)
    }

    @Override
    Result handle(String method, List<Object> params) {
        if (method == "eth_call"
                && params[0].to?.toLowerCase() == "0x0123456789abcdef0123456789abcdef00000002".toLowerCase()) {
            return Result.ok(Collections.unmodifiableList(requests))
        }
        return null
    }

    class Item {
        Integer id
        String timestamp
        String json

        Item(Integer id, String json) {
            this.id = id
            this.timestamp = Instant.now().toString()
            this.json = json
        }
    }
}
