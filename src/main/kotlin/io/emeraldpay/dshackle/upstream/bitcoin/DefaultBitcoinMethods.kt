package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods

class DefaultBitcoinMethods : DirectCallMethods(
        listOf("getbestblockhash", "getblock", "gettransaction")
) {


}