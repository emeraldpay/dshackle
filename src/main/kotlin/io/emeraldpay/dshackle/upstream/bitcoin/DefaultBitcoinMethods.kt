package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods

class DefaultBitcoinMethods : DirectCallMethods(
        listOf(
                "getbestblockhash", "getblock", "getblocknumber", "getblockcount",
                "gettransaction", "getrawtransaction", "gettxout",
                "getreceivedbyaddress", "listunspent",
                "getmemorypool"
        )
) {


}