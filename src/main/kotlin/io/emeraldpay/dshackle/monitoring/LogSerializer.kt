package io.emeraldpay.dshackle.monitoring

import java.nio.ByteBuffer
import java.util.function.Function

interface LogSerializer<T> : Function<T, ByteBuffer>
