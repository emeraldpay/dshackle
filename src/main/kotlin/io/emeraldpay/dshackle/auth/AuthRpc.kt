package io.emeraldpay.dshackle.auth

import io.emeraldpay.api.proto.AuthOuterClass
import io.emeraldpay.api.proto.ReactorAuthGrpc
import io.emeraldpay.dshackle.auth.service.AuthService
import io.grpc.Status
import io.grpc.StatusException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

@Service
class AuthRpc(
    private val authService: AuthService,
    private val authScheduler: Scheduler,
) : ReactorAuthGrpc.AuthImplBase() {

    companion object {
        private val log = LoggerFactory.getLogger(AuthRpc::class.java)
    }

    override fun authenticate(request: Mono<AuthOuterClass.AuthRequest>): Mono<AuthOuterClass.AuthResponse> {
        log.info("Start auth process...")
        return request
            .subscribeOn(authScheduler)
            .map {
                val token = authService.authenticate(it.token)
                AuthOuterClass.AuthResponse.newBuilder()
                    .setProviderToken(token)
                    .build()
            }.onErrorResume {
                if (it is StatusException) {
                    log.error("Error during auth - ${it.message}")
                    Mono.error(it)
                } else {
                    val message = "Error during auth - Internal error: ${it.message}"
                    log.error(message)
                    Mono.error(
                        Status.INTERNAL
                            .withDescription(message)
                            .asException(),
                    )
                }
            }
    }
}
