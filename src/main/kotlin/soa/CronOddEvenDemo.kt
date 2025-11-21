@file:Suppress("WildcardImport", "NoWildcardImports", "MagicNumber")

package soa

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.integration.annotation.Gateway
import org.springframework.integration.annotation.MessagingGateway
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.integration.config.EnableIntegration
import org.springframework.integration.config.EnableMessageHistory
import org.springframework.integration.dsl.DirectChannelSpec
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.dsl.Pollers
import org.springframework.integration.dsl.PublishSubscribeChannelSpec
import org.springframework.integration.dsl.integrationFlow
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

private val logger = LoggerFactory.getLogger("soa.CronOddEvenDemo")

/**
 * Spring Integration configuration for demonstrating Enterprise Integration Patterns.
 * This application implements a message flow that processes numbers and routes them
 * based on whether they are even or odd.
 *
 * **Your Task**: Analyze this configuration, create an EIP diagram, and compare it
 * with the target diagram to identify and fix any issues.
 */
@SpringBootApplication
@EnableIntegration
@EnableScheduling
@EnableMessageHistory
class IntegrationApplication(
    private val sendNumber: SomeService,
) {
    /**
     * Creates an atomic integer source that generates sequential numbers.
     */
    @Bean
    fun integerSource(): AtomicInteger = AtomicInteger()

    @Bean
    fun evenChannel(): DirectChannelSpec = MessageChannels.direct()

    @Bean
    fun numberChannel(): DirectChannelSpec = MessageChannels.direct()

    @Bean
    fun oddChannel(): PublishSubscribeChannelSpec<*> = MessageChannels.publishSubscribe()

    @Bean
    fun monitoringChannel(): DirectChannelSpec = MessageChannels.direct()

    /**
     * Main integration flow that polls the integer source and routes messages.
     * Polls every 100ms and routes based on even/odd logic.
     */
    @Bean
    fun myFlow(): IntegrationFlow =
        integrationFlow("numberChannel") {
            wireTap("monitoringChannel")
            transform { num: Int ->
                logger.info("📥 Source generated number: {}", num)
                num
            }
            route { p: Int ->
                val channel = if (p % 2 == 0) "evenChannel" else "oddChannel"
                logger.info("🔀 Router: {} → {}", p, channel)
                channel
            }
        }

    /**
     * Integration flow for processing even numbers.
     * Transforms integers to strings and logs the result.
     */
    @Bean
    fun evenFlow(): IntegrationFlow =
        integrationFlow("evenChannel") {
            transform { obj: Int ->
                logger.info("  ⚙️  Even Transformer: {} → 'Number {}'", obj, obj)
                "Number $obj"
            }
            handle { p ->
                logger.info("  ✅ Even Handler: Processed [{}]", p.payload)
            }
        }

    @Bean
    fun polling(integerSource: AtomicInteger): IntegrationFlow =
        integrationFlow(
            source = { integerSource.getAndIncrement() },
            options = { poller(Pollers.fixedRate(100)) },
        ) {
            route { p: Int ->
                val channel = "numberChannel"
                channel
            }
        }

    /**
     * Integration flow for processing odd numbers.
     * Applies a filter before transformation and logging.
     * Note: Examine the filter condition carefully.
     */
    @Bean
    fun oddFlow(): IntegrationFlow =
        integrationFlow("oddChannel") {
            transform { obj: Int ->
                logger.info("  ⚙️  Odd Transformer: {} → 'Number {}'", obj, obj)
                "Number $obj"
            }
            handle { p ->
                logger.info("  ✅ Odd Handler: Processed [{}]", p.payload)
            }
        }

    /**
     * Integration flow for handling discarded messages.
     */
    @Bean
    fun discarded(): IntegrationFlow =
        integrationFlow("discardChannel") {
            handle { p ->
                logger.info("  🗑️  Discard Handler: [{}]", p.payload)
            }
        }

    /**
     * Scheduled task that periodically sends negative random numbers via the gateway.
     */
    @Scheduled(fixedRate = 1000)
    fun sendNumber() {
        val number = -Random.nextInt(100)
        logger.info("🚀 Gateway injecting: {}", number)
        sendNumber.sendNumber(number)
    }

    @Bean
    fun monitoringFlow(): IntegrationFlow =
        integrationFlow("monitoringChannel") {
            handle { msg ->
                val history = msg.headers["history"] as? List<*>
                logger.info("📜 Message History: {}", history)
                logger.info("WIRE TAP de mensaje -> {}", msg.payload)
            }
        }
}

/**
 * Service component that processes messages from the odd channel.
 * Uses @ServiceActivator annotation to connect to the integration flow.
 */
@Component
class ServiceOdd {
    @ServiceActivator(inputChannel = "oddChannel")
    fun handle(p: Any) {
        logger.info("  🔧 Service Activator: Received [{}] (type: {})", p, p.javaClass.simpleName)
    }
}

/**
 * Messaging Gateway for sending numbers into the integration flow.
 * This provides a simple interface to inject messages into the system.
 * Note: Check which channel this gateway sends messages to.
 */
@MessagingGateway
interface SomeService {
    @Gateway(requestChannel = "numberChannel")
    fun sendNumber(number: Int)
}

fun main() {
    runApplication<IntegrationApplication>()
}
