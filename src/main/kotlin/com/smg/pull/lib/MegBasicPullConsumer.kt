package com.smg.pull.lib

import com.smg.pull.lib.model.MegMessage
import org.slf4j.LoggerFactory
import java.util.function.Consumer

abstract class MegBasicPullConsumer<T : MegMessage>(
    protected val megPullClient: MegPullClient
) {
    private val log = LoggerFactory.getLogger(javaClass)

    protected fun ackProcessed(subscriptionId: String, token: String, messageId: String) {
        megPullClient.ackProcessed(subscriptionId, token, messageId)
    }

    protected fun ackReject(subscriptionId: String, token: String, messageId: String) {
        megPullClient.ackReject(subscriptionId, token, messageId)
    }

    protected fun ackProcessed(config: MegSubscriptionConfig, messageId: String) {
        ackProcessed(config.subscriptionId, config.token, messageId)
    }

    protected fun ackReject(config: MegSubscriptionConfig, messageId: String) {
        ackReject(config.subscriptionId, config.token, messageId)
    }

    protected fun hasMessages(messages: List<*>?): Boolean = !messages.isNullOrEmpty()

    protected fun processMessages(
        messages: List<MutableMap<String, Any>>?,
        subscriptionId: String,
        token: String,
        processor: Consumer<T>
    ) {
        val batchSize = messages?.size ?: 0
        log.info("PULL batch execution subscriptionId={} batchSize={}", subscriptionId, batchSize)
        if (!hasMessages(messages)) return
        messages!!.forEach { rawMessage ->
            val mapped = try {
                mapMessage(rawMessage)
            } catch (ex: Exception) {
                val fallbackMessageId = resolveMessageId(rawMessage)
                if (fallbackMessageId != null) {
                    ackReject(subscriptionId, token, fallbackMessageId)
                    log.info(
                        "ACK REJECT sent subscriptionId={} reason={} {}",
                        subscriptionId,
                        "invalid message",
                        rawMessageLogContext(rawMessage, fallbackMessageId)
                    )
                } else {
                    log.warn("Mapping failed without messageId; cannot REJECT. subscriptionId={}", subscriptionId, ex)
                }
                return@forEach
            }
            val messageId = mapped.messageId
            if (messageId.isBlank()) {
                log.warn("Mapped message without messageId ignored subscriptionId={}", subscriptionId)
                return@forEach
            }
            val validationResult = validateMappedMessage(mapped)
            if (!validationResult.valid) {
                val validationError = validationResult.message ?: "invalid message"
                val context = mappedLogContext(mapped)
                ackReject(subscriptionId, token, messageId)
                log.info("ACK REJECT sent subscriptionId={} reason={} {}", subscriptionId, validationError, context)
                return@forEach
            }
            val context = mappedLogContext(mapped)
            try {
                processor.accept(mapped)
                ackProcessed(subscriptionId, token, messageId)
                log.info("ACK PROCESSED sent subscriptionId={} {}", subscriptionId, context)
                log.info("PULL message processed successfully subscriptionId={} {}", subscriptionId, context)
                log.debug("PULL message payload subscriptionId={} messageId={} payload={}", subscriptionId, messageId, mapped.payload)
            } catch (ex: Exception) {
                // Sin ACK: el mensaje queda pendiente y se reintenta en el próximo pull; el batch sigue con el resto.
                log.warn("Processing failed; message stays pending subscriptionId={} {}", subscriptionId, context, ex)
            }
        }
    }

    protected fun buildSubscriptionId(topicId: String, version: Int, nameSub: String): String =
        "$topicId-v$version-$nameSub"

    protected fun processMessagesForSubscription(
        messages: List<MutableMap<String, Any>>?,
        topicId: String,
        version: Int,
        nameSub: String,
        token: String,
        processor: Consumer<T>
    ) {
        val subscriptionId = buildSubscriptionId(topicId, version, nameSub)
        processMessages(messages, subscriptionId, token, processor)
    }

    protected fun processMessagesForSubscription(
        messages: List<MutableMap<String, Any>>?,
        config: MegSubscriptionConfig,
        processor: Consumer<T>
    ) {
        processMessages(messages, config.subscriptionId, config.token, processor)
    }

    protected fun resolveHeaderValue(
        message: MutableMap<String, Any>,
        key: String,
        fallback: String?
    ): String? {
        val headerObj = message["header"] as? Map<*, *> ?: return fallback
        val value = headerObj[key]
        return value?.toString() ?: fallback
    }

    protected fun resolveMessageId(message: MutableMap<String, Any>): String? =
        resolveHeaderValue(message, "messageId", null)

    @Suppress("UNCHECKED_CAST")
    protected fun payload(message: MutableMap<String, Any>): MutableMap<String, Any> =
        message["payload"] as? MutableMap<String, Any>
            ?: throw IllegalArgumentException("missing payload")

    protected fun requireNumber(payload: MutableMap<String, Any>, key: String): Number =
        payload[key] as? Number ?: throw IllegalArgumentException("missing $key")

    protected fun requireText(payload: MutableMap<String, Any>, key: String): String {
        val value = payload[key]?.toString() ?: throw IllegalArgumentException("missing $key")
        if (value.isBlank()) throw IllegalArgumentException("blank $key")
        return value
    }

    private fun rawMessageLogContext(message: MutableMap<String, Any>, messageId: String?): String {
        val correlationId = resolveHeaderValue(message, "correlationId", message["correlationId"]?.toString())
        val sourceApp = resolveHeaderValue(message, "sourceApp", message["sourceApp"]?.toString())
        val eventType = resolveHeaderValue(message, "eventType", message["eventType"]?.toString())
        val user = resolveHeaderValue(message, "user", message["user"]?.toString())
        return "messageId=$messageId correlationId=$correlationId sourceApp=$sourceApp eventType=$eventType user=$user"
    }

    private fun mappedLogContext(message: T): String =
        "messageId=${message.messageId} correlationId=${message.correlationId} user=${message.user}"

    protected abstract fun mapMessage(message: MutableMap<String, Any>): T

    protected open fun validateMappedMessage(message: T): MegValidationResult = MegValidationResult.ok()
}
