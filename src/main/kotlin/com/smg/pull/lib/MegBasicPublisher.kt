package com.smg.pull.lib

import org.springframework.core.env.Environment

open class MegBasicPublisher(
    protected val megPullClient: MegPullClient,
    private val environment: Environment? = null
) {
    protected fun publishMessage(
        topicId: String,
        topicVersion: Int,
        topicToken: String,
        user: String,
        eventType: String,
        payload: Any,
        idempotencyKey: String,
        correlationId: String,
        sourceApp: String,
        eventVersion: Int = 1
    ): Map<String, Any> =
        megPullClient.publish(
            topicId = topicId,
            topicVersion = topicVersion,
            metadata = MegPublishMetadata(
                idempotencyKey = idempotencyKey,
                topicToken = topicToken,
                correlationId = correlationId,
                sourceApp = sourceApp
            ),
            request = MegPublishMessageRequest(
                user = user,
                eventType = eventType,
                eventVersion = eventVersion,
                payload = payload
            )
        )

    /**
     * Resuelve del [MegPublishConfig] prefix solo topic id, version y publish token.
     * Metadatos de publish (idempotencyKey, eventType, sourceApp, user, etc.) los define el caller.
     */
    protected fun publishUsingTopicConfig(
        idempotencyKey: String,
        correlationId: String,
        user: String,
        eventType: String,
        sourceApp: String,
        payload: Any,
        eventVersion: Int = 1
    ): Map<String, Any> {
        val env = environment
            ?: throw IllegalStateException("Environment is required to publish by @MegPublishConfig")
        val prefix = resolvePublishPrefix()
        val topicId = requiredProperty(env, "$prefix.id")
        val topicVersion = requiredProperty(env, "$prefix.version").toIntOrNull()
            ?: throw IllegalArgumentException("Invalid version for @MegPublishConfig prefix '$prefix'")
        val topicToken = requiredProperty(env, "$prefix.token")

        return publishMessage(
            topicId = topicId,
            topicVersion = topicVersion,
            topicToken = topicToken,
            user = user,
            eventType = eventType,
            payload = payload,
            idempotencyKey = idempotencyKey,
            correlationId = correlationId,
            sourceApp = sourceApp,
            eventVersion = eventVersion
        )
    }

    private fun resolvePublishPrefix(): String {
        val ann = this::class.java.getAnnotation(MegPublishConfig::class.java)
            ?: throw IllegalStateException(
                "Publisher ${this::class.java.simpleName} must define @MegPublishConfig to use publishUsingTopicConfig"
            )
        return ann.configPrefix
    }

    private fun requiredProperty(environment: Environment, key: String): String =
        environment.getProperty(key)
            ?: throw IllegalArgumentException("Missing required property '$key' for publisher config")
}
