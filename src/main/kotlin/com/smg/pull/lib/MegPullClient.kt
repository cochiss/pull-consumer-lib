package com.smg.pull.lib

import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestTemplate

@Component
class MegPullClient(
    private val props: MegPullClientProperties
) {
    private val restTemplate = RestTemplate()

    @Suppress("UNCHECKED_CAST")
    fun pull(subscriptionId: String, token: String, rows: Int): List<Map<String, Any>> {
        val headers = HttpHeaders()
        headers["X-Sub-Token"] = token
        val entity = HttpEntity<Void>(headers)
        val response = try {
            restTemplate.exchange(
                "${props.baseUrl}/subscriptions/$subscriptionId/messages?rows=$rows",
                HttpMethod.GET,
                entity,
                List::class.java
            )
        } catch (ex: HttpClientErrorException.Forbidden) {
            val responseBody = ex.responseBodyAsString ?: ""
            if (responseBody.contains("INACTIVE", ignoreCase = true)) {
                throw MegInactiveSubscriptionException(subscriptionId)
            }
            throw ex
        }
        return (response.body ?: emptyList<Any>()) as List<Map<String, Any>>
    }

    fun ackProcessed(subscriptionId: String, token: String, messageId: String) {
        ack(subscriptionId, token, messageId, "PROCESSED")
    }

    fun ackReject(subscriptionId: String, token: String, messageId: String) {
        ack(subscriptionId, token, messageId, "REJECT")
    }

    private fun ack(subscriptionId: String, token: String, messageId: String, action: String) {
        val headers = HttpHeaders()
        headers.contentType = MediaType.APPLICATION_JSON
        headers["X-Sub-Token"] = token
        val entity = HttpEntity(mapOf("action" to action), headers)
        restTemplate.exchange(
            "${props.baseUrl}/subscriptions/$subscriptionId/messages/$messageId",
            HttpMethod.PUT,
            entity,
            Void::class.java
        )
    }

    @Suppress("UNCHECKED_CAST")
    fun publish(
        topicId: String,
        topicVersion: Int,
        metadata: MegPublishMetadata,
        request: MegPublishMessageRequest
    ): Map<String, Any> {
        val headers = HttpHeaders()
        headers.contentType = MediaType.APPLICATION_JSON
        headers["Idempotency-Key"] = metadata.idempotencyKey
        headers["X-Topic-Token"] = metadata.topicToken
        headers["X-Correlation-Id"] = metadata.correlationId
        headers["X-Source-App"] = metadata.sourceApp
        val entity = HttpEntity(request, headers)
        val response = restTemplate.exchange(
            "${props.baseUrl}/topics/$topicId/v$topicVersion/messages",
            HttpMethod.POST,
            entity,
            Map::class.java
        )
        return (response.body ?: emptyMap<String, Any>()) as Map<String, Any>
    }
}

class MegInactiveSubscriptionException(subscriptionId: String) :
    RuntimeException("Subscription '$subscriptionId' is INACTIVE")

data class MegPublishMetadata(
    val idempotencyKey: String,
    val topicToken: String,
    val correlationId: String,
    val sourceApp: String
)

data class MegPublishMessageRequest(
    val user: String,
    val eventType: String,
    val eventVersion: Int = 1,
    val payload: Any
)
