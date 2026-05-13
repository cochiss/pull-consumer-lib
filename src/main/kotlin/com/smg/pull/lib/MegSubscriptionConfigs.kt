package com.smg.pull.lib

data class SimpleMegSubscriptionConfig(
    override val topicId: String,
    override val version: Int,
    override val nameSub: String,
    override val token: String,
) : MegSubscriptionConfig

/**
 * Contrato mínimo para armar un [MegSubscriptionConfig] desde el modelo de config de la app (p. ej. YAML).
 */
interface MegInboundSubscriptionBinding {
    val topicId: String
    val topicVersion: Int
    val subscriptionNameSub: String
    val subscriptionToken: String
}

object MegSubscriptionConfigs {
    @JvmStatic
    fun of(
        topicId: String,
        version: Int,
        nameSub: String,
        token: String,
    ): MegSubscriptionConfig = SimpleMegSubscriptionConfig(topicId, version, nameSub, token)

    @JvmStatic
    fun from(binding: MegInboundSubscriptionBinding): MegSubscriptionConfig =
        of(
            binding.topicId,
            binding.topicVersion,
            binding.subscriptionNameSub,
            binding.subscriptionToken,
        )
}
