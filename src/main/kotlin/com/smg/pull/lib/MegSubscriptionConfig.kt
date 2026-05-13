package com.smg.pull.lib

interface MegSubscriptionConfig {
    val topicId: String
    val version: Int
    val nameSub: String
    val token: String

    val subscriptionId: String
        get() = "$topicId-v$version-$nameSub"
}
