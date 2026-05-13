package com.smg.pull.lib.model

open class MegMessage(
    val messageId: String,
    val correlationId: String,
    val user: String,
    val payload: Map<String, Any>
)
