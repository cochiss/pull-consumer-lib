package com.smg.pull.lib.mapper

import com.smg.pull.lib.model.MegMessage

interface MegMessageMapper<T : MegMessage> {
    fun map(message: MutableMap<String, Any>): T
}
