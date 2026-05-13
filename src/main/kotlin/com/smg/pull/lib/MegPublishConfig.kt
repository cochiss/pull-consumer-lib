package com.smg.pull.lib

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class MegPublishConfig(
    val configPrefix: String
)
