package com.smg.pull.lib

@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class MegPullSubscription(
    val configPrefix: String = "",
    val queue: String = "",
    val version: String = "1",
    val nameSub: String = "",
    val token: String = "",
    val rows: String = "10",
    val cron: String = "*/10 * * * * *",
    /** Solo aplica si la anotación está en la clase: nombre del método handler (0 args o 1 List). */
    val handlerMethod: String = "onPullMessage",
)
