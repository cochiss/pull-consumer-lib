package com.smg.pull.lib

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "meg.pull.client")
data class MegPullClientProperties(
    var baseUrl: String = "http://localhost:8080"
)
