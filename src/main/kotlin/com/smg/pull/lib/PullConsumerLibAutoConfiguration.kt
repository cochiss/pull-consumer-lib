package com.smg.pull.lib

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Import

@AutoConfiguration
@EnableConfigurationProperties(MegPullClientProperties::class)
@Import(MegPullClient::class, MegPullSubscriptionRegistrar::class)
class PullConsumerLibAutoConfiguration
