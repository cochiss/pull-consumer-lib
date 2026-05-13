package com.smg.pull.lib

import org.slf4j.LoggerFactory
import org.springframework.aop.support.AopUtils
import org.springframework.beans.factory.SmartInitializingSingleton
import org.springframework.context.ApplicationContext
import org.springframework.core.env.Environment
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import org.springframework.scheduling.support.CronTrigger
import org.springframework.stereotype.Component
import java.lang.reflect.Method

@Component
class MegPullSubscriptionRegistrar(
    private val applicationContext: ApplicationContext,
    private val client: MegPullClient,
    private val environment: Environment
) : SmartInitializingSingleton {

    private val log = LoggerFactory.getLogger(javaClass)
    private val scheduler = ThreadPoolTaskScheduler().apply {
        poolSize = 2
        setThreadNamePrefix("meg-pull-")
        initialize()
    }

    override fun afterSingletonsInstantiated() {
        applicationContext.beanDefinitionNames.forEach { beanName ->
            val bean = applicationContext.getBean(beanName)
            val targetClass = AopUtils.getTargetClass(bean) ?: bean.javaClass
            val classAnn = targetClass.getAnnotation(MegPullSubscription::class.java)
            if (classAnn != null) {
                val handlerName = classAnn.handlerMethod.trim().ifEmpty { "onPullMessage" }
                val handler = findHandlerMethod(targetClass, handlerName)
                val methodAnnOnSameClass = targetClass.declaredMethods.any { it.getAnnotation(MegPullSubscription::class.java) != null }
                if (methodAnnOnSameClass) {
                    throw IllegalStateException(
                        "Bean ${targetClass.simpleName} has @MegPullSubscription on class and on a method; use only one"
                    )
                }
                registerTask(bean, handler, classAnn)
                return@forEach
            }
            targetClass.declaredMethods.forEach methodLoop@{ method ->
                val ann = method.getAnnotation(MegPullSubscription::class.java) ?: return@methodLoop
                registerTask(bean, method, ann)
            }
        }
    }

    private fun findHandlerMethod(clazz: Class<*>, name: String): Method {
        var c: Class<*>? = clazz
        while (c != null) {
            for (method in c.declaredMethods) {
                if (method.name != name) continue
                if (method.parameterCount == 0) return method
                if (method.parameterCount == 1 && List::class.java.isAssignableFrom(method.parameterTypes[0])) {
                    return method
                }
            }
            c = c.superclass
        }
        throw IllegalArgumentException(
            "No handler '$name' with signature () or (List) on ${clazz.simpleName} (for class-level @MegPullSubscription)"
        )
    }

    private fun registerTask(bean: Any, method: Method, ann: MegPullSubscription) {
        val prefix = ann.configPrefix.trim()
        val queue = if (prefix.isNotBlank()) {
            getRequiredProperty("$prefix.id", method, "queue")
        } else {
            environment.resolvePlaceholders(ann.queue)
        }
        val versionRaw = if (prefix.isNotBlank()) {
            getRequiredProperty("$prefix.version", method, "version")
        } else {
            environment.resolvePlaceholders(ann.version)
        }
        val version = versionRaw.toIntOrNull()
            ?: throw IllegalArgumentException("Invalid version for @MegPullSubscription in ${method.name}")
        val nameSub = if (prefix.isNotBlank()) {
            getRequiredProperty("$prefix.subscription.name-sub", method, "nameSub")
        } else {
            environment.resolvePlaceholders(ann.nameSub)
        }
        val token = if (prefix.isNotBlank()) {
            getRequiredProperty("$prefix.subscription.token", method, "token")
        } else {
            environment.resolvePlaceholders(ann.token)
        }
        val rowsRaw = if (prefix.isNotBlank()) {
            environment.getProperty("$prefix.subscription.rows") ?: ann.rows
        } else {
            environment.resolvePlaceholders(ann.rows)
        }
        val rows = rowsRaw.toIntOrNull()
            ?: throw IllegalArgumentException("Invalid rows for @MegPullSubscription in ${method.name}")
        val cron = if (prefix.isNotBlank()) {
            environment.getProperty("$prefix.subscription.cron") ?: ann.cron
        } else {
            environment.resolvePlaceholders(ann.cron)
        }
        val subscriptionId = "${queue}-v${version}-${nameSub}"
        scheduler.schedule(
            {
                runCatching {
                    val messages = client.pull(subscriptionId, token, rows)
                    invokeHandler(bean, method, messages)
                }.onFailure { ex ->
                    if (ex is MegInactiveSubscriptionException) {
                        log.info("Pull skipped because subscription is INACTIVE: {}", subscriptionId)
                    } else {
                        log.warn("Pull cron failed for subscription {}", subscriptionId, ex)
                    }
                }
            },
            CronTrigger(cron)
        )
        log.info(
            "Registered pull subscription task: subId={}, rows={}, cron={}",
            subscriptionId,
            rows,
            cron
        )
    }

    private fun getRequiredProperty(path: String, method: Method, field: String): String =
        environment.getProperty(path)
            ?: throw IllegalArgumentException(
                "Missing required property '$path' for $field in @MegPullSubscription at ${method.declaringClass.simpleName}.${method.name}"
            )

    private fun invokeHandler(bean: Any, method: Method, messages: List<Map<String, Any>>) {
        method.isAccessible = true
        when (method.parameterCount) {
            0 -> method.invoke(bean)
            1 -> method.invoke(bean, messages)
            else -> throw IllegalArgumentException(
                "Method ${method.name} must have 0 or 1 parameter for @MegPullSubscription"
            )
        }
    }

}
