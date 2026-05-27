# Pull Consumer Lib · SPEC

## Objetivo

Ofrecer consumo **PULL** declarativo y publicación de mensajes contra la API REST del Messaging Event Gateway documentada en **[`../messaging-event-gateway/SPEC.md`](../messaging-event-gateway/SPEC.md)**.

**Flujo Git:** todo cambio en este repo va en rama (`feat/...`, `fix/...`) desde `main` actualizado; ver **[SPEC del workspace §6](../SPEC.md#6-flujo-git-obligatorio)**.

## API pública

- `@MegPullSubscription(configPrefix | topicId, version, nameSub, token, rows, cron, handlerMethod)` en **clase** (recomendado) o en **método**
- `MegPullClientProperties`: `meg.pull.client.base-url`
- `MegPullClient.publish(topicId, topicVersion, metadata, request)`
- `MegPublishMetadata`: `idempotencyKey`, `topicToken`, `correlationId`, `sourceApp`
- `MegPublishMessageRequest`: `user`, `eventType`, `eventVersion`, `payload`
- `MegBasicPullConsumer<T : MegMessage>`: base genérica para procesamiento por batch + ACK/REJECT por ítem
- `MegBasicPublisher`: base para encapsular publicación estándar (`publishMessage`, `publishUsingTopicConfig`)
- `@MegPublishConfig(configPrefix)`: resuelve del prefix solo `id`, `version`, `token`; `eventType`, `sourceApp`, `idempotencyKey` y `user` los define el caller en cada publish
- `MegMessage` (model base) + `MegMessageMapper<T>` para transformación tipada de mensajes
- `MegValidationResult`: resultado estándar de validación (`valid`, `message?`) para validación de negocio en consumers
- `MegSubscriptionConfig`: `topicId`, `version`, `nameSub`, `token`, `subscriptionId`
- `MegInboundSubscriptionBinding`: contrato mínimo para mapear YAML/properties a suscripción PULL
- `MegSubscriptionConfigs.of(...)` / `MegSubscriptionConfigs.from(binding)`: fábrica en la lib (sin anónimos en cada consumer)

## Flujo

1. Registrar beans con métodos anotados.
2. Por anotación: tarea cron.
3. En cada tick: `GET /subscriptions/{id}/messages?rows=N`, una llamada al handler con la lista completa; **sin ACK implícito**.

## ACK / REJECT

La aplicación usa el contrato del gateway (ver SPEC del gateway). Con `MegBasicPullConsumer.processMessages*`:

- **Falla de mapping** (`mapMessage` lanza excepción): **REJECT** automático para ese mensaje (si hay `messageId` resoluble); el batch **sigue** con el resto.
- **Validación fallida** (`validateMappedMessage` devuelve `MegValidationResult.invalid(...)`): **REJECT** automático para ese mensaje; el batch **sigue** con el resto.
- **Procesamiento OK** (el `processor` termina sin excepción): **PROCESSED** automático.
- **Excepción en el `processor`**: **sin ACK** para ese mensaje (queda pendiente / reintento en el próximo ciclo); el batch **sigue** con el resto.

## Convenciones

- Subscription id: `{topicId}-v{version}-{nameSub}`.
- Handler: vacío o un `List<Map<String, Object>>`.
- Modo recomendado: `configPrefix` en la **clase** del consumer y handler `onPullMessage` (o `handlerMethod` explícito).
- El adapter/mapping (`mapMessage`) transforma raw `Map<String, Any>` a mensaje tipado de dominio; la validación de rechazo de negocio queda en el consumer (`validateMappedMessage`), no en el mapper.
- **Efectos de negocio** (pagos, DB, APIs externas): convención recomendada en la app consumidora — mantener la subclase de `MegBasicPullConsumer` delgada y delegar en un servicio de dominio (por ejemplo `reintegrosPagoService.pagar(reintegro)` tras validación OK), no acoplar esa lógica al adapter del mensaje.
- Suscripción para ACK: construir `MegSubscriptionConfig` con `MegSubscriptionConfigs.from(...)`; el modelo de config de la app puede implementar `MegInboundSubscriptionBinding`.

## Niveles de log (estándar)

- `ERROR`: fallos críticos de inicialización/infraestructura que comprometen el procesamiento (scheduler no arranca, config inválida crítica).
- `WARN`: validaciones rechazadas, excepciones en handler (mensaje queda pending), reintentos/fallos transitorios por mensaje.
- `INFO`: hitos operativos de bajo volumen (registro de subscriptions, resumen por batch).
- `DEBUG`: detalle por mensaje en camino feliz (payload resumido/sanitizado, decisiones de ruteo, ACK PROCESSED).
- `TRACE`: diagnóstico fino (timings y detalles internos), desactivado por defecto.

## Testing

Cada **caso de uso nuevo** (API pública, cliente HTTP, scheduling, ACK/REJECT, publicación, etc.) debe incluir **al menos un test unitario** que lo ejercite de forma explícita. Los tests de integración complementan pero no sustituyen esa cobertura. Regla alineada con el workspace: [`../SPEC.md`](../SPEC.md) §5.
