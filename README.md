# Pull Consumer Lib

Librería Kotlin/Spring para aplicaciones que consumen mensajes **por HTTP** la API PULL del **Messaging Event Gateway** y también publican eventos: `@MegPullSubscription`, cron, tamaño de batch (`rows`) y cliente configurable.

Con `MegBasicPullConsumer<T>`, el ACK se resuelve por mensaje: falla de `mapMessage` (adapter) o `validateMappedMessage` inválido -> `REJECT`; handler OK -> `PROCESSED`; excepción en handler -> sin ACK (pendiente para próximo ciclo).

Documentación técnica: **[`SPEC.md`](SPEC.md)**.

## Maven

Coordenada:

```text
com.smg:pull-consumer-lib:0.0.1-SNAPSHOT
```

```xml
<dependency>
  <groupId>com.smg</groupId>
  <artifactId>pull-consumer-lib</artifactId>
  <version>0.0.1-SNAPSHOT</version>
</dependency>
```

Instalación local: `mvn install -DskipTests`. Publicá el artefacto en Nexus/Artifactory si consumís la librería desde otro repo.

El proyecto incluye `.mvn/maven.config` + `settings-central.xml` (solo Maven Central) por si tu `~/.m2/settings.xml` fuerza un mirror corporativo inaccesible en tu laptop; ajustalo según tu entorno.

## Handler

Recomendado: anotación en la **clase** (misma convención que `@MegPublishConfig` en publishers). El método handler por defecto es `onPullMessage`.

```java
@Component
@MegPullSubscription(configPrefix = "sample.topics.in.reintegros")
public class PullReintegrosConsumer extends MegBasicPullConsumer {
    public void onPullMessage(List<Map<String, Object>> messages) {
    }
}
```

Otro nombre de método: `@MegPullSubscription(..., handlerMethod = "pullBatch")`.

También se admite la anotación solo en el **método** (legacy).

Con `configPrefix`, la librería resuelve automáticamente:
- `${prefix}.id`
- `${prefix}.version`
- `${prefix}.subscription.name-sub`
- `${prefix}.subscription.token`
- `${prefix}.subscription.rows` (opcional, default `10`)
- `${prefix}.subscription.cron` (opcional, default `*/10 * * * * *`)

## Gateway base URL

```yaml
meg:
  pull:
    client:
      base-url: http://localhost:8080
```

Los endpoints y cabeceras que usa la librería están definidos en **[Messaging Event Gateway · SPEC](../messaging-event-gateway/SPEC.md)**.

## Publicar mensajes con la librería

```java
MegPublishMetadata metadata = new MegPublishMetadata(
    "evt-002",
    "TOPIC_TOKEN_DEVUELTO_EN_CREATE_TOPIC",
    "corr-002",
    "finanzas-api"
);
MegPublishMessageRequest request = new MegPublishMessageRequest(
    "jdoe",
    "reintegro.solicitado",
    1,
    Map.of("monto", 200, "du", "29345928", "cbu", "0110567620056701234560")
);
Map<String, Object> response = megPullClient.publish("solicitudes-reintegros", 1, metadata, request);
```

## Clases base para estandarizar implementaciones

- `MegBasicPullConsumer<T : MegMessage>`: base genérica con helpers (`ackProcessed`, `ackReject`) y template de procesamiento por batch.
- `MegBasicPublisher`: helper para publicar con `MegPullClient` construyendo metadata/request de forma consistente.
- `@MegPublishConfig(configPrefix)`: permite resolver config de publish una sola vez por clase (`id`, `version`, `token`, `event-type`) y usar `publishMessageFromConfig(...)`.
- `MegMessage` + `MegMessageMapper<T>`: patrón para mapear `Map<String, Object>` a modelos tipados por dominio (solo mapping).
- `MegValidationResult`: resultado estándar de validación (`valid` + `message` nullable) para validaciones en el consumer.
- `MegSubscriptionConfigs.from(MegInboundSubscriptionBinding)`: una sola línea para armar `MegSubscriptionConfig` desde tu `@ConfigurationProperties` (implementá el binding en tu clase de topic inbound).

En `MegBasicPullConsumer`, al procesar un **batch**, cada mensaje es independiente: validación → posible `REJECT`; handler OK → `PROCESSED`; excepción en el handler → sin ACK y se continúa con los demás ítems.

Estas clases permiten que cada app defina consumidores y publishers concretos, minimizando duplicación de lógica común.

## Ejemplo completo (estilo sample)

Consumer PULL (anotación en clase + `configPrefix` + adapter/mapping + validación en consumer):

```java
@Component
@MegPullSubscription(configPrefix = "sample.topics.in.reintegros")
public class PullReintegrosStandardConsumer extends MegBasicPullConsumer<ReintegroMessage> {
    private final MegSubscriptionConfig subscriptionConfig;
    private final ReintegrosLargePublisher publisher;
    private final ReintegroMessageMapper mapper;

    public PullReintegrosStandardConsumer(
            MegPullClient megPullClient,
            SampleTopicsProperties sampleTopicsProperties,
            ReintegrosLargePublisher publisher,
            ReintegroMessageMapper mapper
    ) {
        super(megPullClient);
        this.subscriptionConfig = MegSubscriptionConfigs.from(sampleTopicsProperties.getIn().getReintegros());
        this.publisher = publisher;
        this.mapper = mapper;
    }

    @SuppressWarnings("unchecked")
    public void onPullMessage(List<Map<String, Object>> messages) {
        processMessagesForSubscription((List<Map<String, Object>>) (List<?>) messages, subscriptionConfig, message -> {
            if (message.getMonto() > 10000) {
                publisher.publishMessage(
                    message.getMessageId(),
                    message.getCorrelationId(),
                    message.getUser(),
                    (Map<String, Object>) (Map<?, ?>) message.getPayload()
                );
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ReintegroMessage mapMessage(Map<String, Object> message) {
        return mapper.map(message);
    }

    @Override
    protected MegValidationResult validateMappedMessage(ReintegroMessage message) {
        if (message.getMessageId() == null || message.getMessageId().isBlank()) {
            return MegValidationResult.invalid("missing messageId");
        }
        return MegValidationResult.ok();
    }
}
```

Publisher (anotación en clase + `publishMessageFromConfig(...)`):

```java
@Component
@MegPublishConfig(configPrefix = "sample.topics.out.reintegros-alto-monto")
public class ReintegrosLargePublisher extends MegBasicPublisher {
    public ReintegrosLargePublisher(MegPullClient megPullClient, Environment environment) {
        super(megPullClient, environment);
    }

    public Map<String, Object> publishMessage(
            String sourceMessageId,
            String correlationId,
            String user,
            Map<String, Object> payload
    ) {
        return super.publishMessageFromConfig(sourceMessageId, correlationId, user, payload, 1);
    }
}
```

## Build

```bash
mvn clean install
```
