# {서비스명} Kafka 구성 문서

> **네임스페이스**: {namespace}
> **작성일**: {YYYY-MM-DD}
> **Kafka 클러스터**: {cluster-name}

---

## 1. 서비스 개요

| 항목 | 내용 |
|------|------|
| 서비스명 | {service-name} |
| 역할 | Producer / Consumer / Both |
| Consumer Group ID | {group-id} |
| 네임스페이스 | {namespace} |

---

## 2. Topic 구성

| Topic 이름 | 파티션 수 | 복제 인수 | Retention | 용도 |
|-----------|---------|---------|----------|------|
| {topic-name} | {N} | 3 | {N}일 | {설명} |

---

## 3. Producer 설정

```properties
bootstrap.servers=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="{username}" \
  password="${KAFKA_PASSWORD}";

acks=all
enable.idempotence=true
compression.type=lz4
batch.size=16384
linger.ms=5
```

---

## 4. Consumer 설정

```properties
bootstrap.servers=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="{username}" \
  password="${KAFKA_PASSWORD}";

group.id={group-id}
auto.offset.reset=earliest
enable.auto.commit=false
max.poll.records=500
session.timeout.ms=30000
max.poll.interval.ms=300000
```

---

## 5. KafkaUser ACL

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: {username}
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: {topic-name}
        operations: [Write, Describe]
      - resource:
          type: group
          name: {group-id}
        operations: [Read]
      - resource:
          type: topic
          name: {topic-name}
        operations: [Read, Describe]
```

---

## 6. 모니터링

```bash
# Consumer Lag 확인
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group {group-id}
```

- Prometheus: `kafka_consumer_group_lag{group="{group-id}"}`
- 알람 기준: Lag > {N} for 5m
