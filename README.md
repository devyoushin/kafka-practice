# kafka-practice

A hands-on repository for learning Apache Kafka.
- **Environment**: EKS / Kafka 3.7.x (Strimzi Operator)
- **Namespaces**: `kafka` (cluster), `kafka-apps` (producers/consumers)

---

## Learning Path

```
1. Core Concepts     → docs/core-concepts/
2. Producer/Consumer → docs/producer-consumer/
3. Operations        → docs/operations/
4. Security          → docs/security/
5. Observability     → docs/observability/
```

---

## Documents

### Core Concepts (`docs/core-concepts/`)
| File | Description |
|------|-------------|
| [kafka-architecture.md](./docs/core-concepts/kafka-architecture.md) | Kafka 아키텍처 — Broker, Topic, Partition, Offset, KRaft |
| [topic-partition-design.md](./docs/core-concepts/topic-partition-design.md) | Topic/Partition 설계 — 파티션 수, 복제 인수, 리텐션 정책 |
| [consumer-group.md](./docs/core-concepts/consumer-group.md) | Consumer Group — 리밸런싱, 오프셋 커밋, 파티션 할당 전략 |

### Producer/Consumer (`docs/producer-consumer/`)
| File | Description |
|------|-------------|
| [producer-config.md](./docs/producer-consumer/producer-config.md) | Producer 핵심 설정 — acks, batch.size, linger.ms, compression |
| [consumer-config.md](./docs/producer-consumer/consumer-config.md) | Consumer 핵심 설정 — fetch.min.bytes, max.poll.records, session.timeout |
| [exactly-once-semantics.md](./docs/producer-consumer/exactly-once-semantics.md) | Exactly-Once 시맨틱 — Idempotent Producer, Transactional API |

### Operations (`docs/operations/`)
| File | Description |
|------|-------------|
| [topic-management.md](./docs/operations/topic-management.md) | Topic 관리 — 생성/수정/삭제, 파티션 증가, 설정 변경 |
| [consumer-lag-management.md](./docs/operations/consumer-lag-management.md) | Consumer Lag 관리 — Lag 측정, 대응 전략, 오프셋 리셋 |
| [kafka-rebalance.md](./docs/operations/kafka-rebalance.md) | Partition Rebalance — Reassignment, Throttle, 운영 주의사항 |

### Security (`docs/security/`)
| File | Description |
|------|-------------|
| [sasl-authentication.md](./docs/security/sasl-authentication.md) | SASL 인증 — SCRAM-SHA-512, Strimzi KafkaUser |
| [tls-encryption.md](./docs/security/tls-encryption.md) | TLS 암호화 — 인증서 구성, mTLS, Strimzi 자동 인증서 관리 |
| [acl-authorization.md](./docs/security/acl-authorization.md) | ACL 인가 — kafka-acls.sh, Strimzi KafkaUser ACL, 최소 권한 |

### Observability (`docs/observability/`)
| File | Description |
|------|-------------|
| [kafka-metrics.md](./docs/observability/kafka-metrics.md) | Kafka JMX 지표 — Broker/Producer/Consumer 핵심 지표, Prometheus 연동 |
| [consumer-lag-monitoring.md](./docs/observability/consumer-lag-monitoring.md) | Consumer Lag 모니터링 — Kafka Exporter, Prometheus, 알람 설정 |

---

## Manifest Structure

```
config/
├── kafka-cluster.yaml    # Strimzi Kafka CR (3-broker KRaft cluster)
├── kafka-topic.yaml      # KafkaTopic CR
└── kafka-user.yaml       # KafkaUser CR (Producer/Consumer ACL)
```

---

## Key Concept Summary

**Topic → Partition → Offset** 구조가 Kafka의 핵심입니다.

```
Producer
  │
  ▼
[Topic: my-topic]
  ├── Partition 0 → [offset0][offset1][offset2]...  ← Leader: Broker A
  ├── Partition 1 → [offset0][offset1][offset2]...  ← Leader: Broker B
  └── Partition 2 → [offset0][offset1][offset2]...  ← Leader: Broker C
                                                              │
                                                              ▼
                                                       Consumer Group
                                                       ├── Consumer 0 ← Partition 0
                                                       ├── Consumer 1 ← Partition 1
                                                       └── Consumer 2 ← Partition 2
```

> 파티션 수 ≥ Consumer 수여야 모든 Consumer가 작업을 할당받습니다.
