# CLAUDE.md — kafka-practice 지식 베이스

Kafka 운영 경험 기반의 개인 지식 베이스입니다. 문서 추가/수정 시 아래 가이드를 따릅니다.

## 프로젝트 설정

- **환경**: EKS
- **Kafka 버전**: 3.7.x
- **운영 방식**: Strimzi Operator
- **Kafka 네임스페이스**: `kafka`
- **앱 네임스페이스**: `kafka-apps`
- **Bootstrap Server**: `my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092`

---

## 프로젝트 구조

```
kafka-practice/
├── docs/                              # 지식 문서 (카테고리별 분류)
│   ├── core-concepts/       (3개)    # Kafka 아키텍처, Topic/Partition, Consumer Group
│   ├── producer-consumer/   (3개)    # Producer/Consumer 설정, EOS
│   ├── operations/          (3개)    # Topic 관리, Lag 관리, Rebalance
│   ├── security/            (3개)    # SASL, TLS, ACL
│   └── observability/       (2개)    # JMX 지표, Lag 모니터링
│
├── config/                            # Kafka/Strimzi 설정 예제 YAML
│   ├── kafka-cluster.yaml             # Strimzi Kafka CR
│   ├── kafka-topic.yaml               # KafkaTopic CR
│   └── kafka-user.yaml                # KafkaUser CR
│
├── templates/                         # 재사용 문서 템플릿
│   ├── service-doc.md                 # 서비스 Kafka 구성 문서
│   ├── runbook.md                     # 운영 Runbook
│   └── incident-report.md            # 장애 보고서
│
├── rules/                             # Claude 작성 규칙
│   ├── doc-writing.md                 # 문서 스타일 가이드
│   ├── kafka-conventions.md           # CLI/YAML 코드 규칙
│   ├── security-checklist.md          # 보안 검토 체크리스트
│   └── monitoring.md                  # 모니터링/확인 작성 기준
│
├── agents/                            # Claude 전문 에이전트
│   ├── doc-writer.md                  # 문서 작성 에이전트
│   ├── kafka-advisor.md               # Kafka 아키텍처 설계/검토 에이전트
│   ├── consumer-group-analyzer.md     # Consumer Group 분석 에이전트
│   └── incident-analyzer.md           # 장애 분석 에이전트
│
└── .claude/
    ├── settings.json                  # 프로젝트 공유 설정
    └── commands/                      # 커스텀 슬래시 커맨드
        ├── new-doc.md                 # /new-doc
        ├── new-runbook.md             # /new-runbook
        ├── review-doc.md              # /review-doc
        ├── add-troubleshooting.md     # /add-troubleshooting
        └── search-kb.md               # /search-kb
```

---

## 커스텀 슬래시 커맨드

| 커맨드 | 사용법 | 설명 |
|--------|--------|------|
| `/new-doc` | `/new-doc core-concepts replication-factor` | 신규 문서 스캐폴딩 |
| `/new-runbook` | `/new-runbook operations consumer-lag-reset` | 운영 Runbook 생성 |
| `/review-doc` | `/review-doc docs/security/sasl-authentication.md` | 문서 품질 검토 |
| `/add-troubleshooting` | `/add-troubleshooting docs/operations/kafka-rebalance.md <증상>` | 트러블슈팅 추가 |
| `/search-kb` | `/search-kb consumer lag` | 지식 베이스 키워드 검색 |

---

## 파일 네이밍 규칙

```
docs/{카테고리}/{주제}.md
```

- 카테고리: `core-concepts`, `producer-consumer`, `operations`, `security`, `observability`
- 주제: 소문자 영어, 하이픈 구분
- 예시: `docs/operations/topic-compaction.md`, `docs/security/oauth-authentication.md`

---

## 문서 작성 원칙

1. **실제 경험 기반** — 운영 중 실제로 겪은 이슈와 해결 방법 위주
2. **재현 가능한 코드** — kafka-topics.sh, kafka-consumer-groups.sh 복붙 즉시 적용 가능
3. **원인 중심 트러블슈팅** — 증상만 나열하지 말고 근본 원인 설명
4. **한국어 기술 문서** — 주요 개념은 영어 원문 병기
5. **모니터링 필수** — 모든 문서에 JMX/Prometheus 지표 또는 진단 명령어 포함

세부 규칙은 `rules/` 디렉토리를 참조합니다.

---

## 카테고리별 문서 목록

### docs/core-concepts/
| 파일 | 주제 |
|------|------|
| `kafka-architecture.md` | Kafka 아키텍처 — Broker, Topic, Partition, Offset, KRaft |
| `topic-partition-design.md` | Topic/Partition 설계 원칙 — 파티션 수 산정, 복제 인수, 리텐션 |
| `consumer-group.md` | Consumer Group — 리밸런싱, 오프셋 커밋, 파티션 할당 전략 |

### docs/producer-consumer/
| 파일 | 주제 |
|------|------|
| `producer-config.md` | Producer 핵심 설정 — acks, batch.size, linger.ms, compression |
| `consumer-config.md` | Consumer 핵심 설정 — fetch.min.bytes, max.poll.records, session.timeout |
| `exactly-once-semantics.md` | Exactly-Once 시맨틱 — Idempotent Producer, Transactional API |

### docs/operations/
| 파일 | 주제 |
|------|------|
| `topic-management.md` | Topic 관리 — 생성/수정/삭제, 파티션 증가, 설정 변경 |
| `consumer-lag-management.md` | Consumer Lag 관리 — Lag 측정, 대응 전략, 오프셋 리셋 절차 |
| `kafka-rebalance.md` | Partition Rebalance — Reassignment, Throttle, 운영 주의사항 |

### docs/security/
| 파일 | 주제 |
|------|------|
| `sasl-authentication.md` | SASL 인증 — SCRAM-SHA-512, Strimzi KafkaUser |
| `tls-encryption.md` | TLS 암호화 — 인증서 구성, mTLS, Strimzi 자동 인증서 관리 |
| `acl-authorization.md` | ACL 인가 — kafka-acls.sh, Strimzi KafkaUser ACL, 최소 권한 |

### docs/observability/
| 파일 | 주제 |
|------|------|
| `kafka-metrics.md` | Kafka JMX 지표 — Broker/Producer/Consumer 핵심 지표, Prometheus 연동 |
| `consumer-lag-monitoring.md` | Consumer Lag 모니터링 — Kafka Exporter, Prometheus, 알람 설정 |

---

## 추가 예정 주제 (백로그)

- `docs/core-concepts/kraft-mode.md` — KRaft 모드 (Zookeeper 제거), 마이그레이션
- `docs/operations/kafka-upgrade.md` — Kafka 버전 업그레이드 전략 (Strimzi)
- `docs/producer-consumer/schema-registry.md` — Confluent Schema Registry, Avro/Protobuf
- `docs/observability/kafka-ui.md` — Kafka UI 설치 및 활용
- `docs/operations/topic-compaction.md` — Log Compaction, Cleanup Policy
