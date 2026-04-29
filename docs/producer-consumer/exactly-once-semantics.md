# Exactly-Once Semantics (EOS)

Kafka의 Exactly-Once Semantics(EOS)는 메시지가 정확히 한 번만 처리되도록 보장합니다. Idempotent Producer(멱등 프로듀서)와 Transactional API(트랜잭션 API)를 조합하여 구현하며, 특히 금융/결제 시스템처럼 중복 처리가 허용되지 않는 환경에 필수입니다.

---

## 1. 개요

메시지 전달 보장 수준은 세 가지로 구분됩니다:

| 보장 수준 | 설명 | 위험 | 사용 사례 |
|----------|------|------|---------|
| At-most-once | 최대 1회 전달 (유실 가능) | 메시지 유실 | 로그, 메트릭 (일부 손실 허용) |
| At-least-once | 최소 1회 전달 (중복 가능) | 중복 처리 | 일반적인 이벤트 처리 |
| Exactly-once | 정확히 1회 전달 | 성능 오버헤드 | 결제, 재고, 금융 트랜잭션 |

---

## 2. 설명

### 2.1 핵심 개념

#### EOS 구성 요소

```
Idempotent Producer (멱등 프로듀서)
    └── Producer ID(PID) + Sequence Number로 중복 쓰기 방지
        → Broker가 중복 메시지 감지 후 무시

Transactional Producer (트랜잭션 프로듀서)
    └── transactional.id로 여러 파티션에 원자적 쓰기
        → 모두 성공 또는 모두 실패

Consumer (isolation.level=read_committed)
    └── 커밋된 트랜잭션 메시지만 읽음
        → 중단된 트랜잭션 메시지 무시
```

#### Idempotent Producer 동작

| 항목 | 설명 |
|------|------|
| Producer ID (PID) | Broker가 Producer에 부여하는 고유 ID |
| Sequence Number | 파티션별 메시지 순번 (0부터 증가) |
| 중복 감지 | Broker가 동일 PID + Sequence를 받으면 ACK만 반환 (저장 안 함) |
| 재시작 시 | PID가 변경되므로 idempotence는 세션 내에서만 보장 |

#### Transactional API 흐름

```
1. initTransactions()          → Broker에 transactional.id 등록
2. beginTransaction()          → 트랜잭션 시작
3. send() × N                  → 여러 파티션/토픽에 메시지 전송
4. sendOffsetsToTransaction()  → Consumer 오프셋을 트랜잭션에 포함 (Read-Process-Write 패턴)
5. commitTransaction()         → 모든 메시지 원자적 커밋
   또는 abortTransaction()     → 트랜잭션 전체 롤백
```

#### isolation.level 비교

| 값 | 동작 | 사용 사례 |
|----|------|---------|
| `read_uncommitted` (기본) | 커밋 여부 무관하게 모든 메시지 읽음 | 일반 Consumer |
| `read_committed` | 커밋된 트랜잭션 메시지만 읽음 | EOS Consumer (필수) |

### 2.2 실무 적용 코드

#### Idempotent Producer 설정

```properties
bootstrap.servers=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="<USERNAME>" \
  password="<PASSWORD>";

# Idempotent Producer (멱등 프로듀서)
enable.idempotence=true           # 자동으로 아래 설정 강제
acks=all                          # enable.idempotence=true 시 자동 강제
retries=2147483647                # 무한 재시도 (자동 강제)
max.in.flight.requests.per.connection=5   # 최대 5 (자동 강제)
```

#### Transactional Producer 설정 및 Java 코드

```properties
# Transactional Producer 추가 설정
enable.idempotence=true
transactional.id=my-service-transactional-producer-1   # 인스턴스별 고유 ID
transaction.timeout.ms=60000                            # 트랜잭션 타임아웃 (60초)
```

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "...");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-service-transactional-producer-1");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.initTransactions();

try {
    producer.beginTransaction();

    // 여러 파티션/토픽에 메시지 전송
    producer.send(new ProducerRecord<>("topic-a", key, value));
    producer.send(new ProducerRecord<>("topic-b", key, value));

    // Consumer 오프셋을 트랜잭션에 포함 (Read-Process-Write 패턴)
    producer.sendOffsetsToTransaction(offsetsToCommit, consumerGroupMetadata);

    producer.commitTransaction();
} catch (ProducerFencedException | InvalidProducerEpochException e) {
    // 동일 transactional.id로 새 Producer가 시작됨 → 현재 Producer 종료
    producer.close();
} catch (KafkaException e) {
    producer.abortTransaction();
}
```

#### EOS Consumer 설정

```properties
# EOS Consumer — 커밋된 메시지만 읽음
group.id=my-service-eos-consumer-group
enable.auto.commit=false
isolation.level=read_committed    # 트랜잭션 커밋된 메시지만 읽음
auto.offset.reset=earliest
```

#### Strimzi KafkaUser (Transactional Producer ACL)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: my-transactional-producer
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
          name: my-topic
          patternType: literal
        operations:
          - Write
          - Describe
        host: "*"
      - resource:
          type: transactionalId
          name: my-service-transactional-producer   # transactional.id prefix
          patternType: prefix
        operations:
          - Write
        host: "*"
      - resource:
          type: group
          name: my-service-eos-consumer-group
          patternType: literal
        operations:
          - Read
        host: "*"
```

### 2.3 Best Practice

- `transactional.id`는 Producer 인스턴스마다 고유하게 설정 (Pod 이름 또는 인스턴스 번호 포함)
- EOS Consumer는 반드시 `isolation.level=read_committed` + `enable.auto.commit=false` 설정
- `transaction.timeout.ms`는 처리 로직 최대 실행 시간보다 길게 설정
- Transactional API는 성능 오버헤드 있음 → 중복 처리 불가 환경에서만 사용
- Read-Process-Write 패턴: `sendOffsetsToTransaction()`으로 Consumer 오프셋을 트랜잭션에 포함

---

## 3. 트러블슈팅

### 3.1 ProducerFencedException

#### 증상
```
org.apache.kafka.common.errors.ProducerFencedException:
Producer attempted an operation with an old epoch.
```

#### 원인
- 동일 `transactional.id`로 새 Producer 인스턴스가 시작됨
- 이전 Producer가 좀비(Zombie) 상태로 쓰기 시도

#### 해결 방법
```java
// ProducerFencedException 발생 시 현재 Producer 즉시 종료
try {
    producer.commitTransaction();
} catch (ProducerFencedException e) {
    producer.close();   // abortTransaction() 호출 불필요 — 이미 펜싱됨
    // 새 Producer 인스턴스 생성 후 재시작
}
```

> **예방책**: Kubernetes Pod 재시작 시 이전 Pod 완전 종료 후 새 Pod 시작 (Rolling Update 시 주의)

### 3.2 InvalidProducerEpochException

#### 증상
```
org.apache.kafka.common.errors.InvalidProducerEpochException:
Producer attempted to produce with an old epoch.
```

#### 원인
- `transactional.id` 재사용 시 Epoch(에포크) 불일치
- `transaction.timeout.ms` 초과 후 Broker가 트랜잭션 만료 처리

#### 해결 방법
```properties
# transaction.timeout.ms 증가 (처리 시간 충분히 확보)
transaction.timeout.ms=120000   # 2분으로 증가
```

### 3.3 EOS Consumer가 메시지를 읽지 못함

#### 증상
- `isolation.level=read_committed` 설정 후 Consumer가 메시지를 받지 못함

#### 원인
- Producer가 트랜잭션을 커밋하지 않음 (abortTransaction() 또는 비정상 종료)
- `read_committed` Consumer는 커밋되지 않은 트랜잭션 메시지를 읽지 않음

#### 해결 방법
```bash
# 트랜잭션 코디네이터 로그 확인
kubectl logs my-cluster-kafka-0 -n kafka | grep -i "transaction" | tail -20

# Consumer Group Lag 확인 (트랜잭션 커밋 대기 중인지)
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-eos-consumer-group
```

---

## 4. 모니터링 및 확인

```bash
# 트랜잭션 코디네이터 상태 확인
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --topic __transaction_state

# EOS Consumer Lag 확인
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-eos-consumer-group
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_producer_txn_abort_rate` | 트랜잭션 중단율 | > 0 |
| `kafka_producer_record_error_rate` | 레코드 오류율 | > 0 |
| `kafka_consumer_group_lag` | Consumer Lag | > 1000 for 5m |

---

## 5. TIP

- `enable.idempotence=true`만으로는 EOS 미완성 — Consumer `isolation.level=read_committed` 필수
- Kafka Streams는 내부적으로 EOS를 처리 (`processing.guarantee=exactly_once_v2`)
- `transactional.id`에 Pod 이름 포함 시 재시작마다 새 ID 생성 → Zombie 문제 없음
- EOS는 Kafka 내부에서만 보장 — 외부 DB 쓰기는 별도 멱등성 구현 필요
- 참고: [Kafka Transactions](https://kafka.apache.org/documentation/#transactions)
