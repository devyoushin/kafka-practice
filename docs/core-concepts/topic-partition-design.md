# Topic/Partition 설계 원칙

Topic(토픽)과 Partition(파티션) 설계는 Kafka 처리량과 순서 보장에 직접적인 영향을 미칩니다. 파티션 수는 Consumer 병렬 처리의 상한선을 결정하며, Replication Factor는 내구성을 결정합니다.

---

## 1. 개요

Partition은 Kafka의 병렬 처리 단위입니다. Consumer Group 내 각 Consumer는 하나 이상의 파티션을 담당하며, 파티션 수가 처리량의 최대치를 결정합니다. 파티션 수는 늘릴 수 있지만 **줄이는 것은 불가능**하므로 초기 설계가 중요합니다.

---

## 2. 설명

### 2.1 핵심 개념

#### 파티션 수 산정 공식

```
파티션 수 = max(처리량 목표 / 파티션당 처리량, 최대 Consumer 수)

예시:
- 목표 처리량: 100 MB/s
- 파티션당 처리량: 10 MB/s
- 최대 Consumer 수: 20
→ max(100/10, 20) = 20 파티션
```

#### 파티션 수 선택 기준

| 고려사항 | 설명 |
|---------|------|
| 처리량 확장성 | 예상 최대 Consumer 수보다 여유 있게 설정 |
| 순서 보장 | 순서가 중요한 엔티티는 동일 파티션으로 라우팅 (Message Key 활용) |
| 초기 설정 | 파티션 과다 생성 시 메모리/파일 핸들 오버헤드 증가 |
| 키 기반 라우팅 | 파티션 수 변경 시 키 → 파티션 매핑 변경됨 |

#### Replication Factor 및 min.insync.replicas

| 설정 | 권장값 | 설명 |
|------|--------|------|
| `replication.factor` | 3 | Broker 1개 장애 허용 |
| `min.insync.replicas` | 2 | acks=all 시 최소 동기화 복제본 수 |

> **⚠️ 주의**: `min.insync.replicas`가 `replication.factor`와 같으면 Broker 1개만 다운돼도 Producer 쓰기 오류 발생

#### Retention 정책 비교

| 정책 | 설정 | 적합한 사용 사례 |
|------|------|---------------|
| 시간 기반 | `retention.ms=604800000` (7일) | 일반 이벤트 로그 |
| 크기 기반 | `retention.bytes=10737418240` (10GB) | 용량 제한 필요 |
| Log Compaction | `cleanup.policy=compact` | 최신 상태만 필요 (CDC, 설정 변경 이벤트) |
| Compact+Delete | `cleanup.policy=compact,delete` | 최신 상태 + 오래된 데이터 제거 |

### 2.2 실무 적용 코드

#### KafkaTopic CR (Strimzi 권장)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: my-topic
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster   # 필수: 대상 클러스터 지정
spec:
  partitions: 12                     # 파티션 수 (향후 늘릴 수 있지만 줄일 수 없음)
  replicas: 3                        # Replication Factor
  config:
    retention.ms: "604800000"        # 7일 보존
    min.insync.replicas: "2"         # acks=all 시 최소 동기화 복제본
    compression.type: "lz4"          # Broker 측 압축
    cleanup.policy: "delete"         # delete(기본) 또는 compact
```

#### Topic 파티션 수 증가

```bash
# kafka-topics.sh로 파티션 수 증가 (감소 불가)
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --alter \
  --topic my-topic \
  --partitions 24   # 기존보다 큰 값만 허용

# Strimzi 환경: KafkaTopic CR 수정 (권장)
kubectl edit kafkatopic my-topic -n kafka
# spec.partitions: 24 로 변경
```

#### Topic 상세 확인

```bash
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --topic my-topic

# 출력 예시:
# Topic: my-topic  PartitionCount: 12  ReplicationFactor: 3
# Topic: my-topic  Partition: 0  Leader: 2  Replicas: 2,0,1  Isr: 2,0,1
```

#### Topic 설정 변경

```bash
# retention.ms 변경
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config retention.ms=259200000   # 3일로 변경

# 현재 설정 확인
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --entity-type topics --entity-name my-topic
```

### 2.3 Best Practice

- 파티션 수는 **2의 배수** 또는 **Consumer 수의 배수**로 설정 (균등 분배)
- 처음에는 적게 시작하고 필요 시 증가 (파티션은 늘릴 수 있지만 줄일 수 없음)
- Log Compaction 사용 시 `min.cleanable.dirty.ratio`, `segment.ms` 함께 설정
- Strimzi 환경에서는 CLI 대신 **KafkaTopic CR**로 선언적 관리 권장

---

## 3. 트러블슈팅

### 3.1 파티션 수 감소 시도 오류

#### 증상
```
Error: Topic currently has 12 partitions, which is higher than the requested 6.
```

#### 원인
Kafka는 파티션 수 감소를 지원하지 않음. 파티션 감소는 데이터 손실 및 Consumer 오프셋 불일치를 야기하므로 설계 단계에서 불가능하게 막아둠.

#### 해결 방법
```bash
# 파티션 감소는 불가 → Topic 재생성 필요
# 1. 새 Topic 생성
kafka-topics.sh --bootstrap-server <BOOTSTRAP_SERVER> \
  --create --topic my-topic-v2 --partitions 6 --replication-factor 3

# 2. Producer를 새 Topic으로 전환 → Consumer 처리 완료 후 기존 Topic 삭제
```

### 3.2 키 기반 메시지 순서 깨짐

#### 증상
- 파티션 수 변경 후 동일 키의 메시지가 다른 파티션으로 라우팅됨
- 이전 파티션의 데이터와 새 파티션의 데이터가 뒤섞임

#### 원인
기본 파티셔너: `hash(key) % partition_count`. 파티션 수 변경 시 매핑이 변경됨.

#### 해결 방법
- 파티션 수 변경 전 해당 Topic Consumer 처리 완료 대기
- 순서 보장이 중요한 경우 파티션 수 변경 최소화

### 3.3 min.insync.replicas 오류로 Producer 쓰기 실패

#### 증상
```
org.apache.kafka.common.errors.NotEnoughReplicasException:
Messages are rejected since there are fewer in-sync replicas than required.
```

#### 원인
- Broker 장애로 ISR 수가 `min.insync.replicas` 미만으로 감소
- `acks=all` 설정 시 ISR이 충분하지 않으면 쓰기 거부

#### 해결 방법
```bash
# ISR 상태 확인
kafka-topics.sh --bootstrap-server <BOOTSTRAP_SERVER> \
  --describe --under-replicated-partitions

# Broker 복구 후 ISR 자동 복구 대기
kubectl get pods -n kafka -l strimzi.io/name=my-cluster-kafka
```

---

## 4. 모니터링 및 확인

```bash
# 전체 Topic 목록
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --list

# 특정 Topic 상세
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --topic <TOPIC_NAME>
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_server_replicamanager_underreplicatedpartitions` | Under-Replicated 파티션 | > 0 |
| `kafka_log_logflushrateandsecs` | 로그 플러시 속도 | 급격한 증가 |

---

## 5. TIP

- 파티션 당 복제 인수 × 파티션 수 = 총 파티션 수 → Broker 메모리 계획에 반영
- `__consumer_offsets` Topic은 기본 50 파티션 (클러스터 초기화 시 결정)
- 참고: [Kafka 파티션 수 산정 가이드](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/)
