# Topic 관리

Kafka Topic(토픽) 관리는 Strimzi 환경에서 KafkaTopic CR(커스텀 리소스)을 통한 선언적 관리를 권장합니다. CLI(`kafka-topics.sh`)는 즉시 확인/변경이 필요한 운영 상황에서 보조 수단으로 활용합니다.

---

## 1. 개요

Topic 관리의 핵심 원칙:
- **선언적 관리**: Strimzi KafkaTopic CR → GitOps 적용 가능
- **파티션 수 증가만 가능**: 감소 불가 → 초기 설계 중요
- **설정 변경**: `retention.ms`, `cleanup.policy` 등은 운영 중 변경 가능
- **Topic 삭제**: Consumer 처리 완료 및 Producer 전환 후 진행

---

## 2. 설명

### 2.1 핵심 개념

#### Topic 수명주기 관리

| 작업 | Strimzi (권장) | CLI (보조) |
|------|---------------|-----------|
| 생성 | KafkaTopic CR 적용 | `kafka-topics.sh --create` |
| 파티션 증가 | CR `spec.partitions` 수정 | `kafka-topics.sh --alter` |
| 설정 변경 | CR `spec.config` 수정 | `kafka-configs.sh --alter` |
| 삭제 | CR 삭제 | `kafka-topics.sh --delete` |

#### Topic 변경 가능 여부

| 항목 | 변경 가능 | 주의사항 |
|------|----------|---------|
| 파티션 수 | 증가만 가능 | 감소 불가. 키 기반 순서 깨질 수 있음 |
| Replication Factor | 가능 (재할당 필요) | 브로커 부하 발생 |
| retention.ms | 가능 | 즉시 적용 |
| cleanup.policy | 가능 | compact → delete 전환 시 데이터 삭제 위험 |
| min.insync.replicas | 가능 | ISR 수 충족 여부 확인 필요 |

### 2.2 실무 적용 코드

#### Topic 생성 (Strimzi KafkaTopic CR)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: my-topic
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster   # 필수: 대상 클러스터
spec:
  partitions: 12
  replicas: 3
  config:
    retention.ms: "604800000"          # 7일
    min.insync.replicas: "2"
    compression.type: "lz4"
    cleanup.policy: "delete"
```

```bash
kubectl apply -f kafka-topic.yaml
kubectl get kafkatopic my-topic -n kafka
```

#### Topic 생성 (CLI)

```bash
# Topic 생성
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --create \
  --topic my-topic \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2
```

#### Topic 목록 및 상세 확인

```bash
# 전체 Topic 목록
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --list

# 특정 Topic 상세
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --topic my-topic

# 출력 예시:
# Topic: my-topic  PartitionCount: 12  ReplicationFactor: 3
# Topic: my-topic  Partition: 0  Leader: 0  Replicas: 0,1,2  Isr: 0,1,2

# Under-Replicated Partition 확인
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --under-replicated-partitions
```

#### 파티션 수 증가

```bash
# CLI로 파티션 증가
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --alter \
  --topic my-topic \
  --partitions 24

# Strimzi: KafkaTopic CR 수정 (권장)
kubectl edit kafkatopic my-topic -n kafka
# spec.partitions: 24 로 변경
```

#### Topic 설정 변경

```bash
# retention.ms 변경 (3일로 단축)
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config retention.ms=259200000

# cleanup.policy 변경 (compact로 전환)
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config cleanup.policy=compact

# 현재 설정 확인
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --entity-type topics --entity-name my-topic
```

#### Topic 삭제

```bash
# 1. Producer 트래픽 차단 (새 Topic으로 전환)
# 2. Consumer Lag 0 확인
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-consumer-group

# 3. Topic 삭제
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --delete --topic my-topic

# Strimzi: CR 삭제
kubectl delete kafkatopic my-topic -n kafka
```

### 2.3 Best Practice

- 파티션 수는 **Consumer 최대 인스턴스 수 이상**으로 초기 설정
- Strimzi 환경에서 CLI 직접 변경 시 Topic Operator와 불일치 발생 가능 → KafkaTopic CR 우선
- Topic 삭제 전 Consumer Lag = 0 확인 필수
- 운영 Topic 이름은 `{서비스}-{도메인}-{버전}` 패턴 권장 (예: `payment-order-event-v1`)

---

## 3. 트러블슈팅

### 3.1 파티션 수 감소 오류

#### 증상
```
Error: Topic currently has 12 partitions, which is higher than the requested 6.
```

#### 원인
Kafka는 파티션 수 감소를 지원하지 않음.

#### 해결 방법
```bash
# 파티션 감소 불가 → Topic 재생성
# 1. 새 Topic 생성 (파티션 수 조정)
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --create --topic my-topic-v2 --partitions 6 --replication-factor 3

# 2. Producer를 my-topic-v2로 전환
# 3. my-topic Consumer 처리 완료 (Lag = 0) 확인 후 삭제
```

### 3.2 Strimzi Topic Operator 동기화 오류

#### 증상
```
kubectl get kafkatopic my-topic -n kafka
# STATUS: NotReady
# MESSAGE: Error while syncing KafkaTopic my-topic
```

#### 원인
- KafkaTopic CR 설정과 실제 Kafka Topic 상태 불일치
- Topic Operator가 재조정(Reconcile) 중 오류

#### 해결 방법
```bash
# Topic Operator 로그 확인
kubectl logs -n kafka -l strimzi.io/kind=topic-operator | tail -30

# KafkaTopic 상태 상세 확인
kubectl describe kafkatopic my-topic -n kafka

# 강제 재조정: annotation 추가
kubectl annotate kafkatopic my-topic -n kafka \
  strimzi.io/force-replace=true
```

### 3.3 Topic 삭제 후 재생성 오류

#### 증상
```
Error: Topic 'my-topic' already exists.
```

#### 원인
- Kafka 내부에서 Topic 삭제가 완료되지 않은 상태에서 재생성 시도
- `delete.topic.enable=true` 설정 필요

#### 해결 방법
```bash
# Topic 삭제 완료 확인 (목록에서 사라질 때까지 대기)
watch -n 2 "kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --list | grep my-topic"
```

---

## 4. 모니터링 및 확인

```bash
# Topic 파티션/복제 상태 전체 확인
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe

# Strimzi Topic 목록 상태
kubectl get kafkatopic -n kafka

# 특정 Topic 메시지 수 확인
kafka-log-dirs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --topic-list my-topic \
  --describe | grep -oP '"size":\K[0-9]+'
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_server_replicamanager_underreplicatedpartitions` | Under-Replicated 파티션 | > 0 |
| `kafka_log_size` | Topic 파티션 크기 | 디스크 임계값 80% |
| `kafka_server_brokertopicmetrics_messagesinpersec` | 초당 메시지 수 | 급격한 변화 |

---

## 5. TIP

- Strimzi `KafkaTopic` CR에 `strimzi.io/managed: "false"` annotation 추가 시 Topic Operator 관리 제외
- `__consumer_offsets`, `__transaction_state` 등 내부 Topic은 직접 삭제 금지
- Log Compaction Topic은 `min.cleanable.dirty.ratio`, `segment.ms`, `delete.retention.ms` 함께 설정
- 참고: [Kafka Topic Configuration](https://kafka.apache.org/documentation/#topicconfigs)
