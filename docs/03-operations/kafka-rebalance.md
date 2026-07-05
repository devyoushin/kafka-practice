# Partition Rebalance (파티션 재할당)

Kafka Partition Rebalance(파티션 재할당)는 브로커 간 파티션 부하를 균등하게 분산하는 작업입니다. 브로커 추가/제거, 디스크 불균형 시 수동 재할당이 필요하며, Throttle(스로틀) 설정 없이 진행 시 운영 트래픽에 큰 영향을 미칩니다.

---

## 1. 개요

Partition Rebalance는 두 가지 맥락으로 사용됩니다:
1. **Consumer Rebalancing**: Consumer Group 내 파티션 재할당 (Consumer 추가/제거 시 자동)
2. **Partition Reassignment**: Broker 간 파티션 물리 이동 (수동 실행, 브로커 추가/축소 시)

이 문서는 **Partition Reassignment** (브로커 간 파티션 이동)을 다룹니다.

---

## 2. 설명

### 2.1 핵심 개념

#### Partition Reassignment 필요 시점

| 시나리오 | 설명 |
|---------|------|
| 브로커 추가 | 새 브로커에 파티션 분산 (자동 분산 안 됨) |
| 브로커 제거 | 제거 전 파티션을 다른 브로커로 이동 |
| 디스크 불균형 | 특정 브로커에 파티션 집중 → 균등 분산 |
| Preferred Replica Election | Leader를 Preferred Replica로 복구 |

#### Throttle 설정 중요성

```
Throttle 없이 재할당 시:
  → 브로커 간 대용량 데이터 복제 발생
  → 네트워크 대역폭 포화
  → Producer/Consumer 성능 저하

권장: 50MB/s ~ 100MB/s 이하로 스로틀 설정
```

#### 재할당 상태 확인 흐름

```
1. reassignment.json 작성 (대상 파티션 및 목적지 브로커)
2. --verify 로 현재 할당 상태 확인
3. --execute 로 재할당 시작 (Throttle 설정 포함)
4. --verify 로 진행 상황 모니터링
5. 완료 후 Throttle 해제
```

### 2.2 실무 적용 코드

#### 현재 파티션 할당 상태 확인

```bash
# Topic 파티션 Leader/Replica 확인
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --topic my-topic

# 모든 Topic Under-Replicated 파티션 확인
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --under-replicated-partitions

# 브로커별 파티션 분포 확인
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe | grep "Leader:"
```

#### 재할당 계획 생성

```bash
# 재할당 대상 Topic 목록 파일 생성
cat > topics-to-move.json << 'EOF'
{
  "topics": [
    {"topic": "my-topic"},
    {"topic": "my-topic-2"}
  ],
  "version": 1
}
EOF

# Broker 목록으로 재할당 계획 자동 생성
kafka-reassign-partitions.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --broker-list "0,1,2,3" \
  --topics-to-move-json-file topics-to-move.json \
  --generate \
  > reassignment-plan.json

# 생성된 계획 확인
cat reassignment-plan.json
```

#### 수동 재할당 계획 작성

```json
{
  "version": 1,
  "partitions": [
    {
      "topic": "my-topic",
      "partition": 0,
      "replicas": [0, 1, 2],
      "log_dirs": ["any", "any", "any"]
    },
    {
      "topic": "my-topic",
      "partition": 1,
      "replicas": [1, 2, 3],
      "log_dirs": ["any", "any", "any"]
    }
  ]
}
```

#### 재할당 실행 (Throttle 포함)

```bash
# Throttle 설정: 50MB/s = 52428800 bytes/s
kafka-reassign-partitions.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --reassignment-json-file reassignment-plan.json \
  --execute \
  --throttle 52428800

# 재할당 진행 상황 확인
kafka-reassign-partitions.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --reassignment-json-file reassignment-plan.json \
  --verify
```

#### 재할당 완료 후 Throttle 해제

```bash
# Throttle 해제 (재할당 완료 후 반드시 실행)
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --alter --entity-type brokers --entity-default \
  --delete-config leader.replication.throttled.rate,follower.replication.throttled.rate

# Topic별 Throttle 설정 해제
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --delete-config leader.replication.throttled.replicas,follower.replication.throttled.replicas
```

#### Preferred Replica Election (Leader 복구)

```bash
# 모든 파티션 Preferred Replica를 Leader로 선출
kafka-leader-election.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --election-type PREFERRED \
  --all-topic-partitions

# 특정 Topic 파티션만
kafka-leader-election.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --election-type PREFERRED \
  --topic my-topic --partition 0
```

### 2.3 Best Practice

- 재할당 시 **반드시 Throttle 설정** — 미설정 시 운영 트래픽 영향
- 재할당 완료 후 **Throttle 해제 필수** — 미해제 시 정상 복제도 제한됨
- 브로커 제거 전 해당 브로커의 모든 파티션을 다른 브로커로 이동
- 대용량 Topic 재할당은 **오프피크 시간대** 진행

---

## 3. 트러블슈팅

### 3.1 재할당 중 Producer/Consumer 성능 저하

#### 증상
- 재할당 진행 중 Producer 전송 지연 증가
- Consumer Lag 급증

#### 원인
- Throttle 미설정으로 브로커 네트워크 대역폭 포화

#### 해결 방법
```bash
# 실행 중인 재할당에 Throttle 적용 (즉시 효과)
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --alter --entity-type brokers --entity-default \
  --add-config leader.replication.throttled.rate=26214400,follower.replication.throttled.rate=26214400
# 26214400 = 25MB/s
```

### 3.2 재할당 완료 후 Throttle 미해제

#### 증상
- 재할당 완료 후에도 복제 지연 지속
- Under-Replicated Partition 발생

#### 원인
- Throttle 해제 누락

#### 해결 방법
```bash
# Broker Throttle 설정 확인
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --entity-type brokers --entity-default

# Throttle 해제
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --alter --entity-type brokers --entity-default \
  --delete-config leader.replication.throttled.rate,follower.replication.throttled.rate
```

### 3.3 재할당 도중 브로커 장애

#### 증상
- 재할당 진행 중 브로커 다운
- `--verify` 결과에 실패 파티션 표시

#### 해결 방법
```bash
# 현재 재할당 상태 확인
kafka-reassign-partitions.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --reassignment-json-file reassignment-plan.json \
  --verify

# 브로커 복구 후 재할당 재시도
# 또는 계획을 수정하여 장애 브로커 제외 후 재실행
```

---

## 4. 모니터링 및 확인

```bash
# 재할당 진행 중 브로커별 복제 트래픽 확인 (JMX)
# kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesOutPerSec

# Under-Replicated Partition 실시간 확인
watch -n 10 "kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --under-replicated-partitions"
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_server_replicamanager_underreplicatedpartitions` | Under-Replicated 파티션 | > 0 |
| `kafka_server_replicamanager_isrshrinkspersec` | ISR 축소 빈도 | > 0 |
| `kafka_server_replicamanager_isrexpandspersec` | ISR 확장 빈도 | 재할당 완료 후 0으로 수렴 |
| `kafka_network_requestmetrics_remotetimems` | 복제 요청 지연 | 급격한 증가 |

---

## 5. TIP

- Strimzi 환경에서 브로커 수 변경은 `Kafka` CR의 `spec.kafka.replicas` 수정 → Strimzi가 자동 재할당
- `kafka-reassign-partitions.sh --cancel`로 진행 중인 재할당 취소 가능
- 재할당 시간 예측: 이동 데이터 크기 ÷ Throttle 속도 (예: 100GB ÷ 50MB/s = 2000초)
- 참고: [Kafka Partition Reassignment](https://kafka.apache.org/documentation/#basic_ops_partitionassignment)
