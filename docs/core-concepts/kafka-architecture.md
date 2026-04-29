# Kafka 아키텍처

Apache Kafka(아파치 카프카)는 분산 스트리밍 플랫폼으로, Broker(브로커), Topic(토픽), Partition(파티션), Consumer Group(컨슈머 그룹)의 조합으로 수백만 msg/s 처리를 지원합니다. 고가용성과 내구성을 위해 데이터를 복제(Replication)하며, KRaft 모드에서는 Zookeeper 없이 독립적으로 동작합니다.

---

## 1. 개요

Kafka는 LinkedIn에서 개발된 분산 이벤트 스트리밍 플랫폼으로 다음 3가지 역할을 수행합니다:
- **메시지 큐**: 서비스 간 비동기 통신 (Decoupling)
- **스트림 처리**: 실시간 데이터 파이프라인
- **이벤트 저장소**: 이벤트 소싱 (Event Sourcing), 감사 로그

---

## 2. 설명

### 2.1 핵심 개념

#### 아키텍처 다이어그램

```
Producer (생산자)
    │  PRODUCE (topic, partition, key, value)
    ▼
┌─────────────────────────────────────────┐
│             Kafka Cluster               │
│                                         │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐  │
│  │Broker 0 │ │Broker 1 │ │Broker 2 │  │
│  │(Leader) │ │(Replica)│ │(Replica)│  │
│  │ Part. 0 │ │ Part. 1 │ │ Part. 2 │  │
│  └─────────┘ └─────────┘ └─────────┘  │
│                                         │
│  Controller (KRaft) ← 클러스터 메타데이터  │
└─────────────────────────────────────────┘
    │  FETCH (offset)
    ▼
Consumer Group (소비자 그룹)
    ├── Consumer 0 ← Partition 0
    ├── Consumer 1 ← Partition 1
    └── Consumer 2 ← Partition 2
```

#### 핵심 구성요소

| 구성요소 | 역할 | 설명 |
|---------|------|------|
| Broker | 메시지 저장/전달 | Kafka 서버. 클러스터는 최소 3개 브로커 권장 |
| Topic | 메시지 분류 단위 | 이름으로 구분되는 메시지 채널 |
| Partition | 병렬 처리 단위 | Topic을 N개로 분할. 순서는 파티션 내에서만 보장 |
| Offset | 메시지 위치 | 파티션 내 메시지의 순번 (0부터 증가) |
| Leader | 파티션 주 서버 | Producer/Consumer가 통신하는 브로커 |
| Follower | 파티션 복제 서버 | Leader의 데이터를 비동기 복제 |
| ISR | In-Sync Replica | Leader와 동기화된 Follower 목록 |
| Controller | 클러스터 관리자 | Leader 선출, 클러스터 메타데이터 관리 |

#### KRaft vs Zookeeper

| 항목 | Zookeeper 모드 | KRaft 모드 |
|------|------------|---------|
| 외부 의존성 | Zookeeper 클러스터 필요 | 없음 |
| 운영 복잡도 | 높음 | 낮음 |
| Controller | Zookeeper 기반 Leader 선출 | Raft 합의 알고리즘 |
| Kafka 버전 | 모든 버전 | 2.8+ (GA: 3.3+) |
| 권장 여부 | Kafka 4.0에서 제거 예정 | **권장** |

### 2.2 실무 적용 코드

#### Strimzi Kafka 클러스터 배포 (KRaft 모드)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
  namespace: kafka
spec:
  kafka:
    version: 3.7.0
    metadataVersion: "3.7-IV4"
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: tls
        port: 9093
        type: internal
        tls: true
        authentication:
          type: scram-sha-512
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      default.replication.factor: 3
      min.insync.replicas: 2
    storage:
      type: jbod
      volumes:
        - id: 0
          type: persistent-claim
          size: 100Gi
          deleteClaim: false
  entityOperator:
    topicOperator: {}
    userOperator: {}
```

#### ISR 및 파티션 상태 확인

```bash
# Topic ISR 상태 확인
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --topic <TOPIC_NAME>

# 출력 예시:
# Topic: my-topic  Partition: 0  Leader: 0  Replicas: 0,1,2  Isr: 0,1,2

# Under-Replicated Partition 확인 (0이어야 정상)
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --under-replicated-partitions
```

### 2.3 Best Practice

- **Broker 수**: 최소 3개 (Controller Quorum 충족)
- **Replication Factor**: 3 (Broker 1개 장애 허용)
- **min.insync.replicas**: 2 (데이터 유실 방지)
- **KRaft 모드**: Kafka 3.3+ 환경에서 Zookeeper 대신 KRaft 사용 권장

---

## 3. 트러블슈팅

### 3.1 Under-Replicated Partition 발생

#### 증상
- `kafka-topics.sh --describe --under-replicated-partitions` 결과가 있음
- Prometheus: `kafka_server_replicamanager_underreplicatedpartitions > 0`

#### 원인
- Follower Broker 중단 또는 네트워크 불안정
- `replica.lag.time.max.ms` 초과로 ISR에서 제외됨

#### 해결 방법
```bash
# 영향받은 Broker 상태 확인 (Strimzi)
kubectl get pods -n kafka -l strimzi.io/name=my-cluster-kafka

# Broker 로그 확인
kubectl logs my-cluster-kafka-0 -n kafka | grep -E "ERROR|WARN" | tail -30
```

> **예방책**: `min.insync.replicas=2` 설정으로 ISR 부족 시 Producer 오류 발생시켜 데이터 유실 방지

### 3.2 Controller 재선출 빈번 발생

#### 증상
- `kafka_controller_kafkacontroller_activecontrollercount` 지표가 0이 되었다가 복구
- 클러스터 메타데이터 변경이 지연됨

#### 원인
- Controller Broker의 리소스 부족 (CPU/메모리)
- GC pause로 Heartbeat 누락

#### 해결 방법
```bash
# Controller 로그 확인
kubectl logs my-cluster-kafka-0 -n kafka | grep -i "controller" | tail -20
```

### 3.3 Strimzi Kafka Pod CrashLoopBackOff

#### 증상
- Kafka Pod가 재시작을 반복하며 `CrashLoopBackOff` 상태

#### 원인
- PVC 용량 부족 (로그 디렉토리 가득 참)
- JVM 메모리 부족 (Xmx 설정 미흡)

#### 해결 방법
```bash
# Pod 로그 확인
kubectl logs my-cluster-kafka-0 -n kafka --previous | tail -50

# PVC 용량 확인
kubectl get pvc -n kafka

# 로그 보존 기간 단축 (임시 조치)
kafka-configs.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --alter --entity-type topics --entity-name <TOPIC_NAME> \
  --add-config retention.ms=86400000   # 1일로 단축
```

---

## 4. 모니터링 및 확인

```bash
# 전체 Broker 상태 확인
kubectl get pods -n kafka -l strimzi.io/name=my-cluster-kafka -o wide

# 클러스터 브로커 버전 확인
kafka-broker-api-versions.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_server_replicamanager_underreplicatedpartitions` | Under-Replicated 파티션 수 | > 0 |
| `kafka_controller_kafkacontroller_activecontrollercount` | Active Controller 수 | != 1 |
| `kafka_server_brokertopicmetrics_bytesinpersec` | 초당 수신 바이트 | 급격한 변화 |

---

## 5. TIP

- KRaft 모드에서 Controller와 Broker 역할 통합 가능 — 소규모 클러스터는 통합 권장
- Strimzi 0.36+ 부터 KRaft 모드 GA 지원
- `kafka-metadata-quorum.sh --describe` 명령어로 KRaft 쿼럼 상태 확인 가능
- 참고: [Apache Kafka KRaft](https://kafka.apache.org/documentation/#kraft)
