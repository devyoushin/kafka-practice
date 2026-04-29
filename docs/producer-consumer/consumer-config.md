# Consumer 핵심 설정

Kafka Consumer(컨슈머)의 핵심 설정은 Lag(지연) 관리와 리밸런싱 안정성에 직접적인 영향을 미칩니다. 특히 `max.poll.interval.ms`와 `max.poll.records`의 균형이 중요합니다.

---

## 1. 개요

Consumer는 Kafka Broker에서 메시지를 읽어 처리하는 클라이언트입니다. Consumer Group에 속하며, 파티션을 분담하여 병렬 처리합니다. 잘못된 타임아웃 설정은 리밸런싱 폭풍을 야기하므로 처리 시간에 맞는 설정이 필수입니다.

---

## 2. 설명

### 2.1 핵심 개념

#### 핵심 설정 표

| 설정 | 기본값 | 권장값 | 설명 |
|------|--------|--------|------|
| `fetch.min.bytes` | `1` | `1` | 최소 fetch 크기 (1=즉시 반환) |
| `fetch.max.wait.ms` | `500` | `500` | fetch.min.bytes 충족 대기 시간 |
| `max.poll.records` | `500` | `100~500` | 1회 poll()에서 가져올 최대 수 |
| `session.timeout.ms` | `45000` | `30000` | Heartbeat 없을 때 사망 판단 시간 |
| `heartbeat.interval.ms` | `3000` | `10000` | Heartbeat 전송 주기 |
| `max.poll.interval.ms` | `300000` | 처리 시간 × 1.5 | poll() 호출 최대 간격 |
| `auto.offset.reset` | `latest` | `earliest` | 오프셋 없을 때 시작 위치 |
| `enable.auto.commit` | `true` | `false` | 자동 오프셋 커밋 여부 |

#### 타임아웃 설정 관계

```
session.timeout.ms (30s)
    ├── heartbeat.interval.ms (10s) — 이 주기로 Heartbeat 전송
    │   (session.timeout의 1/3 이하로 설정)
    └── Heartbeat 없으면 30s 후 Consumer 사망으로 판단 → 리밸런싱

max.poll.interval.ms (300s)
    └── poll() 호출 간격이 이 시간을 초과하면 Consumer 강제 제거 → 리밸런싱
        (처리 시간이 긴 경우 가장 흔한 리밸런싱 원인)
```

#### auto.offset.reset 비교

| 값 | 동작 | 사용 사례 |
|----|------|---------|
| `earliest` | 가장 오래된 메시지부터 처리 | 새 Consumer Group 초기화 시 전체 이력 처리 |
| `latest` | 가장 최신 메시지부터 처리 | 실시간 처리 (이전 데이터 불필요) |
| `none` | 오프셋 없으면 오류 발생 | 오프셋이 항상 존재해야 하는 경우 |

### 2.2 실무 적용 코드

#### Consumer 설정 (운영 환경 권장)

```properties
bootstrap.servers=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="<USERNAME>" \
  password="<PASSWORD>";

# Consumer Group
group.id=my-service-consumer-group
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor

# 오프셋 관리 (수동 커밋 권장)
enable.auto.commit=false
auto.offset.reset=earliest

# 타임아웃 설정
session.timeout.ms=30000          # 30초
heartbeat.interval.ms=10000       # 10초 (session의 1/3)
max.poll.interval.ms=300000       # 처리 시간에 맞게 조정

# 처리량 설정
max.poll.records=500              # 1회 최대 500건
fetch.min.bytes=1                 # 즉시 반환
fetch.max.wait.ms=500             # 최대 500ms 대기
```

#### Consumer Group 상태 확인

```bash
# Lag 및 Consumer 할당 상태 확인
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-consumer-group

# 오프셋 리셋 (Consumer 중지 후 실행)
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --group my-service-consumer-group \
  --reset-offsets --to-latest \
  --execute --topic my-topic
```

### 2.3 Best Practice

- `max.poll.records` × 평균 처리 시간 < `max.poll.interval.ms` 가 되도록 설정
- `session.timeout.ms` = 3 × `heartbeat.interval.ms` 관계 유지
- `enable.auto.commit=false` + 처리 완료 후 `commitSync()` 호출 (at-least-once)
- 처리 중 외부 API 호출이 있는 경우 해당 타임아웃도 `max.poll.interval.ms`에 포함

---

## 3. 트러블슈팅

### 3.1 max.poll.interval.ms 초과로 리밸런싱

#### 증상
```
[Consumer clientId=consumer-1, groupId=my-group] This member will leave the group
because consumer poll timeout has expired.
```
- Lag이 줄지 않고 Consumer Group이 계속 REBALANCING 상태

#### 원인
- 처리 로직이 `max.poll.interval.ms`(기본 5분)를 초과
- 처리 중 DB 응답 지연, 외부 API 타임아웃 등

#### 해결 방법
```properties
# 1. 처리 시간에 맞게 max.poll.interval.ms 증가
max.poll.interval.ms=600000   # 10분

# 2. 단위 처리량 줄이기
max.poll.records=100          # 500 → 100으로 감소
```

### 3.2 session.timeout.ms 오류 (Heartbeat 누락)

#### 증상
```
[Consumer clientId=consumer-1] Attempt to heartbeat failed since group is rebalancing
```

#### 원인
- GC Pause 또는 CPU 과부하로 Heartbeat 전송 지연
- `session.timeout.ms`가 너무 짧게 설정됨

#### 해결 방법
```properties
# session.timeout.ms 증가
session.timeout.ms=60000       # 60초로 증가
heartbeat.interval.ms=20000    # 20초로 조정
```

### 3.3 오프셋 리셋 후 중복 처리

#### 증상
- 오프셋 리셋 후 Consumer가 이미 처리한 메시지를 다시 처리

#### 원인
- `--to-earliest` 리셋으로 처음부터 재처리
- 처리 로직에 멱등성(Idempotency) 없음

#### 해결 방법
- 오프셋 리셋 시 `--to-datetime` 또는 `--shift-by` 옵션으로 필요한 만큼만 리셋
- 처리 로직에 중복 처리 방지 로직 구현 (DB unique key, 처리 이력 확인 등)

```bash
# 특정 시점으로 리셋
kafka-consumer-groups.sh \
  --bootstrap-server <BOOTSTRAP_SERVER> \
  --group my-service-consumer-group \
  --reset-offsets \
  --to-datetime 2026-04-29T10:00:00.000 \
  --execute --topic my-topic
```

---

## 4. 모니터링 및 확인

```bash
# Consumer Lag 확인
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-consumer-group

# 전체 Consumer Group Lag
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --all-groups
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_consumer_group_lag` | Lag (파티션별) | > 1000 for 5m |
| `kafka_consumer_group_members` | Consumer 수 | == 0 |
| `kafka_consumer_fetch_latency_avg` | fetch 지연 | > 500ms |

---

## 5. TIP

- `fetch.min.bytes=1`은 메시지가 있으면 즉시 반환 → 저지연 필요 시 유지
- `fetch.min.bytes`를 높이면 네트워크 효율은 올라가지만 지연 증가
- Consumer 처리 실패 메시지는 DLQ(Dead Letter Queue)로 분리하여 본 처리 흐름 영향 최소화
- 참고: [Kafka Consumer Configuration](https://kafka.apache.org/documentation/#consumerconfigs)
