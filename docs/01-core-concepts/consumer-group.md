# Consumer Group

Consumer Group(컨슈머 그룹)은 Kafka에서 메시지를 병렬 처리하는 단위입니다. 동일 Group ID를 가진 Consumer들이 Topic의 파티션을 분담하여 처리하며, Offset(오프셋)을 통해 처리 진행 상황을 추적합니다.

---

## 1. 개요

Consumer Group의 핵심 동작:
- 파티션은 동시에 1개의 Consumer에게만 할당 (순서 보장)
- Consumer 수 > 파티션 수이면 초과 Consumer는 유휴 상태
- Consumer 추가/제거 시 리밸런싱(Rebalancing) 발생하여 파티션 재할당

---

## 2. 설명

### 2.1 핵심 개념

#### 파티션 할당 전략 (Partition Assignment Strategy)

| 전략 | 동작 방식 | 리밸런싱 영향 | 권장 여부 |
|------|---------|------------|---------|
| `RangeAssignor` | Topic별 파티션 순서대로 분배 | 전체 중단 | 비권장 |
| `RoundRobinAssignor` | 전체 파티션 Round Robin 분배 | 전체 중단 | 비권장 |
| `StickyAssignor` | 리밸런싱 시 기존 할당 최대한 유지 | 전체 중단 | 보통 |
| `CooperativeStickyAssignor` | 증분 리밸런싱 — 영향 파티션만 재할당 | 부분적 | **권장** |

#### 오프셋 커밋 방식

| 방식 | 설정 | 특징 | 위험 |
|------|------|------|------|
| 자동 커밋 | `enable.auto.commit=true` | 간편함 | 처리 완료 전 커밋 → 메시지 유실 가능 |
| 수동 커밋 (동기) | `commitSync()` | 처리 완료 후 커밋 보장 | 느림 (RTT 대기) |
| 수동 커밋 (비동기) | `commitAsync()` | 빠름 | 실패 시 재시도 복잡 |

> **운영 환경 권장**: `enable.auto.commit=false` + `commitSync()`

#### 리밸런싱 트리거 조건

| 조건 | 원인 | 영향 |
|------|------|------|
| Consumer 추가 | 스케일 아웃 | 정상 리밸런싱 |
| Consumer 제거 | 정상 종료 | 정상 리밸런싱 |
| Heartbeat 실패 | `session.timeout.ms` 초과 | 비정상 리밸런싱 |
| 처리 시간 초과 | `max.poll.interval.ms` 초과 | **가장 흔한 원인** |
| 파티션 수 변경 | 파티션 추가 | 정상 리밸런싱 |

### 2.2 실무 적용 코드

#### Consumer 설정 (권장)

```properties
# Consumer Group 설정
group.id=my-service-consumer-group

# 파티션 할당 전략 (증분 리밸런싱)
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor

# 오프셋 커밋 (수동)
enable.auto.commit=false

# 타임아웃 설정 (리밸런싱 방지)
session.timeout.ms=30000          # Heartbeat 없을 때 Consumer 사망으로 판단
heartbeat.interval.ms=10000       # Heartbeat 전송 주기 (session.timeout의 1/3)
max.poll.interval.ms=300000       # 처리 시간 상한

# 폴링 설정
max.poll.records=500              # 1회 poll()에서 가져올 최대 메시지 수
fetch.min.bytes=1                 # 최소 fetch 크기
```

#### Consumer Group 상태 확인

```bash
# Consumer Group 상태 확인
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-consumer-group

# 출력 예시:
# GROUP                       TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# my-service-consumer-group  my-topic  0          1000            1000            0
# my-service-consumer-group  my-topic  1          980             1000            20

# 모든 Consumer Group 목록
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --list

# Dead Consumer Group 확인 (EMPTY 상태)
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --all-groups | grep "EMPTY\|Dead"
```

### 2.3 Best Practice

- `CooperativeStickyAssignor` 사용으로 리밸런싱 영향 최소화
- `max.poll.interval.ms`는 실제 최대 처리 시간의 1.5배 이상으로 설정
- `max.poll.records`를 낮춰 단위 처리 시간 감소 → `max.poll.interval.ms` 초과 방지
- 처리 완료 후 `commitSync()` 호출 (at-least-once 보장)

---

## 3. 트러블슈팅

### 3.1 리밸런싱 폭풍 (Rebalancing Storm)

#### 증상
- Consumer 로그에 지속적인 `Rebalancing...` 메시지
- Lag이 처리되지 않고 Consumer Group 상태가 `REBALANCING`으로 유지

#### 원인
- `max.poll.interval.ms` 내에 처리를 완료하고 `poll()`을 호출하지 못함
- Consumer 처리 로직이 느림 (DB 지연, 외부 API 호출 등)

#### 해결 방법
```bash
# Consumer Group 상태 확인
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-consumer-group
```

```properties
# 설정 조정 (두 가지 중 택일)
# 1. max.poll.interval.ms 증가 (처리 시간 허용)
max.poll.interval.ms=600000

# 2. max.poll.records 감소 (단위 처리량 줄임)
max.poll.records=100
```

> **예방책**: `max.poll.records`를 낮게 설정하여 단위 처리 시간을 줄임

### 3.2 Consumer가 파티션을 할당받지 못함

#### 증상
- Consumer Group에 Consumer가 있지만 일부 Consumer의 파티션 할당이 없음

#### 원인
- Consumer 수 > 파티션 수 → 초과 Consumer는 유휴 상태

#### 해결 방법
```bash
# 파티션 수 확인
kafka-topics.sh --bootstrap-server <BOOTSTRAP_SERVER> \
  --describe --topic <TOPIC_NAME>

# 파티션 수 부족 시 증가
kafka-topics.sh --bootstrap-server <BOOTSTRAP_SERVER> \
  --alter --topic <TOPIC_NAME> --partitions <NEW_COUNT>
```

### 3.3 오프셋 커밋 실패로 메시지 중복 처리

#### 증상
- Consumer 재시작 후 이미 처리한 메시지가 다시 처리됨

#### 원인
- `enable.auto.commit=true` 환경에서 자동 커밋 주기 이전에 비정상 종료
- 오프셋 커밋 전 처리 완료되지 않은 상태에서 Consumer 종료

#### 해결 방법
- `enable.auto.commit=false`로 전환
- 처리 완료 후 `commitSync()` 호출
- 멱등성(Idempotent) 처리 로직 구현 (중복 처리 허용 설계)

---

## 4. 모니터링 및 확인

```bash
# Consumer Lag 지속 모니터링
watch -n 5 "kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-consumer-group"

# Consumer Group 삭제 (Empty 상태에서만 가능)
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --delete --group my-service-consumer-group
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_consumer_group_lag` | Consumer Lag (파티션별) | > 1000 for 5m |
| `kafka_consumer_group_lag_sum` | 전체 Lag 합계 | 서비스별 임계값 |
| `kafka_consumer_group_members` | Consumer 수 | == 0 (Dead Group) |

---

## 5. TIP

- `CooperativeStickyAssignor`는 Kafka 2.4+ 지원. 기존 Consumer와 혼용 시 점진적 마이그레이션 필요
- 오프셋 리셋: `kafka-consumer-groups.sh --reset-offsets` (Consumer 중지 후 실행)
- Dead Consumer Group 정리: `kafka-consumer-groups.sh --delete --group <GROUP_ID>`
- 참고: [Kafka Consumer Configuration](https://kafka.apache.org/documentation/#consumerconfigs)
