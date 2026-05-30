# Consumer Lag 관리

Consumer Lag(컨슈머 랙)은 Kafka Consumer Group이 처리해야 할 남은 메시지 수입니다. Lag 증가는 Consumer 처리 지연, 리밸런싱 폭풍, 또는 Consumer 수 부족의 신호입니다. 적시 감지와 원인 파악이 중요합니다.

---

## 1. 개요

Lag = Log-End-Offset (LEO) − Current-Offset (Consumer가 커밋한 마지막 오프셋)

- Lag = 0: Consumer가 최신 메시지까지 처리 완료
- Lag 증가: Producer 속도 > Consumer 처리 속도 또는 Consumer 장애
- Lag 모니터링 주기: 5초~1분 (서비스 SLA에 따라 조정)

---

## 2. 설명

### 2.1 핵심 개념

#### Lag 증가 원인 분류

| 원인 | 증상 | 우선 대응 |
|------|------|---------|
| Consumer 처리 속도 부족 | Lag 지속 증가, Consumer 정상 동작 | Consumer 인스턴스 추가 또는 처리 로직 최적화 |
| 리밸런싱 반복 | Lag 증가 + REBALANCING 상태 | max.poll.interval.ms 또는 max.poll.records 조정 |
| Consumer 중단 | Lag 급증, Consumer Group 멤버 0 | Consumer 재기동 |
| 외부 의존성 지연 | DB/API 응답 지연으로 처리 속도 저하 | 타임아웃 조정 또는 Circuit Breaker 적용 |
| 파티션 수 부족 | Consumer 추가해도 Lag 미감소 | 파티션 수 증가 |

#### 오프셋 리셋 옵션 비교

| 옵션 | 동작 | 사용 사례 |
|------|------|---------|
| `--to-latest` | 현재 최신 오프셋으로 이동 (Lag → 0) | 오래된 Lag 포기, 최신부터 처리 |
| `--to-earliest` | 가장 오래된 오프셋으로 이동 | 전체 재처리 필요 시 |
| `--to-datetime` | 특정 시점 오프셋으로 이동 | 특정 시간 이후 재처리 |
| `--shift-by` | 현재 오프셋에서 N만큼 이동 | 일부 메시지만 건너뛰기 |
| `--to-offset` | 특정 오프셋으로 이동 | 정확한 위치 지정 |

### 2.2 실무 적용 코드

#### Consumer Lag 확인

```bash
# Consumer Group Lag 확인
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-consumer-group

# 출력 예시:
# GROUP                      TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# my-service-consumer-group  my-topic  0          1000            1050            50
# my-service-consumer-group  my-topic  1          980             1100            120
# my-service-consumer-group  my-topic  2          1010            1010            0

# 전체 Consumer Group Lag 요약
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --all-groups

# Lag 지속 모니터링 (5초 간격)
watch -n 5 "kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-consumer-group"
```

#### 오프셋 리셋 (Consumer 중지 후 실행)

```bash
# ⚠️ 반드시 Consumer를 먼저 중지한 후 실행

# 1. 최신 오프셋으로 리셋 (Lag 포기)
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --group my-service-consumer-group \
  --reset-offsets --to-latest \
  --execute --topic my-topic

# 2. 특정 시점으로 리셋
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --group my-service-consumer-group \
  --reset-offsets \
  --to-datetime 2026-04-29T10:00:00.000 \
  --execute --topic my-topic

# 3. 특정 오프셋만큼 건너뛰기 (+100 메시지)
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --group my-service-consumer-group \
  --reset-offsets \
  --shift-by 100 \
  --execute --topic my-topic --partitions 1

# 4. 리셋 전 dry-run (실제 적용 전 확인)
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --group my-service-consumer-group \
  --reset-offsets --to-latest \
  --dry-run --topic my-topic
```

#### Consumer 스케일 아웃 (Kubernetes)

```bash
# Consumer Deployment 인스턴스 증가
kubectl scale deployment my-service-consumer \
  --replicas=6 -n kafka-apps

# 파티션 수 확인 (Consumer 수 ≤ 파티션 수)
kafka-topics.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --topic my-topic | grep PartitionCount
```

#### Lag 임계값 기반 알람 (Prometheus Alert)

```yaml
groups:
  - name: kafka-consumer-lag
    rules:
      - alert: KafkaConsumerLagHigh
        expr: kafka_consumer_group_lag{group="my-service-consumer-group"} > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Consumer Lag이 1000을 초과했습니다"
          description: "Group: {{ $labels.group }}, Topic: {{ $labels.topic }}, Partition: {{ $labels.partition }}, Lag: {{ $value }}"

      - alert: KafkaConsumerGroupDead
        expr: kafka_consumer_group_members{group="my-service-consumer-group"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Consumer Group 멤버가 0입니다"
```

### 2.3 Best Practice

- Lag 알람은 절대값보다 **Lag 증가 속도(Rate)**를 기준으로 설정 (순간 급증 구분)
- 오프셋 리셋 전 반드시 `--dry-run`으로 대상 확인
- 오프셋 리셋 후 멱등성(Idempotency) 미보장 서비스는 중복 처리 주의
- Consumer 스케일 아웃 시 파티션 수 초과하지 않도록 확인

---

## 3. 트러블슈팅

### 3.1 Lag이 줄어들지 않음

#### 증상
- Consumer가 동작 중이지만 Lag이 감소하지 않음
- Consumer Group 상태가 `REBALANCING` 반복

#### 원인
- `max.poll.interval.ms` 초과로 Consumer가 강제 제거 후 재가입 반복
- 처리 로직 내 외부 API/DB 지연

#### 해결 방법
```bash
# Consumer 로그 확인 (리밸런싱 원인 파악)
kubectl logs -n kafka-apps -l app=my-service-consumer | grep -E "rebalanc|timeout|poll" | tail -20

# max.poll.records 감소 (단위 처리량 줄임)
# consumer.properties:
# max.poll.records=100        # 500 → 100으로 감소
# max.poll.interval.ms=600000 # 10분으로 증가
```

### 3.2 오프셋 리셋 후 중복 처리

#### 증상
- 오프셋 리셋 후 이미 처리한 메시지가 다시 처리됨

#### 원인
- `--to-earliest` 또는 `--shift-by -N`으로 이전 오프셋으로 이동

#### 해결 방법
- DB Unique Key, 처리 이력 테이블로 중복 방지 구현
- `--to-datetime`으로 정확한 시점 기준 리셋
- 처리 로직에 멱등성 적용

### 3.3 오프셋 리셋 실패

#### 증상
```
Error: Assignments can only be reset if the group 'my-service-consumer-group' is inactive.
```

#### 원인
- Consumer Group이 활성 상태(Consumer가 실행 중)에서 오프셋 리셋 시도

#### 해결 방법
```bash
# Consumer Deployment 중지
kubectl scale deployment my-service-consumer --replicas=0 -n kafka-apps

# Consumer Group 상태가 Empty가 될 때까지 대기
watch -n 2 "kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-consumer-group | grep -E 'STATE|EMPTY'"

# 오프셋 리셋 실행
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --group my-service-consumer-group \
  --reset-offsets --to-latest \
  --execute --topic my-topic

# Consumer 재기동
kubectl scale deployment my-service-consumer --replicas=3 -n kafka-apps
```

---

## 4. 모니터링 및 확인

```bash
# Consumer Group 전체 상태
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --all-groups | grep -v "^$"

# Empty/Dead Consumer Group 정리 대상 확인
kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --all-groups | grep "EMPTY\|Dead"
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_consumer_group_lag` | Consumer Lag (파티션별) | > 1000 for 5m |
| `kafka_consumer_group_lag_sum` | 전체 Lag 합계 | 서비스별 임계값 |
| `kafka_consumer_group_members` | Consumer 수 | == 0 (Dead Group) |
| `kafka_consumer_fetch_rate` | 초당 fetch 수 | 급격한 감소 |

---

## 5. TIP

- Kafka Exporter(`danielqsj/kafka-exporter`)를 통해 Consumer Lag Prometheus 지표 수집 가능
- Lag SLO 예시: "5분 이상 Lag 1000 초과 시 알람" → P95 처리 지연과 함께 설정
- Dead Consumer Group 정리: `kafka-consumer-groups.sh --delete --group <GROUP_ID>`
- 참고: [Kafka Consumer Group Management](https://kafka.apache.org/documentation/#basic_ops_consumer_group)
