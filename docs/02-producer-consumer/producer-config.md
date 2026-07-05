# Producer 핵심 설정

Kafka Producer(프로듀서)의 핵심 설정은 처리량(Throughput)과 안정성(Reliability) 사이의 트레이드오프를 결정합니다. `acks`, `batch.size`, `linger.ms` 설정이 특히 중요합니다.

---

## 1. 개요

Producer는 메시지를 Kafka Broker로 전송하는 클라이언트입니다. 기본 설정으로도 동작하지만, 운영 환경에서는 데이터 유실 방지와 처리량 최적화를 위한 명시적 설정이 필요합니다.

---

## 2. 설명

### 2.1 핵심 개념

#### 핵심 설정 표

| 설정 | 기본값 | 권장값 | 설명 |
|------|--------|--------|------|
| `acks` | `1` | `all` | 브로커 응답 대기 수준 |
| `enable.idempotence` | `false` | `true` | 중복 없는 전송 보장 |
| `max.in.flight.requests.per.connection` | `5` | `5` | 동시 미완료 요청 수 (idempotence와 함께) |
| `batch.size` | `16384` (16KB) | `65536` (64KB) | 배치 크기 (처리량 향상) |
| `linger.ms` | `0` | `5` | 배치 대기 시간 (ms) |
| `compression.type` | `none` | `lz4` | 메시지 압축 |
| `retries` | `2147483647` | 기본값 | 재시도 횟수 |
| `delivery.timeout.ms` | `120000` | `120000` | 전송 총 타임아웃 |

#### acks 설정별 비교

| acks | 설명 | 데이터 유실 위험 | 지연 |
|------|------|---------------|------|
| `0` | 응답 대기 없음 | 매우 높음 | 최저 |
| `1` | Leader 확인만 | 중간 (Follower 복제 전 Leader 장애 시) | 중간 |
| `all` | 모든 ISR 확인 | 최소 (min.insync.replicas 설정에 따름) | 높음 |

> **운영 환경 권장**: `acks=all` + `enable.idempotence=true` + `min.insync.replicas=2`

### 2.2 실무 적용 코드

#### 안정성 우선 설정 (운영 환경)

```properties
bootstrap.servers=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="<USERNAME>" \
  password="<PASSWORD>";

# 안정성 설정
acks=all                                    # 모든 ISR 응답 대기
enable.idempotence=true                     # 정확히 한 번 전송 (중복 방지)
max.in.flight.requests.per.connection=5     # idempotence 활성화 시 최대 5

# 처리량 설정
batch.size=65536                            # 64KB 배치
linger.ms=5                                 # 5ms 대기 후 배치 전송
compression.type=lz4                        # lz4 압축 (속도/압축률 균형)

# 재시도 설정
retries=2147483647                          # 최대 재시도
delivery.timeout.ms=120000                  # 총 전송 타임아웃 2분
```

#### 처리량 우선 설정 (대용량 배치)

```properties
bootstrap.servers=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092

# 처리량 극대화
acks=1                      # Leader 확인만 (속도 우선)
batch.size=1048576          # 1MB 배치
linger.ms=50                # 50ms 대기
compression.type=snappy     # snappy 압축 (CPU 효율)
buffer.memory=67108864      # 64MB 버퍼
```

#### Strimzi KafkaUser (Producer ACL)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: my-producer
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
```

### 2.3 Best Practice

- `acks=all` + `enable.idempotence=true` 조합은 Exactly-once에 가장 가까운 보장
- `linger.ms=0`은 메시지를 즉시 전송하지만 처리량 저하 → 5~10ms로 설정 권장
- `compression.type=lz4`는 속도와 압축률 균형이 좋아 일반적으로 권장
- `buffer.memory`가 가득 차면 `max.block.ms` 동안 블로킹 후 `TimeoutException` 발생

---

## 3. 트러블슈팅

### 3.1 메시지 유실 (acks=1 또는 acks=0)

#### 증상
- Producer는 성공 응답을 받았지만 Consumer에서 메시지를 찾을 수 없음
- Leader Broker 장애 직후 일부 메시지 미수신

#### 원인
- `acks=1`: Leader가 응답 후 Follower에 복제되기 전 Leader 장애 발생
- `acks=0`: 전송 즉시 성공 처리, 브로커 수신 여부 확인 없음

#### 해결 방법
```properties
# 즉시 적용
acks=all
enable.idempotence=true
```

### 3.2 BufferExhaustedException

#### 증상
```
org.apache.kafka.clients.producer.BufferExhaustedException:
Failed to allocate memory within the configured max blocking time
```

#### 원인
- `buffer.memory` 가득 참 (기본 32MB)
- Broker 응답 지연으로 배치가 쌓임

#### 해결 방법
```properties
# 버퍼 메모리 증가
buffer.memory=134217728    # 128MB로 증가

# 또는 max.block.ms 증가 (대기 허용)
max.block.ms=60000         # 60초 대기
```

### 3.3 전송 타임아웃 (TimeoutException)

#### 증상
```
org.apache.kafka.common.errors.TimeoutException:
Expiring 1 record(s) for my-topic-0: 120001 ms has passed
```

#### 원인
- `delivery.timeout.ms` 내에 메시지 전송 완료되지 않음
- Broker 응답 지연 또는 재시도 반복

#### 해결 방법
```bash
# Broker 상태 확인
kubectl get pods -n kafka -l strimzi.io/name=my-cluster-kafka

# Network Policy 확인 (Kafka 포트 접근 가능한지)
kubectl describe networkpolicy -n kafka
```

---

## 4. 모니터링 및 확인

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_producer_record_error_rate` | 레코드 전송 오류율 | > 0 |
| `kafka_producer_record_send_rate` | 초당 전송 레코드 수 | 급격한 감소 |
| `kafka_producer_request_latency_avg` | 평균 요청 지연 | > 100ms |
| `kafka_producer_buffer_available_bytes` | 사용 가능한 버퍼 | 0에 근접 시 |

---

## 5. TIP

- `enable.idempotence=true` 설정 시 `acks=all`, `max.in.flight.requests.per.connection<=5`, `retries>0` 자동 강제
- `lz4` 압축은 CPU 사용량이 낮고 압축률이 적당해 일반적으로 가장 권장
- Transactional API가 필요하면 `transactional.id` 추가 설정 필요 (`exactly-once-semantics.md` 참조)
- 참고: [Kafka Producer Configuration](https://kafka.apache.org/documentation/#producerconfigs)
