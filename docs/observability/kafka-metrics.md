# Kafka JMX 지표

Kafka는 JMX(Java Management Extensions)를 통해 Broker, Producer, Consumer의 핵심 지표를 노출합니다. Strimzi 환경에서는 JMX Exporter를 통해 Prometheus 형식으로 수집합니다.

---

## 1. 개요

Kafka 모니터링 계층:
- **Broker 지표**: 메시지 처리량, 파티션 상태, ISR, 컨트롤러 상태
- **Producer 지표**: 전송 속도, 오류율, 배치 크기, 버퍼 사용량
- **Consumer 지표**: Fetch 속도, Lag, Rebalancing 빈도

---

## 2. 설명

### 2.1 핵심 개념

#### 핵심 Broker 지표

| JMX 지표 | Prometheus 지표 | 설명 | 알람 기준 |
|---------|----------------|------|---------|
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | `kafka_server_replicamanager_underreplicatedpartitions` | Under-Replicated 파티션 수 | > 0 |
| `kafka.controller:type=KafkaController,name=ActiveControllerCount` | `kafka_controller_kafkacontroller_activecontrollercount` | Active Controller 수 | != 1 |
| `kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec` | `kafka_server_brokertopicmetrics_bytesinpersec` | 초당 수신 바이트 | 급격한 변화 |
| `kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec` | `kafka_server_brokertopicmetrics_bytesoutpersec` | 초당 송신 바이트 | 급격한 변화 |
| `kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec` | `kafka_server_brokertopicmetrics_messagesinpersec` | 초당 메시지 수 | 기준치 ±50% |
| `kafka.network:type=RequestMetrics,name=RequestsPerSec` | `kafka_network_requestmetrics_requestspersec` | 초당 요청 수 | 급격한 감소 |
| `kafka.network:type=RequestMetrics,name=TotalTimeMs` | `kafka_network_requestmetrics_totaltimems` | 요청 처리 총 시간 | > 100ms |

#### 핵심 Producer 지표

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_producer_record_error_rate` | 레코드 전송 오류율 | > 0 |
| `kafka_producer_record_send_rate` | 초당 전송 레코드 수 | 급격한 감소 |
| `kafka_producer_request_latency_avg` | 평균 요청 지연 | > 100ms |
| `kafka_producer_buffer_available_bytes` | 사용 가능 버퍼 | 0에 근접 시 |
| `kafka_producer_batch_size_avg` | 평균 배치 크기 | `batch.size` 대비 낮으면 비효율 |
| `kafka_producer_compression_rate_avg` | 압축률 | 1.0에 근접 시 압축 비효율 |

#### 핵심 Consumer 지표

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_consumer_group_lag` | Consumer Lag (파티션별) | > 1000 for 5m |
| `kafka_consumer_group_members` | Consumer 수 | == 0 |
| `kafka_consumer_fetch_rate` | 초당 Fetch 수 | 급격한 감소 |
| `kafka_consumer_fetch_latency_avg` | 평균 Fetch 지연 | > 500ms |
| `kafka_consumer_records_consumed_rate` | 초당 처리 레코드 수 | 기준치 대비 급감 |
| `kafka_consumer_rebalance_rate_and_time_count` | Rebalance 횟수 | 주기적으로 0이 아닐 시 |

### 2.2 실무 적용 코드

#### Strimzi JMX Exporter 설정

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
  namespace: kafka
spec:
  kafka:
    metricsConfig:
      type: jmxPrometheusExporter
      valueFrom:
        configMapKeyRef:
          name: kafka-metrics-config
          key: kafka-metrics-config.yml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-metrics-config
  namespace: kafka
data:
  kafka-metrics-config.yml: |
    lowercaseOutputName: true
    rules:
      # Broker 핵심 지표
      - pattern: "kafka.server<type=ReplicaManager, name=UnderReplicatedPartitions><>Value"
        name: kafka_server_replicamanager_underreplicatedpartitions
      - pattern: "kafka.controller<type=KafkaController, name=ActiveControllerCount><>Value"
        name: kafka_controller_kafkacontroller_activecontrollercount
      - pattern: "kafka.server<type=BrokerTopicMetrics, name=BytesInPerSec><>OneMinuteRate"
        name: kafka_server_brokertopicmetrics_bytesinpersec
      - pattern: "kafka.server<type=BrokerTopicMetrics, name=MessagesInPerSec><>OneMinuteRate"
        name: kafka_server_brokertopicmetrics_messagesinpersec
      - pattern: "kafka.network<type=RequestMetrics, name=TotalTimeMs, request=(.+)><>99thPercentile"
        name: kafka_network_requestmetrics_totaltimems_p99
        labels:
          request: "$1"
```

#### Prometheus ServiceMonitor (Strimzi Kafka)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: kafka-metrics
  namespace: kafka
  labels:
    app: kafka
spec:
  selector:
    matchLabels:
      strimzi.io/kind: Kafka
  endpoints:
    - port: tcp-prometheus
      path: /metrics
      interval: 30s
```

#### Prometheus Alert Rules

```yaml
groups:
  - name: kafka-broker-alerts
    rules:
      - alert: KafkaUnderReplicatedPartitions
        expr: kafka_server_replicamanager_underreplicatedpartitions > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Under-Replicated Partition 발생: {{ $labels.instance }}"

      - alert: KafkaActiveControllerCount
        expr: kafka_controller_kafkacontroller_activecontrollercount != 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Active Controller 이상: {{ $value }}"

      - alert: KafkaProducerRecordErrorRate
        expr: kafka_producer_record_error_rate > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Producer 오류 발생: {{ $labels.client_id }}"
```

#### Grafana 대시보드 주요 패널 쿼리

```promql
# 초당 처리 메시지 수 (Broker)
sum(rate(kafka_server_brokertopicmetrics_messagesinpersec[5m])) by (topic)

# P99 요청 지연
kafka_network_requestmetrics_totaltimems_p99{request="Produce"}

# Consumer Lag 합계 (Group별)
sum(kafka_consumer_group_lag) by (group, topic)

# Under-Replicated Partition 수
kafka_server_replicamanager_underreplicatedpartitions

# Producer 버퍼 사용률
1 - (kafka_producer_buffer_available_bytes / kafka_producer_buffer_total_bytes)
```

### 2.3 Best Practice

- Broker 지표는 30초 간격, Consumer Lag은 10~15초 간격으로 수집
- `UnderReplicatedPartitions > 0`은 Critical 알람 — 즉시 대응 필요
- Grafana 공식 Kafka 대시보드 ID: `7589` (Strimzi), `721` (Confluent)
- JMX Exporter 설정에서 불필요한 지표 제외 → Prometheus 부하 감소

---

## 3. 트러블슈팅

### 3.1 JMX Exporter 메트릭 미수집

#### 증상
- Prometheus에서 Kafka 지표가 없음

#### 원인
- JMX Exporter 포트 미노출 또는 ServiceMonitor 설정 오류

#### 해결 방법
```bash
# JMX Exporter 포트 확인
kubectl get pods -n kafka -l strimzi.io/name=my-cluster-kafka -o wide
kubectl port-forward -n kafka my-cluster-kafka-0 9404:9404

# 메트릭 직접 확인
curl http://localhost:9404/metrics | grep kafka_server_replicamanager

# ServiceMonitor 확인
kubectl get servicemonitor -n kafka
kubectl describe servicemonitor kafka-metrics -n kafka
```

### 3.2 지표 레이블 불일치

#### 증상
- Grafana 대시보드에서 일부 지표가 표시되지 않음

#### 원인
- JMX Exporter 설정의 `pattern`과 실제 JMX 지표 이름 불일치

#### 해결 방법
```bash
# 실제 노출된 지표 목록 확인
curl http://localhost:9404/metrics | grep "^kafka_" | cut -d'{' -f1 | sort -u
```

---

## 4. 모니터링 및 확인

```bash
# 전체 Broker 상태
kubectl get pods -n kafka -l strimzi.io/name=my-cluster-kafka

# Prometheus 타겟 상태
# Prometheus UI → Status → Targets → kafka-metrics
```

---

## 5. TIP

- Strimzi Kafka Exporter(`spec.kafkaExporter`)는 Consumer Lag 전용 지표 수집에 특화
- JMX Exporter CPU 사용량 과다 시 수집 지표 범위 축소 (`rules` 필터링)
- Kafka 공식 지표 목록: [Kafka Monitoring](https://kafka.apache.org/documentation/#monitoring)
- Strimzi 공식 Grafana 대시보드: [Strimzi Grafana](https://github.com/strimzi/strimzi-kafka-operator/tree/main/examples/metrics)
