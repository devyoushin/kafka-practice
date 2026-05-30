# Consumer Lag 모니터링

Consumer Lag(컨슈머 랙) 모니터링은 Kafka 운영에서 가장 중요한 관측 지표입니다. Strimzi Kafka Exporter를 통해 파티션별 Lag을 Prometheus로 수집하고, Grafana와 AlertManager로 가시화 및 알람을 구성합니다.

---

## 1. 개요

Consumer Lag 모니터링 스택:
- **Strimzi Kafka Exporter**: Consumer Group Lag 전용 Prometheus 지표 수집
- **Prometheus**: 지표 저장 및 알람 평가
- **Grafana**: Lag 시각화 대시보드
- **AlertManager**: Slack/PagerDuty 알람 발송

---

## 2. 설명

### 2.1 핵심 개념

#### Kafka Exporter vs JMX Exporter

| 비교 항목 | Kafka Exporter | JMX Exporter |
|---------|---------------|-------------|
| 수집 대상 | Consumer Group Lag, Topic 오프셋 | Broker/Producer/Consumer JMX 지표 전체 |
| 수집 방식 | Kafka Admin API | JMX |
| Consumer Lag 지원 | 전문화 (파티션별) | 미지원 (별도 설정 필요) |
| 설정 복잡도 | 낮음 | 높음 |
| 권장 사용 | Consumer Lag 모니터링 | Broker 내부 지표 모니터링 |

#### Kafka Exporter 주요 지표

| 지표 | 설명 | 알람 기준 |
|------|------|---------|
| `kafka_consumer_group_lag` | 파티션별 Consumer Lag | > 1000 for 5m |
| `kafka_consumer_group_lag_sum` | Consumer Group 전체 Lag 합계 | 서비스별 임계값 |
| `kafka_consumer_group_current_offset` | 현재 Consumer 오프셋 | - |
| `kafka_consumer_group_members` | Consumer Group 멤버 수 | == 0 |
| `kafka_topic_partition_current_offset` | Topic 파티션 현재 오프셋 | - |
| `kafka_topic_partition_oldest_offset` | Topic 파티션 가장 오래된 오프셋 | - |

### 2.2 실무 적용 코드

#### Strimzi Kafka Exporter 활성화

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
  namespace: kafka
spec:
  kafka:
    # ... 생략
  kafkaExporter:
    topicRegex: ".*"              # 모든 Topic 모니터링
    groupRegex: ".*"              # 모든 Consumer Group 모니터링
    resources:
      requests:
        memory: 64Mi
        cpu: "200m"
      limits:
        memory: 128Mi
        cpu: "500m"
    readinessProbe:
      initialDelaySeconds: 15
      timeoutSeconds: 5
    livenessProbe:
      initialDelaySeconds: 15
      timeoutSeconds: 5
```

```bash
# Kafka Exporter Pod 확인
kubectl get pods -n kafka -l strimzi.io/name=my-cluster-kafka-exporter

# 메트릭 직접 확인
kubectl port-forward -n kafka \
  $(kubectl get pods -n kafka -l strimzi.io/name=my-cluster-kafka-exporter -o jsonpath='{.items[0].metadata.name}') \
  9308:9308

curl http://localhost:9308/metrics | grep kafka_consumer_group_lag
```

#### Prometheus ServiceMonitor (Kafka Exporter)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: kafka-exporter
  namespace: kafka
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      strimzi.io/kind: Kafka
      strimzi.io/name: my-cluster-kafka-exporter
  endpoints:
    - port: tcp-prometheus
      path: /metrics
      interval: 15s   # Lag은 15초 간격 수집 권장
```

#### Prometheus Alert Rules (Consumer Lag)

```yaml
groups:
  - name: kafka-consumer-lag
    rules:
      # Lag 임계값 초과
      - alert: KafkaConsumerLagHigh
        expr: |
          kafka_consumer_group_lag{
            group="my-service-consumer-group"
          } > 1000
        for: 5m
        labels:
          severity: warning
          team: platform
        annotations:
          summary: "Consumer Lag 임계값 초과"
          description: |
            Group: {{ $labels.group }}
            Topic: {{ $labels.topic }}
            Partition: {{ $labels.partition }}
            Lag: {{ $value }}

      # Consumer Group 멤버 0 (서비스 중단)
      - alert: KafkaConsumerGroupDead
        expr: |
          kafka_consumer_group_members{
            group=~"my-service-.*"
          } == 0
        for: 1m
        labels:
          severity: critical
          team: platform
        annotations:
          summary: "Consumer Group 멤버 없음 (서비스 중단)"
          description: "Group {{ $labels.group }}에 Consumer가 없습니다."

      # Lag 증가율 급증
      - alert: KafkaConsumerLagIncreasing
        expr: |
          increase(kafka_consumer_group_lag[10m]) > 5000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Consumer Lag 급증"
          description: |
            10분간 Lag 증가: {{ $value }}
            Group: {{ $labels.group }}, Topic: {{ $labels.topic }}
```

#### Grafana 대시보드 쿼리

```promql
# Consumer Group별 전체 Lag
sum(kafka_consumer_group_lag{group="my-service-consumer-group"}) by (topic)

# 파티션별 Lag (히트맵)
kafka_consumer_group_lag{group="my-service-consumer-group"}

# Lag 증가 속도 (초당)
rate(kafka_consumer_group_lag{group="my-service-consumer-group"}[5m])

# Consumer Group 멤버 수
kafka_consumer_group_members{group=~"my-service-.*"}

# 특정 Group의 최대 Lag 파티션
topk(5, kafka_consumer_group_lag{group="my-service-consumer-group"})

# Log-End-Offset vs Current-Offset 비교
kafka_topic_partition_current_offset{topic="my-topic"}
kafka_consumer_group_current_offset{group="my-service-consumer-group", topic="my-topic"}
```

#### AlertManager 설정 (Slack)

```yaml
receivers:
  - name: kafka-lag-slack
    slack_configs:
      - channel: "#kafka-alerts"
        api_url: "<SLACK_WEBHOOK_URL>"
        title: "{{ .GroupLabels.alertname }}"
        text: |
          {{ range .Alerts }}
          *{{ .Annotations.summary }}*
          {{ .Annotations.description }}
          {{ end }}

route:
  group_by: ["alertname", "group"]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 1h
  routes:
    - match:
        team: platform
      receiver: kafka-lag-slack
```

### 2.3 Best Practice

- Lag 알람은 **파티션별**로 설정 — 특정 파티션의 Consumer 장애 조기 감지
- Lag **절대값** 알람과 **증가율** 알람을 함께 운용 (급증 vs 지속 증가 구분)
- Consumer Group `members == 0` 알람은 Critical → PagerDuty 연동
- Lag SLO 예시: "P99 Lag < 5000, 10분 이내 Lag 0 복구"

---

## 3. 트러블슈팅

### 3.1 Kafka Exporter 메트릭 누락

#### 증상
- `kafka_consumer_group_lag` 지표가 Prometheus에 없음

#### 원인
- Kafka Exporter Pod 비정상 종료
- `groupRegex` 패턴이 대상 Consumer Group과 불일치

#### 해결 방법
```bash
# Kafka Exporter 상태 확인
kubectl get pods -n kafka -l strimzi.io/name=my-cluster-kafka-exporter
kubectl logs -n kafka -l strimzi.io/name=my-cluster-kafka-exporter | tail -20

# groupRegex 수정 (Kafka CR)
kubectl edit kafka my-cluster -n kafka
# kafkaExporter.groupRegex: ".*"  # 모든 Group
```

### 3.2 Lag은 있지만 알람이 발송되지 않음

#### 증상
- Grafana에서 Lag이 임계값 초과하지만 알람 없음

#### 원인
- Prometheus Alert Rule `for` 조건 미충족 (순간 초과)
- AlertManager 라우팅 설정 오류

#### 해결 방법
```bash
# Prometheus Alert 상태 확인
# Prometheus UI → Alerts → KafkaConsumerLagHigh 상태 확인

# AlertManager 상태 확인
kubectl port-forward -n monitoring svc/alertmanager 9093:9093
# AlertManager UI → Status 확인
```

### 3.3 Lag이 0인데 알람 발생

#### 증상
- Lag = 0이지만 `KafkaConsumerLagHigh` 알람 지속

#### 원인
- Prometheus 데이터 수집 지연으로 이전 값이 평가에 사용됨
- Kafka Exporter와 Consumer Group 오프셋 불일치

#### 해결 방법
```bash
# Prometheus 쿼리로 현재 값 직접 확인
kafka_consumer_group_lag{group="my-service-consumer-group"}

# AlertManager에서 알람 수동 Silence
# AlertManager UI → Silences → Add Silence
```

---

## 4. 모니터링 및 확인

```bash
# CLI로 실시간 Lag 확인
watch -n 5 "kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --describe --group my-service-consumer-group"

# Kafka Exporter 메트릭 직접 확인
curl http://<kafka-exporter-ip>:9308/metrics | \
  grep kafka_consumer_group_lag | grep my-service-consumer-group
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_consumer_group_lag` | 파티션별 Lag | > 1000 for 5m |
| `kafka_consumer_group_lag_sum` | Group 전체 Lag 합계 | 서비스별 임계값 |
| `kafka_consumer_group_members` | Consumer 수 | == 0 |
| `kafka_consumer_group_current_offset` | 현재 오프셋 (처리 속도 추적) | - |

---

## 5. TIP

- Strimzi Kafka Exporter는 `spec.kafkaExporter` 설정만으로 자동 배포 — 별도 Helm 차트 불필요
- Grafana 공식 Strimzi 대시보드 ID: `7589` (Consumer Lag 포함)
- Lag 모니터링 주기: Consumer SLA에 따라 10~30초 권장
- 참고: [Strimzi Kafka Exporter](https://strimzi.io/docs/operators/latest/deploying.html#assembly-kafka-exporter-str)
