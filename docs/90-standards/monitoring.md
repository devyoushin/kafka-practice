# 모니터링 작성 기준 (Monitoring Guidelines)

Kafka 관련 문서에서 모니터링/확인 섹션 작성 시 따라야 할 기준입니다.

---

## 1. 모니터링 섹션 필수 포함 항목

| 항목 | 포함 조건 |
|------|----------|
| `kafka-consumer-groups.sh` 진단 명령어 | Consumer 관련 모든 문서 |
| `kafka-topics.sh --describe` | Topic/Partition 관련 문서 |
| Prometheus 지표명 | 지표 관련 문서 |
| JMX MBean 이름 | 성능/운영 관련 문서 |

## 2. 핵심 진단 명령어

```bash
# Consumer Lag 확인
kafka-consumer-groups.sh --bootstrap-server <BOOTSTRAP_SERVER> \
  --describe --group <GROUP_ID>

# 모든 Consumer Group Lag 확인
kafka-consumer-groups.sh --bootstrap-server <BOOTSTRAP_SERVER> \
  --describe --all-groups

# Topic 상태 확인 (ISR, Leader)
kafka-topics.sh --bootstrap-server <BOOTSTRAP_SERVER> \
  --describe --topic <TOPIC_NAME>

# Under-Replicated Partition 확인
kafka-topics.sh --bootstrap-server <BOOTSTRAP_SERVER> \
  --describe --under-replicated-partitions
```

## 3. 핵심 Prometheus 지표

### Broker 지표
| 지표 | 설명 | 알람 기준 예시 |
|------|------|--------------|
| `kafka_server_replicamanager_underreplicatedpartitions` | Under-Replicated 파티션 수 | > 0 |
| `kafka_controller_kafkacontroller_activecontrollercount` | Active Controller 수 | != 1 |
| `kafka_server_brokertopicmetrics_bytesinpersec` | 초당 수신 바이트 | 급격한 변화 |
| `kafka_network_requestmetrics_requestspersec` | 초당 요청 수 | 급격한 변화 |

### Consumer Group 지표
| 지표 | 설명 | 알람 기준 예시 |
|------|------|--------------|
| `kafka_consumer_group_lag` | Consumer Lag (메시지 수) | > 1000 for 5m |
| `kafka_consumer_group_lag_sum` | 전체 Lag 합계 | 서비스별 임계값 |
| `kafka_consumer_group_members` | Consumer 수 | 0 (Consumer 없음) |

## 4. Consumer Lag 알람 PromQL 예시

```promql
# Consumer Lag이 1000을 초과하는 그룹 탐지
kafka_consumer_group_lag{} > 1000

# Consumer가 0명인 그룹 탐지 (Dead Consumer Group)
kafka_consumer_group_members{} == 0

# Under-Replicated Partition 알람
kafka_server_replicamanager_underreplicatedpartitions{} > 0
```

## 5. Strimzi Kafka Exporter

```yaml
# Kafka CR에 Exporter 활성화
spec:
  kafkaExporter:
    topicRegex: ".*"
    groupRegex: ".*"
    resources:
      requests:
        cpu: 200m
        memory: 64Mi
```
