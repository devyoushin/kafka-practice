# Kafka 코드 작성 규칙 (Kafka Code Conventions)

이 저장소에서 Kafka CLI 및 Strimzi YAML 예시 코드 작성 시 따라야 할 규칙입니다.

---

## 1. Kafka CLI 공통 규칙

### 기본 형식
```bash
kafka-topics.sh \
  --bootstrap-server <BOOTSTRAP_SERVER> \
  --topic <TOPIC_NAME> \
  --command

kafka-consumer-groups.sh \
  --bootstrap-server <BOOTSTRAP_SERVER> \
  --group <GROUP_ID> \
  --command
```

- `--bootstrap-server` 항상 명시 (환경 변수 의존 금지)
- 긴 명령어는 `\`로 줄 바꿈하여 가독성 확보
- 플레이스홀더: `<BOOTSTRAP_SERVER>`, `<TOPIC_NAME>`, `<GROUP_ID>`, `<NAMESPACE>`

### Kubernetes Pod에서 실행 시
```bash
kubectl exec -it <KAFKA_POD> -n <NAMESPACE> -- \
  kafka-topics.sh --bootstrap-server <BOOTSTRAP_SERVER> \
  --list
```

### 환경 변수 설정 (Runbook 상단)
```bash
export BOOTSTRAP_SERVER="my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
export NAMESPACE="kafka"
export KAFKA_POD=$(kubectl get pod -n ${NAMESPACE} \
  -l strimzi.io/name=my-cluster-kafka \
  -o jsonpath='{.items[0].metadata.name}')
```

## 2. Strimzi Kafka CR 규칙

```yaml
apiVersion: kafka.strimzi.io/v1beta2   # v1beta1 대신 v1beta2 사용
kind: Kafka
metadata:
  name: <CLUSTER_NAME>
  namespace: <NAMESPACE>               # namespace 항상 명시
spec:
  kafka:
    version: 3.7.0
    replicas: 3
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      default.replication.factor: 3
      min.insync.replicas: 2
```

## 3. KafkaTopic CR 규칙

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: <TOPIC_NAME>
  namespace: <NAMESPACE>
  labels:
    strimzi.io/cluster: <CLUSTER_NAME>   # 필수 레이블
spec:
  partitions: <PARTITION_COUNT>
  replicas: 3
  config:
    retention.ms: "604800000"            # 7일
    min.insync.replicas: "2"
```

## 4. KafkaUser CR 규칙

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: <USERNAME>
  namespace: <NAMESPACE>
  labels:
    strimzi.io/cluster: <CLUSTER_NAME>
spec:
  authentication:
    type: scram-sha-512                  # plain 사용 금지 (운영 환경)
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: <TOPIC_NAME>
          patternType: literal
        operations:
          - Read
          - Describe
        host: "*"
```

## 5. Producer/Consumer Properties 형식

```properties
# 필수 공통 설정
bootstrap.servers=<BOOTSTRAP_SERVER>
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="<USERNAME>" \
  password="<PASSWORD>";

# Producer 추가 설정
acks=all
enable.idempotence=true
compression.type=lz4

# Consumer 추가 설정
group.id=<GROUP_ID>
auto.offset.reset=earliest
enable.auto.commit=false
```

## 6. 환경 설정

- 예시의 기본 네임스페이스: `kafka` (Kafka 클러스터), `kafka-apps` (앱)
- 클러스터 이름 컨벤션: `my-cluster`
- Bootstrap Server: `my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092`
- Strimzi 버전: `0.42.x` (Kafka 3.7.x 지원)
