# ACL 인가

ACL(Access Control List) 인가는 인증된 Kafka 클라이언트가 특정 리소스(Topic, Consumer Group 등)에 허용된 작업만 수행하도록 제어합니다. Strimzi에서는 KafkaUser CR의 `spec.authorization`으로 선언적 관리합니다.

---

## 1. 개요

ACL 구성 요소:
- **Principal**: 인증된 사용자 (SASL: `User:username`, mTLS: `User:CN=username`)
- **Resource**: Topic, Consumer Group, Cluster, TransactionalId
- **Operation**: Read, Write, Create, Delete, Describe, Alter, DescribeConfigs, AlterConfigs 등
- **Permission**: Allow / Deny

---

## 2. 설명

### 2.1 핵심 개념

#### 주요 리소스 및 작업

| 리소스 | 필수 작업 | 대상 |
|--------|---------|------|
| `topic` | `Read`, `Describe` | Consumer |
| `topic` | `Write`, `Describe` | Producer |
| `topic` | `Create`, `Delete`, `Alter` | 관리자 |
| `group` | `Read` | Consumer (Consumer Group 오프셋 커밋) |
| `cluster` | `Describe`, `Create` | 관리자 |
| `transactionalId` | `Write` | Transactional Producer |

#### 최소 권한 원칙 (Least Privilege)

```
Producer 최소 권한:
  - topic:{topic_name}:Write, Describe

Consumer 최소 권한:
  - topic:{topic_name}:Read, Describe
  - group:{group_id}:Read

Transactional Producer 추가 권한:
  - transactionalId:{txn_id_prefix}*:Write
  - group:{group_id}:Read (sendOffsetsToTransaction 사용 시)
```

#### patternType 비교

| patternType | 설명 | 예시 |
|-------------|------|------|
| `literal` | 정확한 이름 일치 | `my-topic` 만 허용 |
| `prefix` | 접두사 일치 | `my-` 로 시작하는 모든 토픽 |

### 2.2 실무 적용 코드

#### KafkaUser CR — Producer (최소 권한)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: payment-producer
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
          name: payment-event
          patternType: literal
        operations:
          - Write
          - Describe
        host: "*"
```

#### KafkaUser CR — Consumer (최소 권한)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: payment-consumer
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
          name: payment-event
          patternType: literal
        operations:
          - Read
          - Describe
        host: "*"
      - resource:
          type: group
          name: payment-service-consumer-group
          patternType: literal
        operations:
          - Read
        host: "*"
```

#### KafkaUser CR — 접두사 패턴 (여러 Topic)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: payment-service-producer
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
          name: payment-   # payment- 로 시작하는 모든 Topic
          patternType: prefix
        operations:
          - Write
          - Describe
        host: "*"
```

#### CLI로 ACL 조회 및 관리

```bash
# 특정 사용자 ACL 목록
kafka-acls.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --list --principal User:payment-producer

# 전체 ACL 목록
kafka-acls.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --list

# ACL 추가 (CLI — Strimzi 환경에서는 KafkaUser CR 권장)
kafka-acls.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --add \
  --allow-principal User:payment-producer \
  --operation Write --operation Describe \
  --topic payment-event

# ACL 삭제
kafka-acls.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --remove \
  --allow-principal User:payment-producer \
  --operation Write \
  --topic payment-event
```

#### Kafka CR — Authorization 활성화

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
  namespace: kafka
spec:
  kafka:
    authorization:
      type: simple              # Kafka 기본 ACL
      superUsers:               # Superuser (모든 ACL 우회)
        - User:admin
        - User:kafka-admin
```

### 2.3 Best Practice

- KafkaUser CR 한 개 = 서비스 한 개 원칙 (공유 계정 금지)
- Topic 이름 접두사 규칙 사용 시 `patternType: prefix`로 ACL 단순화
- Superuser는 관리 목적에만 한정 — 애플리케이션 계정에 Superuser 부여 금지
- ACL 변경은 KafkaUser CR 수정으로 관리 (CLI 직접 변경 시 CR과 불일치)

---

## 3. 트러블슈팅

### 3.1 ACL 권한 부족 오류 (Authorization Failed)

#### 증상
```
org.apache.kafka.common.errors.TopicAuthorizationException:
Not authorized to access topics: [payment-event]
```

#### 원인
- KafkaUser에 해당 Topic 또는 작업 권한 미설정

#### 해결 방법
```bash
# 현재 ACL 확인
kafka-acls.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --list --principal User:payment-producer

# KafkaUser CR에 필요한 ACL 추가
kubectl edit kafkauser payment-producer -n kafka
# spec.authorization.acls 에 항목 추가
```

### 3.2 Consumer Group ACL 누락

#### 증상
```
org.apache.kafka.common.errors.GroupAuthorizationException:
Not authorized to access group: payment-service-consumer-group
```

#### 원인
- Consumer KafkaUser에 `group` 리소스 Read 권한 누락

#### 해결 방법
```yaml
# KafkaUser에 group ACL 추가
spec:
  authorization:
    acls:
      - resource:
          type: group
          name: payment-service-consumer-group
          patternType: literal
        operations:
          - Read
        host: "*"
```

### 3.3 Strimzi User Operator ACL 동기화 지연

#### 증상
- KafkaUser CR은 Ready이지만 ACL이 적용되지 않음

#### 원인
- User Operator 처리 지연 또는 오류

#### 해결 방법
```bash
# User Operator 로그 확인
kubectl logs -n kafka -l strimzi.io/kind=user-operator | grep -i "acl\|error" | tail -20

# KafkaUser 상태 확인
kubectl describe kafkauser payment-producer -n kafka

# 실제 ACL 적용 여부 CLI로 확인
kafka-acls.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --list --principal User:payment-producer
```

---

## 4. 모니터링 및 확인

```bash
# 전체 ACL 목록 조회
kafka-acls.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --list

# KafkaUser 전체 목록 및 상태
kubectl get kafkauser -n kafka

# User Operator 정상 동작 확인
kubectl get pods -n kafka -l strimzi.io/kind=user-operator
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_server_socketservermetrics_failed_authentication_total` | 인증/인가 실패 수 | > 0 |

---

## 5. TIP

- Strimzi `KafkaUser` CR 삭제 시 해당 ACL도 자동 삭제 — 공유 계정 사용 시 주의
- CLI로 추가한 ACL은 Strimzi가 관리하지 않음 → CR 기반 관리로 통일
- Kafka ACL Deny 규칙은 Allow보다 우선 적용 — 명시적 Deny 주의
- 참고: [Kafka Authorization](https://kafka.apache.org/documentation/#security_authz)
