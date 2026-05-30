# SASL 인증

SASL(Simple Authentication and Security Layer) 인증은 Kafka Client(클라이언트)와 Broker(브로커) 간 신원을 검증하는 메커니즘입니다. Strimzi 환경에서는 `SCRAM-SHA-512`를 권장하며, KafkaUser CR로 사용자를 선언적으로 관리합니다.

---

## 1. 개요

Kafka가 지원하는 SASL 메커니즘:

| 메커니즘 | 설명 | 권장 여부 |
|---------|------|---------|
| `PLAIN` | 평문 username/password | 비권장 (TLS와 함께만 사용) |
| `SCRAM-SHA-256` | Challenge-Response 방식 | 보통 |
| `SCRAM-SHA-512` | SHA-512 기반 SCRAM | **권장** |
| `GSSAPI` (Kerberos) | Kerberos 기반 인증 | 엔터프라이즈 환경 |
| `OAUTHBEARER` | OAuth 2.0 토큰 기반 | 클라우드 환경 |

---

## 2. 설명

### 2.1 핵심 개념

#### SASL_SSL vs SASL_PLAINTEXT

| 프로토콜 | 설명 | 사용 환경 |
|---------|------|---------|
| `SASL_SSL` | SASL 인증 + TLS 암호화 | **운영 환경 필수** |
| `SASL_PLAINTEXT` | SASL 인증 + 평문 전송 | 클러스터 내부 테스트 환경만 |

#### Strimzi SCRAM-SHA-512 인증 흐름

```
1. KafkaUser CR 생성 → Strimzi User Operator가 Secret 자동 생성
2. Client가 Secret에서 username/password 읽음
3. SCRAM Challenge-Response로 Broker 인증
4. 인증 성공 후 ACL로 권한 검사
```

#### KafkaUser CR 구조

```
KafkaUser
├── metadata.name          → username (= Secret 이름)
├── spec.authentication    → 인증 방식 (scram-sha-512)
└── spec.authorization     → ACL 권한 목록
    └── acls[]
        ├── resource       → topic / group / cluster / transactionalId
        ├── operations[]   → Read / Write / Create / Delete / Describe 등
        └── host           → 허용 클라이언트 IP (* = 모든 IP)
```

### 2.2 실무 적용 코드

#### Strimzi Kafka 리스너 설정 (SASL_SSL)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
  namespace: kafka
spec:
  kafka:
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false           # 내부 테스트용 (운영 비권장)
      - name: tls
        port: 9093
        type: internal
        tls: true
        authentication:
          type: scram-sha-512   # SASL_SSL
      - name: external
        port: 9094
        type: loadbalancer
        tls: true
        authentication:
          type: scram-sha-512
```

#### KafkaUser CR — Producer

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

#### KafkaUser CR — Consumer

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: my-consumer
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
          - Read
          - Describe
        host: "*"
      - resource:
          type: group
          name: my-service-consumer-group
          patternType: literal
        operations:
          - Read
        host: "*"
```

#### Secret에서 인증 정보 조회

```bash
# KafkaUser Secret 확인
kubectl get secret my-producer -n kafka -o yaml

# username/password 디코딩
kubectl get secret my-producer -n kafka \
  -o jsonpath='{.data.password}' | base64 -d

# SASL JAAS config 형식으로 조합
echo "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username=\"my-producer\" \
  password=\"$(kubectl get secret my-producer -n kafka -o jsonpath='{.data.password}' | base64 -d)\";"
```

#### Client 설정 (운영 환경)

```properties
bootstrap.servers=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9093
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="my-producer" \
  password="<PASSWORD>";

# TLS — Strimzi 자동 생성 CA 신뢰
ssl.truststore.location=/opt/kafka/certs/truststore.jks
ssl.truststore.password=<TRUSTSTORE_PASSWORD>
```

#### Kubernetes Secret을 Java Keystore로 변환 (TLS 신뢰 체인)

```bash
# Strimzi CA 인증서 추출
kubectl get secret my-cluster-cluster-ca-cert -n kafka \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt

# Truststore 생성
keytool -import -alias strimzi-ca \
  -file ca.crt \
  -keystore truststore.jks \
  -storepass changeit \
  -noprompt
```

### 2.3 Best Practice

- Strimzi 환경에서 username/password를 직접 관리하지 말고 **KafkaUser CR**로 선언적 관리
- Client Secret은 Kubernetes Secret으로 마운트 → 환경 변수 또는 파일 형태로 주입
- `SASL_PLAINTEXT`는 클러스터 외부 노출 금지 — 내부 테스트에만 제한 사용
- KafkaUser 삭제 시 해당 Secret도 함께 삭제됨 — 사전 확인 필요

---

## 3. 트러블슈팅

### 3.1 인증 실패 (Authentication Failed)

#### 증상
```
org.apache.kafka.common.errors.SaslAuthenticationException:
Authentication failed: Invalid username or password
```

#### 원인
- username/password 불일치
- KafkaUser Secret이 아직 생성되지 않음

#### 해결 방법
```bash
# KafkaUser 상태 확인
kubectl get kafkauser my-producer -n kafka
# Ready 상태여야 함

# Secret 생성 여부 확인
kubectl get secret my-producer -n kafka

# User Operator 로그 확인
kubectl logs -n kafka -l strimzi.io/kind=user-operator | tail -20
```

### 3.2 SASL 메커니즘 불일치

#### 증상
```
org.apache.kafka.common.errors.UnsupportedSaslMechanismException:
Client SASL mechanism 'SCRAM-SHA-256' not enabled in the server
```

#### 원인
- Client `sasl.mechanism`과 Broker 리스너 설정 불일치

#### 해결 방법
```bash
# Broker 리스너 설정 확인
kubectl get kafka my-cluster -n kafka -o jsonpath='{.spec.kafka.listeners}'

# Client 설정 수정
# sasl.mechanism=SCRAM-SHA-512  (브로커 설정과 일치)
```

### 3.3 TLS 인증서 검증 실패

#### 증상
```
javax.net.ssl.SSLHandshakeException:
PKIX path building failed: unable to find valid certification path
```

#### 원인
- Client Truststore에 Strimzi CA 인증서 미포함

#### 해결 방법
```bash
# Strimzi CA 인증서를 Truststore에 추가
kubectl get secret my-cluster-cluster-ca-cert -n kafka \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt

keytool -import -alias strimzi-ca -file ca.crt \
  -keystore truststore.jks -storepass changeit -noprompt
```

---

## 4. 모니터링 및 확인

```bash
# 인증 성공/실패 통계 (브로커 로그)
kubectl logs my-cluster-kafka-0 -n kafka | grep -i "authentication" | tail -20

# 활성 연결 확인
kafka-broker-api-versions.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9093 \
  --command-config client.properties
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_server_socketservermetrics_connection_creation_rate` | 연결 생성 속도 | 급격한 감소 |
| `kafka_server_socketservermetrics_failed_authentication_total` | 인증 실패 총 수 | > 0 |

---

## 5. TIP

- KafkaUser 비밀번호 갱신: KafkaUser CR 삭제 후 재생성 → Strimzi가 새 비밀번호로 Secret 자동 재생성
- `sasl.jaas.config`에 비밀번호 하드코딩 금지 → Kubernetes Secret 환경 변수로 주입
- OAUTHBEARER 인증은 Strimzi 0.34+ 지원
- 참고: [Kafka Security Authentication](https://kafka.apache.org/documentation/#security_sasl)
