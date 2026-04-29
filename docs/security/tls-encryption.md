# TLS 암호화

TLS(Transport Layer Security) 암호화는 Kafka Client와 Broker 간 통신 데이터를 암호화합니다. Strimzi는 클러스터 CA와 클라이언트 CA를 자동 관리하며, 인증서 갱신도 자동으로 처리합니다.

---

## 1. 개요

Kafka TLS 구성 요소:
- **Cluster CA**: Broker 인증서 서명 (Strimzi 자동 관리)
- **Client CA**: mTLS 사용 시 Client 인증서 서명 (Strimzi 자동 관리)
- **Listener TLS**: 리스너별 TLS 활성화 여부
- **mTLS (Mutual TLS)**: Broker ↔ Client 상호 인증서 검증

---

## 2. 설명

### 2.1 핵심 개념

#### TLS 구성 방식 비교

| 방식 | 설명 | 사용 사례 |
|------|------|---------|
| TLS (서버 인증만) | Client가 Broker 인증서만 검증 | SASL_SSL + username/password |
| mTLS (상호 인증) | Client↔Broker 양방향 인증서 검증 | `tls` 인증 타입 사용 시 |

#### Strimzi 인증서 자동 관리

```
Strimzi Cluster CA (자동 생성)
    ├── 유효 기간: 기본 365일
    ├── 갱신: 30일 전 자동 갱신 (Rolling Restart 발생)
    └── Secret: my-cluster-cluster-ca-cert (namespace: kafka)

Strimzi Client CA (mTLS 사용 시)
    ├── 유효 기간: 기본 365일
    └── Secret: my-cluster-clients-ca-cert (namespace: kafka)
```

#### 인증서 갱신 시 영향

| 갱신 유형 | 영향 | 다운타임 |
|---------|------|---------|
| Cluster CA 갱신 | Broker Rolling Restart | 없음 (파티션 단위 순차) |
| Client CA 갱신 | Client 재연결 필요 | 없음 |
| 수동 강제 갱신 | annotation으로 트리거 | 없음 |

### 2.2 실무 적용 코드

#### Strimzi TLS 리스너 설정

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
  namespace: kafka
spec:
  kafka:
    listeners:
      # 내부 TLS (SASL_SSL)
      - name: tls
        port: 9093
        type: internal
        tls: true
        authentication:
          type: scram-sha-512

      # 외부 TLS (LoadBalancer, SASL_SSL)
      - name: external
        port: 9094
        type: loadbalancer
        tls: true
        authentication:
          type: scram-sha-512
        configuration:
          brokerCertChainAndKey:         # 커스텀 인증서 사용 시
            secretName: my-tls-secret
            certificate: tls.crt
            key: tls.key
```

#### Cluster CA 인증서 추출 및 Truststore 생성

```bash
# Cluster CA 인증서 추출
kubectl get secret my-cluster-cluster-ca-cert \
  -n kafka \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > cluster-ca.crt

# JKS Truststore 생성
keytool -import \
  -alias strimzi-cluster-ca \
  -file cluster-ca.crt \
  -keystore client-truststore.jks \
  -storepass truststorepassword \
  -noprompt

# PEM 형식 확인
openssl x509 -in cluster-ca.crt -text -noout | grep -E "Subject|Issuer|Not"
```

#### mTLS KafkaUser 설정

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: my-mtls-client
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster
spec:
  authentication:
    type: tls          # mTLS — 인증서 기반 인증
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: my-topic
          patternType: literal
        operations:
          - Read
          - Write
          - Describe
        host: "*"
```

#### mTLS Client 설정

```bash
# mTLS용 Keystore 생성 (KafkaUser Secret에서 추출)
kubectl get secret my-mtls-client -n kafka \
  -o jsonpath='{.data.user\.p12}' | base64 -d > client-keystore.p12

kubectl get secret my-mtls-client -n kafka \
  -o jsonpath='{.data.user\.password}' | base64 -d > client-keystore-password
```

```properties
# mTLS Client 설정
bootstrap.servers=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9093
security.protocol=SSL

# Truststore (Broker 인증서 검증)
ssl.truststore.location=/opt/kafka/certs/client-truststore.jks
ssl.truststore.password=truststorepassword
ssl.truststore.type=JKS

# Keystore (Client 인증서 — mTLS)
ssl.keystore.location=/opt/kafka/certs/client-keystore.p12
ssl.keystore.password=<CLIENT_KEYSTORE_PASSWORD>
ssl.keystore.type=PKCS12
```

#### 인증서 유효 기간 연장 (Strimzi)

```yaml
# Kafka CR에서 CA 유효 기간 설정
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
  namespace: kafka
spec:
  clusterCa:
    generateCertificateAuthority: true
    validityDays: 3650         # 10년 (기본 365일)
    renewalDays: 30            # 만료 30일 전 자동 갱신
  clientsCa:
    generateCertificateAuthority: true
    validityDays: 3650
    renewalDays: 30
```

#### 인증서 수동 강제 갱신

```bash
# Cluster CA 강제 갱신
kubectl annotate secret my-cluster-cluster-ca-cert \
  -n kafka \
  strimzi.io/force-replace=true

# 갱신 상태 확인
kubectl get kafka my-cluster -n kafka -o jsonpath='{.status.conditions}'
```

### 2.3 Best Practice

- 운영 환경에서 `SASL_PLAINTEXT` 금지 — 반드시 `SASL_SSL` 또는 `SSL` 사용
- Strimzi 자동 CA 갱신 시 Rolling Restart 발생 → PodDisruptionBudget 확인
- 커스텀 인증서 사용 시 갱신 주기 직접 관리 필요 (Strimzi 자동 갱신 미적용)
- Client Truststore는 ConfigMap이 아닌 Secret으로 관리

---

## 3. 트러블슈팅

### 3.1 SSL Handshake 실패

#### 증상
```
javax.net.ssl.SSLHandshakeException:
PKIX path building failed: unable to find valid certification path to requested target
```

#### 원인
- Client Truststore에 Cluster CA 인증서 미포함
- 인증서 만료

#### 해결 방법
```bash
# 인증서 만료 확인
kubectl get secret my-cluster-cluster-ca-cert -n kafka \
  -o jsonpath='{.data.ca\.crt}' | base64 -d | \
  openssl x509 -noout -enddate

# Truststore에 CA 인증서 재추가
keytool -import -alias strimzi-cluster-ca \
  -file cluster-ca.crt \
  -keystore client-truststore.jks \
  -storepass truststorepassword -noprompt
```

### 3.2 인증서 갱신 후 Client 연결 끊김

#### 증상
- CA 갱신 후 일부 Client가 연결 오류 발생
- Truststore 업데이트 전 새 CA로 서명된 인증서를 받은 경우

#### 원인
- Client Truststore에 이전 CA만 포함 (신규 CA 미포함)

#### 해결 방법
```bash
# 신규 CA 인증서를 기존 Truststore에 추가 (교체 아닌 추가)
kubectl get secret my-cluster-cluster-ca-cert -n kafka \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > new-ca.crt

keytool -import -alias strimzi-cluster-ca-new \
  -file new-ca.crt \
  -keystore client-truststore.jks \
  -storepass truststorepassword -noprompt

# Client 재배포 (새 Truststore 적용)
kubectl rollout restart deployment my-service-consumer -n kafka-apps
```

### 3.3 mTLS 인증 실패

#### 증상
```
SSL mutual authentication failed
```

#### 원인
- KafkaUser Secret의 클라이언트 인증서가 만료됨
- Keystore 비밀번호 불일치

#### 해결 방법
```bash
# Client 인증서 만료 확인
kubectl get secret my-mtls-client -n kafka \
  -o jsonpath='{.data.user\.crt}' | base64 -d | \
  openssl x509 -noout -enddate

# KafkaUser CR 재생성으로 인증서 갱신
kubectl delete kafkauser my-mtls-client -n kafka
kubectl apply -f kafkauser-mtls.yaml
```

---

## 4. 모니터링 및 확인

```bash
# Cluster CA 인증서 만료일 확인
kubectl get secret my-cluster-cluster-ca-cert -n kafka \
  -o jsonpath='{.data.ca\.crt}' | base64 -d | \
  openssl x509 -noout -enddate

# Broker TLS 인증서 확인
kubectl get secret my-cluster-kafka-brokers -n kafka \
  -o jsonpath='{.data.my-cluster-kafka-0\.crt}' | base64 -d | \
  openssl x509 -noout -subject -enddate
```

| Prometheus 지표 | 설명 | 알람 기준 |
|----------------|------|---------|
| `kafka_server_socketservermetrics_failed_authentication_total` | TLS/인증 실패 수 | > 0 |
| `ssl_certificate_expiry_seconds` | 인증서 만료까지 남은 시간 | < 30일 |

---

## 5. TIP

- Strimzi는 CA 갱신 30일 전 자동 갱신 시작 → `renewalDays` 값 조정으로 여유 확보
- 외부 CA(AWS ACM, Let's Encrypt 등) 사용 시 `brokerCertChainAndKey` 설정으로 커스텀 인증서 주입
- TLS 1.2 강제: `ssl.enabled.protocols=TLSv1.2,TLSv1.3` (기본값은 TLSv1.2, TLSv1.3)
- 참고: [Kafka TLS Encryption](https://kafka.apache.org/documentation/#security_ssl)
