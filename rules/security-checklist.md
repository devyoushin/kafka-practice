# 보안 체크리스트 (Security Checklist)

Kafka 관련 문서 작성 및 설정 코드 작성 시 보안 검토 기준입니다.

---

## 1. 인증 (Authentication) 체크리스트

- [ ] SASL 메커니즘이 `SCRAM-SHA-512` 또는 `GSSAPI(Kerberos)` 인지 확인
- [ ] `PLAIN` 메커니즘 사용 시 반드시 **운영 환경 금지** 주석 추가
- [ ] KafkaUser CR의 `authentication.type`이 `scram-sha-512`인지 확인
- [ ] 클라이언트 `security.protocol`이 `SASL_SSL` 또는 `SSL`인지 확인

```yaml
# ⚠️ 주의: PLAINTEXT는 개발 환경 전용. 운영 환경에서 사용 금지
listeners:
  - name: plain
    port: 9092
    type: internal
    tls: false
```

## 2. 암호화 (Encryption) 체크리스트

- [ ] 클라이언트-브로커 통신 TLS 활성화 여부 확인
- [ ] 브로커 간 통신 (inter-broker) TLS 설정 확인
- [ ] Strimzi 환경에서 인증서 자동 갱신 설정 확인

## 3. 인가 (Authorization / ACL) 체크리스트

- [ ] 와일드카드(`*`) 토픽 ACL 사용 시 반드시 이유 주석 추가
- [ ] Producer는 Write + Describe 권한만 부여 (Read 불필요)
- [ ] Consumer는 Read + Describe 권한 + ConsumerGroup 권한만 부여
- [ ] `allow.everyone.if.no.acl.found=false` 설정 확인 (기본 차단)

```yaml
# ⚠️ 주의: 아래는 와일드카드 예시 — 운영 환경에서는 토픽을 명시할 것
acls:
  - resource:
      type: topic
      name: "*"   # 개발 환경 전용
```

## 4. 자격 증명 관리

- [ ] JAAS 설정에 평문 비밀번호 하드코딩 금지
- [ ] Kubernetes Secret으로 SASL 자격 증명 관리
- [ ] Strimzi KafkaUser CR 사용 시 Secret 자동 생성 확인

## 5. 문서 내 보안 표현 규칙

- 보안 취약 설정 예시 작성 시 반드시 **⚠️ 주의** 또는 **운영 환경 금지** 표시
- 예시:
  ```properties
  # ⚠️ 주의: 아래 설정은 개발 환경 전용. 운영 환경에서 사용 금지
  security.protocol=PLAINTEXT
  ```
