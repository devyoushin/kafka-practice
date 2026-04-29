# Runbook: {작업명}

> **분류**: {Topic 변경 | Consumer 오프셋 리셋 | 보안 정책 변경 | 클러스터 업그레이드}
> **대상 서비스**: {서비스명}
> **작성일**: {YYYY-MM-DD}
> **예상 소요 시간**: {N분}
> **영향 범위**: {무중단 | Consumer 일시 중지 필요 | 메시지 재처리 발생}

---

## 사전 체크리스트

- [ ] 변경 대상 Topic/Consumer Group 목록 확인 완료
- [ ] 관련 팀 슬랙 채널 공지
- [ ] 현재 Lag 스냅샷 저장 완료
- [ ] 롤백 계획 수립 완료

---

## 환경 변수 설정

```bash
export BOOTSTRAP_SERVER="my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
export NAMESPACE="kafka"
export TOPIC_NAME="<TOPIC_NAME>"
export GROUP_ID="<GROUP_ID>"
export KAFKA_POD=$(kubectl get pod -n ${NAMESPACE} \
  -l strimzi.io/name=my-cluster-kafka \
  -o jsonpath='{.items[0].metadata.name}')
```

---

## Step 1: 사전 상태 확인

```bash
# Consumer Lag 현재 상태 저장
kafka-consumer-groups.sh --bootstrap-server ${BOOTSTRAP_SERVER} \
  --describe --group ${GROUP_ID} \
  > lag-snapshot-$(date +%Y%m%d%H%M%S).txt

# Topic 상태 확인
kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} \
  --describe --topic ${TOPIC_NAME}

# Under-Replicated Partition 없는지 확인
kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} \
  --describe --under-replicated-partitions
```

**예상 출력:**
```
# Under-Replicated Partition: 출력 없음 (정상)
# Consumer Group: STABLE 상태
```

---

## Step 2: {작업 내용}

```bash
# 작업 명령어
```

**확인 포인트:**
- 작업 완료 후 Topic/Consumer Group 상태 정상인지 확인

---

## Step 3: 완료 확인

```bash
# 변경 후 상태 확인
kafka-consumer-groups.sh --bootstrap-server ${BOOTSTRAP_SERVER} \
  --describe --group ${GROUP_ID}

kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} \
  --describe --topic ${TOPIC_NAME}
```

**성공 기준:**
- [ ] Consumer Group 상태: `STABLE`
- [ ] Under-Replicated Partition: 0
- [ ] Consumer Lag 정상 범위 유지

---

## 롤백 절차

> 아래 상황에서 롤백 수행: {Lag 급증 / Consumer 오류 지속 / 메시지 미처리}

```bash
# 롤백 명령어
```

---

## 모니터링 포인트

작업 완료 후 **30분간** 아래 지표 모니터링:

| 지표 | 정상 범위 | 이상 기준 |
|------|----------|---------|
| Consumer Lag | < {N} | > {M} |
| Under-Replicated Partition | 0 | > 0 |
| Consumer Group 상태 | STABLE | REBALANCING 지속 |

---

## 완료 보고 템플릿

```
[작업 완료 보고]
- 작업명: {작업명}
- 수행 시간: {시작} ~ {종료}
- 영향: {실제 영향}
- 결과: 정상 완료 / 롤백 / 이슈 발생
- 특이사항: {없음 | 내용}
```
