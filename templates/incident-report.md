# 장애 보고서: {장애명}

> **발생 시각**: {YYYY-MM-DD HH:MM} KST
> **복구 시각**: {YYYY-MM-DD HH:MM} KST
> **영향 시간**: {N분}
> **심각도**: P1 / P2 / P3
> **영향 범위**: {Topic명, Consumer Group, 서비스명}

---

## 1. 요약 (Executive Summary)

{3줄 이내로 장애 원인과 조치 내용 요약}

---

## 2. 타임라인

| 시각 | 이벤트 |
|------|--------|
| HH:MM | 최초 알람 발생 (Consumer Lag > {N}) |
| HH:MM | 원인 파악 시작 |
| HH:MM | 원인 식별 |
| HH:MM | 조치 시작 |
| HH:MM | 서비스 복구 확인 |
| HH:MM | 모니터링 완료 |

---

## 3. 근본 원인 (Root Cause)

{왜 발생했는지 기술적 원인 설명}

### 관련 Kafka 설정

```properties
# 문제가 된 Producer/Consumer 설정
```

### 진단 명령어 실행 결과

```bash
kafka-consumer-groups.sh --bootstrap-server <BOOTSTRAP_SERVER> \
  --describe --group <GROUP_ID>
```

---

## 4. 조치 내용

### 즉각 조치 (임시 처리)

```bash
# 롤백 또는 임시 조치 명령어
```

### 영구 조치 (근본 해결)

```yaml
# 수정된 KafkaTopic/KafkaUser CR 또는 설정
```

---

## 5. 영향 분석

| 항목 | 내용 |
|------|------|
| 최대 Consumer Lag | {N} 메시지 |
| 미처리 메시지 수 | {N} |
| 영향받은 Topic | {Topic 목록} |
| 영향받은 서비스 | {서비스 목록} |
| 데이터 유실 여부 | 없음 / {N}건 |

---

## 6. 재발 방지 대책

| 항목 | 담당 | 기한 |
|------|------|------|
| {예: max.poll.interval.ms 튜닝} | {담당자} | {YYYY-MM-DD} |
| {예: Consumer Lag 알람 임계값 조정} | {담당자} | {YYYY-MM-DD} |

---

## 7. 참고 자료

- 관련 문서: `docs/{category}/{file}.md`
- 관련 Runbook: `docs/{category}/runbook-{name}.md`
