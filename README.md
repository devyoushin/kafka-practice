# kafka-practice

EKS + Strimzi Operator 기준으로 Apache Kafka 3.7.x 운영 지식을 정리한 개인 학습 문서입니다.

## 빠른 시작

- 처음 볼 문서: `docs/guides/core-concepts/kafka-architecture.md`
- 전체 흐름: 핵심 개념 -> Producer/Consumer -> 운영 -> 보안 -> 관측
- AI 작업 지침: `CLAUDE.md`

## 구조

```text
kafka-practice/
├── README.md
├── CLAUDE.md
├── docs/
│   ├── guides/     # 학습 문서
│   ├── rules/      # 작성/운영 규칙
│   ├── templates/  # 재사용 템플릿
│   └── agents/     # Claude 에이전트 프롬프트
└── ops/
    └── config/     # Kafka/Strimzi 매니페스트
```

## 학습 경로

| 단계 | 위치 |
|------|------|
| 핵심 개념 | `docs/guides/core-concepts/` |
| Producer/Consumer | `docs/guides/producer-consumer/` |
| 운영 | `docs/guides/operations/` |
| 보안 | `docs/guides/security/` |
| 관측 | `docs/guides/observability/` |

## 환경

| 항목 | 값 |
|------|-----|
| Platform | EKS |
| Kafka | 3.7.x |
| Operator | Strimzi |
| Cluster namespace | `kafka` |
| App namespace | `kafka-apps` |
