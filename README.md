# kafka-practice

EKS와 Strimzi Operator 기준으로 Apache Kafka를 운영하기 위한 개인 학습 공간입니다.

## 어디서 시작할까

- 문서 지도: `docs/README.md`
- 첫 문서: `docs/core-concepts/kafka-architecture.md`
- 운영 보조 자료: `ops/README.md`
- AI 작업 지침: `CLAUDE.md`

## 구조

| 경로 | 내용 |
|------|------|
| `docs/` | Kafka 개념, Producer/Consumer, 운영, 보안, 관측 문서 |
| `docs/rules/` | 문서 작성 및 운영 규칙 |
| `docs/templates/` | 재사용 문서 템플릿 |
| `docs/agents/` | Claude 에이전트 프롬프트 |
| `ops/` | Strimzi/Kafka 매니페스트 예시 |

## 환경

| 항목 | 값 |
|------|-----|
| Platform | EKS |
| Kafka | 3.7.x |
| Operator | Strimzi |
