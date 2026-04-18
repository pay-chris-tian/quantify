# Quantify Roadmap

## 목적

이 문서는 Quantify 프로젝트의 개발 순서를 정리한 실행용 로드맵입니다.

---

## Phase 0. 문제 정의

해야 할 일:
- 프로젝트 목표 1문장으로 정리
- 투자 유니버스 정의
- 초기 전략 가설 정의
- 성과 평가 지표 정의

산출물:
- README
- docs/strategy.md

---

## Phase 1. 데이터 레이어

해야 할 일:
- 가격 데이터 수집 방식 정의
- 재무 데이터 수집 방식 정의
- 종목 메타데이터 구조 정의
- 저장 포맷(CSV/Parquet/DB) 결정
- 데이터 정제 규칙 정의

산출물 예시:
- data/raw/
- data/processed/
- quantify/data_loader.py

주의:
- 데이터 누수 방지
- 수정주가 반영 여부
- 상장폐지 및 종목 변경 이슈 고려

---

## Phase 2. 팩터 엔진

해야 할 일:
- Value 팩터 계산
- Momentum 팩터 계산
- Quality 팩터 계산
- 필터 로직 구현

산출물 예시:
- quantify/factors.py
- quantify/filters.py

---

## Phase 3. 랭킹 및 포트폴리오 엔진

해야 할 일:
- 점수화 방식 구현
- 종목 순위 계산
- 상위 종목 선정
- 동일비중 포트폴리오 구성

산출물 예시:
- quantify/ranking.py
- quantify/portfolio.py

---

## Phase 4. 백테스트 엔진

해야 할 일:
- 리밸런싱 로직 구현
- 거래비용 반영
- 자산곡선 계산
- 매매 내역 기록
- 성과 지표 계산

산출물 예시:
- quantify/backtester.py
- quantify/metrics.py

---

## Phase 5. 리포트 및 시각화

해야 할 일:
- 전략 결과 요약
- 벤치마크 비교
- 월별 편입/편출 종목 정리
- 차트 생성
- 대시보드 검토

산출물 예시:
- quantify/report.py
- app/
- notebooks/

---

## Phase 6. 검증

해야 할 일:
- 기간 분할 검증
- 파라미터 민감도 테스트
- 유니버스 변화 테스트
- 비용 변화 테스트

산출물:
- 백테스트 비교표
- 전략 변경 로그

---

## Phase 7. 페이퍼 트레이딩

해야 할 일:
- 실시간 시그널 생성
- 주문 없는 운영
- 체결 가능성 점검
- 운영 로그 기록

---

## Phase 8. 자동화 검토

해야 할 일:
- 증권사 API 검토
- 주문 실패 대응
- 알림 시스템 구축
- 모니터링 구축

주의:
- 자동매매는 가장 마지막 단계
- 전략 검증 이전에 자동화하지 않음
