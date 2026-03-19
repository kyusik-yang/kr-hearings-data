# kr-hearings-data Expansion Roadmap

**Created**: 2026-03-19
**Current**: v7 (8.7M speeches, 14,749 meetings)

---

## 1. Current Coverage vs Universe

16-22대 국회 기준 (2000-2025), 국회회의록시스템에 23,726건 회의가 등록되어 있다. v7은 이 중 11,312건(47.7%)을 커버한다.

| 회의 유형 | 전체 | v7 커버 | 커버율 | 비고 |
|-----------|------|---------|--------|------|
| 상임위원회 | 15,110 | 7,402 | 49.0% | v5 원본. 16대 일부 + 소위 등 미수집 |
| 국정감사 | 4,452 | 3,674 | 82.5% | v5 원본 |
| 특별위원회 | 1,993 | 228 | 11.4% | v7: 인사청문특별위원회 270건만 |
| 국회본회의 | 1,114 | 4 | 0.4% | 거의 미수집 |
| 예산결산특별위원회 | 845 | 3 | 0.4% | 거의 미수집 |
| **국정조사** | **204** | **1** | **0.5%** | **거의 미수집** |
| 전원위원회 | 8 | 0 | 0% | 극소수 |
| **합계** | **23,726** | **11,312** | **47.7%** | |

모든 회의에 **PDF 다운로드 URL**이 있다 (VCONFDETAIL DOWN_URL). v7 gap-fill에서 검증된 PDF 파싱 파이프라인을 그대로 적용 가능.

---

## 2. Expansion Priorities

### Priority 1: 국정조사 (204 meetings)

**연구 가치**: 국정조사는 국회의 행정부 감시 기능의 최고 수준. 청문회 25건 포함. 최순실 국정농단, IMF 환란, 세월호 등 한국 정치사의 핵심 이벤트.

| Item | Detail |
|------|--------|
| 규모 | 204 meetings (16-22대), 청문회 25건 포함 |
| 수집 방법 | DOWN_URL → PDF → PyMuPDF 파싱 (v7 파이프라인 재활용) |
| 예상 effort | Low. PDF 204개 다운로드 + 파싱 자동화 |
| 새로운 committee_key | `investigation_special` |
| 새로운 hearing_type | `국정조사` |

주요 국정조사 목록:
- 박근혜정부 최순실 국정농단 (20대, 18건)
- IMF 환란 원인규명 (15대, 25건)
- 저축은행 비리 진상규명 (19대, 19건)
- 미국산 쇠고기 수입 (18대, 15건)
- 한보 사건 (15대, 27건)
- 한국조폐공사 파업유도 (16대, 16건)

### Priority 2: 상임위 공청회 (796 meetings)

**연구 가치**: 공청회는 전문가/이해관계자 의견을 수렴하는 형식. witness, expert_witness 역할의 발언이 풍부. 법안 심사 과정 연구에 필수.

| Item | Detail |
|------|--------|
| 규모 | 796 meetings (상임위 내 PBHRG_YN=Y) |
| 수집 | 이미 v7 상임위에 **포함되어 있을 가능성**. 확인 필요 |
| 확인 방법 | meeting_id 매칭으로 v7에 이미 있는지 점검 |
| 예상 추가 | 대부분 v7에 이미 있을 것 (상임위 회의록에 공청회가 포함) |

### Priority 3: 상임위 미수집분 (7,708 meetings)

**연구 가치**: 상임위 커버리지를 49%에서 100%로 올리면 패널 분석, 시계열 분석의 완전성 확보.

| Item | Detail |
|------|--------|
| 규모 | ~7,700 meetings (16-22대 상임위 중 v7에 없는 것) |
| 주요 gap | 16대 290건, 17대 1,067건, 21-22대 ~3,100건 |
| 수집 방법 | (a) v5 원본 XLSX 재점검, (b) PDF 파이프라인, (c) HTML 스크래핑 |
| 예상 effort | High. 양이 많고 원인 분석 필요 (v5에서 왜 빠졌는지) |

v5가 놓친 이유 추정:
- 16대 초기: XLSX 디지털화 미완
- 21-22대: v5 수집 시점(2024) 이후 추가된 회의
- 일부: 소위원회 별도 회의록

### Priority 4: 예산결산특별위원회 (845 meetings)

**연구 가치**: 예산 심의는 입법부 핵심 기능. 공청회 28건 포함. 장관/처장 출석 답변 다수.

| Item | Detail |
|------|--------|
| 규모 | 845 meetings |
| 수집 | PDF 파이프라인 또는 HTML 스크래핑 |
| 새로운 hearing_type | `예산결산특별위원회` |

### Priority 5: 특별위원회 비인사청문 (1,765 meetings)

**연구 가치**: 정치개혁, 사법제도개혁, 헌법개정 등 주요 정치 이슈. 공청회 154건, 청문회 22건.

| Item | Detail |
|------|--------|
| 규모 | 1,993 - 228(인사청문) = 1,765 meetings |
| 주요 위원회 | 정치개혁(310), 윤리(142), 사법제도개혁(78), 헌법개정(49) |
| 수집 | PDF 파이프라인 |
| 새로운 committee_key | 위원회별 개별 키 또는 `special_other` |

### Priority 6: 국회본회의 (1,114 meetings)

**연구 가치**: 법안 표결, 대정부질문, 시정연설 등. 개별 발언보다는 의사진행 중심. 텍스트 분석 가치는 상대적으로 낮으나 완전성 확보 차원.

| Item | Detail |
|------|--------|
| 규모 | 1,114 meetings |
| 특성 | 발언이 짧고 절차적. 대정부질문은 예외적으로 길고 분석 가치 높음 |
| 수집 | PDF 파이프라인 |

---

## 3. Collection Methods

### Method A: PDF Pipeline (v7 검증 완료)

v7 gap-fill에서 검증된 방법. 모든 회의에 적용 가능.

```
VCONFDETAIL DOWN_URL → download_gap_pdfs.py → PyMuPDF → speaker parsing → refine
```

장점: 모든 회의에 DOWN_URL 존재 (100%). 자동화 파이프라인 구축 완료.
단점: HTML 대비 낮은 파싱 정확도 (특히 역할 분류). 의원 메타데이터 별도 매칭 필요.

### Method B: HTML Scraping (v5/v6 방법)

국회회의록시스템 HTML 페이지를 Playwright로 스크래핑.

```
likms.assembly.go.kr → Playwright → structured HTML parsing → role classification
```

장점: 가장 높은 정확도 (구조화된 HTML). member_id 직접 추출 가능.
단점: Playwright 세팅 필요. 안티스크래핑 대응. 느린 속도.

### Method C: Chrome Extension + record.assembly.go.kr

record.assembly.go.kr의 새로운 인터페이스를 Chrome으로 직접 접근.

```
record.assembly.go.kr → Chrome (Claude-in-Chrome) → 회의별 페이지 → 발언 텍스트 추출
```

장점: 최신 인터페이스, 발언자별 구분된 HTML. JS 렌더링 자동 처리.
단점: 속도 제한. 대량 수집에 부적합.

### Recommended Strategy

| Priority | Method | Reason |
|----------|--------|--------|
| P1 국정조사 (204) | PDF (A) | 소규모, 파이프라인 검증됨 |
| P2 상임위 공청회 (796) | 확인만 | v7에 이미 포함 가능성 |
| P3 상임위 미수집 (7,700) | HTML (B) 우선 → PDF (A) 보완 | 정확도 중요, 대규모 |
| P4 예결위 (845) | PDF (A) | 중규모, 구조 유사 |
| P5 특별위 비인사청문 (1,765) | PDF (A) | 중규모 |
| P6 본회의 (1,114) | PDF (A) | 구조 상이하나 파이프라인 적용 가능 |

---

## 4. Technical Notes

### 4.1 PDF 파싱 주의사항

- `○` (U+25CB) vs `◯` (U+25EF): 시대에 따라 다른 발언자 마커 사용
- 16-17대 회의록은 한자 이름 사용 → 한자 변환 사전 필요 (1,224 entries 구축 완료)
- 본회의 PDF는 상임위와 구조 다름 (의사일정, 투표 결과 등 포함)
- 국정조사 청문회 PDF는 증인 선서, 질의-응답 순서가 인사청문회와 유사

### 4.2 Schema 확장

새 회의 유형 추가 시 변경:
- `hearing_type`: 새 값 추가 (`국정조사`, `예산결산특별위원회`, `본회의`)
- `committee_key`: 새 키 추가 (`investigation_special`, `budget_special`, `plenary`)
- 기존 컬럼 스키마는 변경 불필요

### 4.3 VCONFDETAIL 활용

모든 회의의 메타데이터가 이미 `vconfdetail_all.csv`에 있음:
- CONF_ID → meeting_id
- DOWN_URL → PDF 다운로드
- BG_PTM / ED_PTM → 회의 시간
- HR_HRG_YN / PBHRG_YN / HRG_YN / SITG_YN → 회의 유형 플래그

---

## 5. Estimated Scale After Full Expansion

| Phase | hearing_type | Meetings | Est. speeches | Cum. speeches |
|-------|-------------|----------|--------------|---------------|
| v7 (current) | 상임위+국감+인사청문특별위 | 14,749 | 8,740,779 | 8.7M |
| +P1 국정조사 | +국정조사 | +204 | +~100K | ~8.8M |
| +P3 상임위 gap | +상임위(보완) | +7,700 | +~4M | ~12.8M |
| +P4 예결위 | +예산결산특별위 | +845 | +~500K | ~13.3M |
| +P5 특별위 | +특별위(비인사청문) | +1,765 | +~800K | ~14.1M |
| +P6 본회의 | +본회의 | +1,114 | +~300K | ~14.4M |
| **Full** | **all 7 types** | **~26,000** | **~14.4M** | **~14.4M** |

전체 확장 시 현재 대비 **+65% speeches**, **+76% meetings**.
