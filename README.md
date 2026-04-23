# 🏃 Korean Sports Survey ML

> 국민체육실태조사(2016–2025) 데이터를 기반으로 한 머신러닝 예측 모델 프로젝트

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![GitHub Pages](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://hanbi-kim.github.io/korean-sports-survey-ml/)

---

## 📌 프로젝트 개요

문화체육관광부에서 매년 실시하는 **국민체육실태조사** 데이터(2016~2025년, 10개년)를 수집·통합하고,  
머신러닝 모델을 통해 스포츠 참여 행태를 예측하는 end-to-end 파이프라인을 구축합니다.

### 주요 목표
- 연도별로 상이한 설문 코드·컬럼 구조를 통합 마스터 데이터셋으로 정제
- EDA를 통해 10년간 국민 체육활동 트렌드 분석
- 머신러닝 모델로 스포츠 참여 여부 / 빈도 / 종목 예측
- GitHub Pages를 통한 분석 결과 시각화 및 공유

---

## 📁 프로젝트 구조

```
korean-sports-survey-ml/
│
├── data/
│   ├── raw/            # 연도별 원본 데이터 (2016~2025)
│   ├── processed/      # 전처리된 통합 데이터
│   └── codebook/       # 연도별 변수 매핑 테이블
│
├── notebooks/
│   ├── 01_eda.ipynb           # 탐색적 데이터 분석
│   ├── 02_preprocessing.ipynb # 전처리 과정
│   └── 03_modeling.ipynb      # 모델링 & 평가
│
├── src/
│   ├── preprocess.py   # 데이터 정제 파이프라인
│   ├── train.py        # 모델 학습
│   └── evaluate.py     # 평가 & SHAP 분석
│
├── models/             # 저장된 모델 (*.pkl, *.joblib)
├── reports/            # 자동 생성 HTML 리포트
├── docs/               # GitHub Pages 소스
└── .github/workflows/  # CI/CD 자동화
```

---

## 🚀 시작하기

### 환경 설정
```bash
git clone https://github.com/Hanbi-Kim/korean-sports-survey-ml.git
cd korean-sports-survey-ml
pip install -r requirements.txt
```

### 데이터 전처리 실행
```bash
python src/preprocess.py
```

### 모델 학습
```bash
python src/train.py
```

---

## 📊 데이터 출처

- **국민체육실태조사** (문화체육관광부 / 한국스포츠정책과학원)
- 조사 기간: 2016년 ~ 2025년 (연간)
- 대상: 전국 만 10세 이상 국민

---

## 📈 진행 현황

| 단계 | 상태 | 설명 |
|------|------|------|
| 데이터 수집 | 🔄 진행 중 | 2016~2025 원본 파일 수집 |
| 데이터 전처리 | ⏳ 대기 | 연도별 코드 통합 |
| EDA | ⏳ 대기 | 트렌드 분석 |
| 모델링 | ⏳ 대기 | 예측 모델 개발 |
| GitHub Pages | ⏳ 대기 | 결과 시각화 |

---

## 👤 Author

**Hanbi Kim** · [@Hanbi-Kim](https://github.com/Hanbi-Kim)
