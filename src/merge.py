"""
국민생활체육조사 전체 연도 데이터 통합 (2016~2025)
===================================================
프로젝트 구조:
    korean-sports-survey-ml/
    ├── data/
    │   ├── 5_processed/       ← survey_{year}_standardized.csv
    │   └── sports_survey.csv  ← 최종 통합본 (출력)
    └── src/
        └── merge.py           ← 이 파일

사용법 (프로젝트 루트에서 실행):
    python src/merge.py
"""

import sys
from pathlib import Path

import pandas as pd

BASE_DIR    = Path(__file__).resolve().parent.parent
PROC_DIR    = BASE_DIR / "data" / "5_processed"
OUTPUT_FILE = BASE_DIR / "data" / "sports_survey.csv"


def main():
    # 1. 표준화 파일 탐색
    csv_files = sorted(PROC_DIR.glob("survey_*_standardized.csv"))
    if not csv_files:
        print(f"❌ 표준화 파일 없음: {PROC_DIR}")
        sys.exit(1)

    print(f"{'='*50}")
    print(f"발견된 파일 ({len(csv_files)}개)")
    print(f"{'='*50}")

    # 2. 로드
    dfs = []
    for f in csv_files:
        df = pd.read_csv(f, encoding="utf-8-sig", low_memory=False)
        year = int(df["연도"].iloc[0]) if "연도" in df.columns else "?"
        print(f"  {f.name}: {df.shape[0]:,}행 × {df.shape[1]}열  (연도={year})")
        dfs.append(df)

    # 3. 통합
    df_merged = pd.concat(dfs, ignore_index=True, sort=False)
    cols_order = ["연도"] + [c for c in df_merged.columns if c != "연도"]
    df_merged  = df_merged[cols_order]

    print(f"\n{'='*50}")
    print(f"통합 결과: {df_merged.shape[0]:,}행 × {df_merged.shape[1]}열")
    print(f"{'='*50}")
    print("연도별 행수:")
    print(df_merged["연도"].value_counts().sort_index().to_string())

    # 4. 저장
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_merged.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n✅ 저장 완료: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()