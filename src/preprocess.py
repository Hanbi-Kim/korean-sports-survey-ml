"""
국민생활체육조사 2016~2025 통합 전처리 스크립트

흐름:
  1. data/codebook/variable_mapping_table_final.xlsx 로드
     → 연도별 {원본컬럼명: 표준컬럼명(2025기준)} 매핑 딕셔너리 생성
  2. data/raw/*.xlsx 파일을 연도별로 읽기
     → 매핑된 컬럼만 표준명으로 rename해서 추출
     → 매핑 안 된 컬럼은 제외 (연도별 고유 컬럼은 data/processed/raw_by_year/ 에 별도 저장)
  3. 전 연도 concat → data/processed/master.csv (통합 분석용)

실행: python src/preprocess.py
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ── 경로 ──────────────────────────────────────────────────────────────────────
RAW_DIR      = Path("data/raw")
CODEBOOK_DIR = Path("data/codebook")
OUT_DIR      = Path("data/processed")
RAW_BY_YEAR  = OUT_DIR / "raw_by_year"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_BY_YEAR.mkdir(parents=True, exist_ok=True)

MAPPING_FILE = CODEBOOK_DIR / "variable_mapping_table_final.xlsx"

# ── 연도별 헤더 행 위치 ────────────────────────────────────────────────────────
HEADER_ROW = {
    2016: 0, 2017: 0,
    2018: 1, 2019: 1, 2020: 1, 2021: 1, 2022: 1,
    2023: 2,
    2024: 1,
    2025: 4,
}

# ── 매핑 테이블 파싱 ───────────────────────────────────────────────────────────
def load_mapping(filepath: Path) -> dict:
    """
    Returns:
        year_to_std  : {year: {원본컬럼명: 표준컬럼명}}
        std_col_order: 표준 컬럼 순서 (2025 기준)
    """
    raw = pd.read_excel(filepath, sheet_name="변수매핑테이블", header=None)
    years_order = [2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016]
    data = raw.iloc[2:].reset_index(drop=True)

    # 표준 컬럼 순서 (2025 컬럼 목록)
    std_col_order = []
    for _, row in data.iterrows():
        std = str(row.iloc[0]).strip()
        if std and std not in ("nan", "해당없음"):
            std_col_order.append(std)

    year_to_std = {}
    for i, yr in enumerate(years_order):
        col_idx = i * 2
        year_map = {}
        used_std = set()
        for _, row in data.iterrows():
            std    = str(row.iloc[0]).strip()       # 2025 표준명
            yr_col = str(row.iloc[col_idx]).strip() # 해당 연도 원본명
            if (not std    or std    in ("nan", "해당없음") or
                not yr_col or yr_col in ("nan", "해당없음")):
                continue
            if yr_col not in year_map and std not in used_std:
                year_map[yr_col] = std
                used_std.add(std)
        year_to_std[yr] = year_map

    return year_to_std, std_col_order


# ── 단일 연도 읽기 ────────────────────────────────────────────────────────────
def load_year(year: int, year_map: dict, std_col_order: list) -> pd.DataFrame:
    pattern = f"*{year}*.xlsx"
    files   = sorted(RAW_DIR.glob(pattern))
    if not files:
        print(f"  [SKIP] {year}년 파일 없음")
        return pd.DataFrame()

    path = files[0]
    print(f"  [{year}] {path.name}", end=" ")

    df_raw = pd.read_excel(path, header=HEADER_ROW[year], dtype=object)
    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    print(f"→ {len(df_raw):,}행 × {len(df_raw.columns)}열", end=" ")

    # 매핑된 컬럼만 추출 + 표준명으로 rename
    available = {orig: std for orig, std in year_map.items() if orig in df_raw.columns}
    df_std = df_raw[list(available.keys())].rename(columns=available).copy()

    # 표준 컬럼 전체를 순서대로 맞추기 (없는 컬럼은 NaN)
    row_data = {"SURVEY_YEAR": [year] * len(df_std)}
    for col in std_col_order:
        row_data[col] = df_std[col].values if col in df_std.columns else np.nan
    df_out = pd.DataFrame(row_data)

    mapped   = len(available)
    total    = len(df_raw.columns)
    excluded = total - mapped
    print(f"(매핑:{mapped} | 미포함:{excluded})")

    # 연도별 원본 전체도 별도 저장 (분석 필요 시 참고용)
    raw_out = RAW_BY_YEAR / f"{year}_raw.csv"
    pd.concat([pd.DataFrame({"SURVEY_YEAR": [year]*len(df_raw)}), df_raw], axis=1)\
      .to_csv(raw_out, index=False, encoding="utf-8-sig")

    return df_out


# ── 전체 파이프라인 ────────────────────────────────────────────────────────────
def build_master() -> pd.DataFrame:
    print("▶ 매핑 테이블 로드 중...")
    year_to_std, std_col_order = load_mapping(MAPPING_FILE)
    print(f"  → 표준 컬럼 {len(std_col_order)}개, {len(year_to_std)}개 연도 매핑 완료\n")

    print("▶ 연도별 데이터 읽는 중...")
    frames = []
    for year in sorted(year_to_std.keys()):
        df = load_year(year, year_to_std[year], std_col_order)
        if not df.empty:
            frames.append(df)

    print("\n▶ 통합 중...")
    master = pd.concat(frames, ignore_index=True)

    # 숫자형 변환
    num_cols = ["SEX", "AGE", "YEAR", "MON", "Q1", "Q2", "Q3", "Q4",
                "Q5_1", "Q5_2", "Q5_3", "Q5_4",
                "Q7", "Q16", "DQ1_1", "DQ1_2", "DQ3", "DQ4"]
    for col in num_cols:
        if col in master.columns:
            master[col] = pd.to_numeric(master[col], errors="coerce")

    return master


# ── 요약 출력 ──────────────────────────────────────────────────────────────────
def print_summary(df: pd.DataFrame):
    print("\n" + "=" * 60)
    print(f"통합 데이터셋  shape: {df.shape}")
    print(f"  → {df['SURVEY_YEAR'].nunique()}개 연도, "
          f"{len(df):,}명, {len(df.columns)}개 표준 컬럼")
    print("=" * 60)

    print("\n▶ 연도별 행 수")
    print(df["SURVEY_YEAR"].value_counts().sort_index().to_string())

    key_cols = ["SEX", "AGE", "Q1", "Q7", "Q16", "DQ1_1", "DQ3", "DQ4"]
    key_cols = [c for c in key_cols if c in df.columns]
    print(f"\n▶ 핵심 컬럼 결측률 (%)")
    missing = (df[key_cols].isna().mean() * 100).round(1)
    print(missing.to_string())

    if "Q16" in df.columns or "Q7" in df.columns:
        target = "Q16" if "Q16" in df.columns else "Q7"
        print(f"\n▶ {target} (규칙적 체육활동 참여 빈도) 연도별 분포")
        pivot = (df.groupby("SURVEY_YEAR")[target]
                 .value_counts(normalize=True)
                 .mul(100).round(1)
                 .unstack(fill_value=0.0))
        print(pivot.to_string())


# ── 실행 ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  국민생활체육조사 통합 전처리 시작")
    print("=" * 60 + "\n")

    master = build_master()

    out_path = OUT_DIR / "master.csv"
    master.to_csv(out_path, index=False, encoding="utf-8-sig")

    print_summary(master)
    print(f"\n✓ 저장 완료:")
    print(f"  통합 데이터 : {out_path}  ({out_path.stat().st_size/1024/1024:.1f} MB)")
    print(f"  연도별 원본 : data/processed/raw_by_year/{{year}}_raw.csv")