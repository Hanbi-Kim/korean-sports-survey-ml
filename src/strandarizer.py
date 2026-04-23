"""
국민생활체육조사 통합 데이터 표준화 스크립트

입력 : data/processed/master.csv
출력 : data/processed/master_standardized.csv
      data/processed/standardize_report.txt  (변환 내역 리포트)

단계:
  1. 결측치 비율이 높은 컬럼 제거
  2. 수치형 변환
  3. 지역 코드 통일 (CODE3)
  4. 연령 표준화 (AGE: 코드 → 실제 나이)
  5. 기타 카테고리 정제

실행: python src/standardize.py
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path

IN_PATH  = Path("data/processed/master.csv")
OUT_PATH = Path("data/processed/master_standardized.csv")
RPT_PATH = Path("data/processed/standardize_report.txt")

# ── 설정 ──────────────────────────────────────────────────────────────────────
MISSING_DROP_THRESHOLD = 0.9   # 결측률 90% 이상 컬럼 제거
                                # (실제 전체 데이터 기준으로 조정 권장)

report_lines = []
def log(msg=""):
    print(msg)
    report_lines.append(msg)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1. 결측 컬럼 제거
# ══════════════════════════════════════════════════════════════════════════════
def step1_drop_missing(df: pd.DataFrame) -> pd.DataFrame:
    log("\n" + "─"*60)
    log("STEP 1.  결측치 과다 컬럼 제거")
    log("─"*60)

    miss_rate = df.isna().mean()
    drop_cols = miss_rate[miss_rate >= MISSING_DROP_THRESHOLD].index.tolist()

    log(f"기준 : 결측률 {MISSING_DROP_THRESHOLD*100:.0f}% 이상")
    log(f"제거 : {len(drop_cols)}개 컬럼")
    log(f"유지 : {len(df.columns) - len(drop_cols)}개 컬럼")

    if drop_cols:
        log("\n제거된 컬럼 목록:")
        for c in drop_cols:
            log(f"  {c:30s}  결측 {miss_rate[c]*100:.1f}%")

    return df.drop(columns=drop_cols)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2. 수치형 변환
# ══════════════════════════════════════════════════════════════════════════════
NUM_COLS = [
    "SEX", "AGE", "YEAR", "MON",
    "Q1", "Q2", "Q3", "Q4",
    "Q5_1", "Q5_2", "Q5_3", "Q5_4",
    "Q6_1", "Q6_2", "Q6_3",
    "Q7", "Q16",
    "DQ1_1", "DQ1_2", "DQ2_1", "DQ2_2", "DQ2_3",
    "DQ3", "DQ4", "DQ4_1",
]

def step2_numeric(df: pd.DataFrame) -> pd.DataFrame:
    log("\n" + "─"*60)
    log("STEP 2.  수치형 변환")
    log("─"*60)
    converted = []
    for col in NUM_COLS:
        if col in df.columns:
            before = df[col].dtype
            df[col] = pd.to_numeric(df[col], errors="coerce")
            converted.append(f"  {col:15s}  {str(before):10s} → {str(df[col].dtype)}")
    log("\n".join(converted))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3. 지역 코드 통일 (CODE3 - 시도)
# ══════════════════════════════════════════════════════════════════════════════

# 숫자 코드 → 표준 시도명 (2016/2017 AREA 코드 기준)
REGION_CODE_MAP = {
    11: "서울특별시",   21: "부산광역시",   22: "대구광역시",
    23: "인천광역시",   24: "광주광역시",   25: "대전광역시",
    26: "울산광역시",   29: "세종특별자치시",
    31: "경기도",       32: "강원도",        33: "충청북도",
    34: "충청남도",     35: "전라북도",      36: "전라남도",
    37: "경상북도",     38: "경상남도",      39: "제주특별자치도",
}

# 텍스트 변형 → 표준명
REGION_TEXT_MAP = {
    # 서울
    "서울": "서울특별시",
    "서울특별시": "서울특별시",
    # 부산
    "부산": "부산광역시",
    "부산광역시": "부산광역시",
    # 대구
    "대구": "대구광역시",
    "대구광역시": "대구광역시",
    # 인천
    "인천": "인천광역시",
    "인천광역시": "인천광역시",
    # 광주
    "광주": "광주광역시",
    "광주광역시": "광주광역시",
    # 대전
    "대전": "대전광역시",
    "대전광역시": "대전광역시",
    # 울산
    "울산": "울산광역시",
    "울산광역시": "울산광역시",
    # 세종
    "세종": "세종특별자치시",
    "세종특별자치시": "세종특별자치시",
    # 경기
    "경기": "경기도",
    "경기도": "경기도",
    # 강원
    "강원": "강원도",
    "강원도": "강원도",
    "강원특별자치도": "강원도",
    # 충북
    "충북": "충청북도",
    "충청북도": "충청북도",
    # 충남
    "충남": "충청남도",
    "충청남도": "충청남도",
    # 전북
    "전북": "전라북도",
    "전라북도": "전라북도",
    "전북특별자치도": "전라북도",
    # 전남
    "전남": "전라남도",
    "전라남도": "전라남도",
    # 경북
    "경북": "경상북도",
    "경상북도": "경상북도",
    # 경남
    "경남": "경상남도",
    "경상남도": "경상남도",
    # 제주
    "제주": "제주특별자치도",
    "제주특별자치도": "제주특별자치도",
    "제주도": "제주특별자치도",
}

def normalize_region(val):
    """단일 지역값 정규화"""
    if pd.isna(val):
        return np.nan
    s = str(val).strip()

    # 숫자 코드 처리 ("11", "21" 등)
    try:
        code = int(float(s))
        if code in REGION_CODE_MAP:
            return REGION_CODE_MAP[code]
    except (ValueError, TypeError):
        pass

    # "01. 서울특별시" 형태 → "서울특별시"
    cleaned = re.sub(r"^\d+\.\s*", "", s).strip()

    # 텍스트 매핑
    if cleaned in REGION_TEXT_MAP:
        return REGION_TEXT_MAP[cleaned]

    # 부분 일치 (앞 2글자로 시도 추정)
    for key, std in REGION_TEXT_MAP.items():
        if len(cleaned) >= 2 and cleaned[:2] == key[:2]:
            return std

    return cleaned  # 매핑 실패 시 정제된 텍스트 반환

def step3_region(df: pd.DataFrame) -> pd.DataFrame:
    log("\n" + "─"*60)
    log("STEP 3.  지역 코드 통일 (CODE3 - 시도)")
    log("─"*60)

    if "CODE3" not in df.columns:
        log("  CODE3 컬럼 없음, 스킵")
        return df

    # 2022년 CODE3는 읍면동 구분(동부/읍면부)이 잘못 매핑됨 → NaN 처리
    mask_2022_wrong = (df["SURVEY_YEAR"] == 2022) & df["CODE3"].str.contains("동부|읍면", na=False)
    if mask_2022_wrong.sum() > 0:
        df.loc[mask_2022_wrong, "CODE3"] = np.nan
        log(f"  2022년 CODE3 오매핑 → NaN: {mask_2022_wrong.sum()}건")

    before = df["CODE3"].value_counts(dropna=False)
    df["CODE3"] = df["CODE3"].apply(normalize_region)
    after  = df["CODE3"].value_counts(dropna=False)

    log("\n변환 전:")
    log(before.to_string())
    log("\n변환 후:")
    log(after.to_string())

    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4. 연령 표준화
# ══════════════════════════════════════════════════════════════════════════════

# 2016년 AGE 코드 → 연령대 대표값 (중간값)
AGE_CODE_MAP = {
    1: 15,   # 10대
    2: 25,   # 20대
    3: 35,   # 30대
    4: 45,   # 40대
    5: 55,   # 50대
    6: 65,   # 60대 이상
}

def step4_age(df: pd.DataFrame) -> pd.DataFrame:
    log("\n" + "─"*60)
    log("STEP 4.  연령 표준화")
    log("─"*60)

    if "AGE" not in df.columns:
        log("  AGE 컬럼 없음, 스킵")
        return df

    age = df["AGE"].copy()
    year_col = df["SURVEY_YEAR"]

    # 2016/2017: AGE는 연령대 코드 (1~6)
    # → 중간값으로 대체하고 AGE_GROUP 컬럼 추가
    mask_code = year_col.isin([2016, 2017]) & age.notna()

    # 실제 나이인지 코드인지 구분: 1~6 범위면 코드
    possible_code = mask_code & age.between(1, 6)

    if possible_code.sum() > 0:
        df.loc[possible_code, "AGE"] = age[possible_code].map(AGE_CODE_MAP)
        log(f"  2016/2017 연령대 코드 → 대표값 변환: {possible_code.sum()}건")
        log(f"  (1=10대→15, 2=20대→25, 3=30대→35, 4=40대→45, 5=50대→55, 6=60대→65)")

    # 출생연도(YEAR)로 실제 나이 보완 (AGE가 NaN인 경우)
    if "YEAR" in df.columns:
        year_vals = pd.to_numeric(df["YEAR"], errors="coerce")
        # 유효한 출생연도 범위: 1920~2015
        valid_birth = year_vals.between(1920, 2015)
        missing_age = df["AGE"].isna() & valid_birth

        if missing_age.sum() > 0:
            df.loc[missing_age, "AGE"] = (
                df.loc[missing_age, "SURVEY_YEAR"] - year_vals[missing_age]
            ).astype("Int64")
            log(f"\n  출생연도로 AGE 보완: {missing_age.sum()}건")

    # AGE 이상값 제거 (10세 미만, 100세 초과)
    age_num = pd.to_numeric(df["AGE"], errors="coerce")
    invalid = age_num.notna() & ~age_num.between(10, 100)
    if invalid.sum() > 0:
        df.loc[invalid, "AGE"] = np.nan
        log(f"\n  AGE 이상값 제거 (10세 미만 / 100세 초과): {invalid.sum()}건")

    # AGE_GROUP 파생변수 생성
    age_num = pd.to_numeric(df["AGE"], errors="coerce")
    df["AGE_GROUP"] = pd.cut(
        age_num,
        bins=[0, 19, 29, 39, 49, 59, 69, 120],
        labels=["10대", "20대", "30대", "40대", "50대", "60대", "70대이상"],
    )

    log("\n  AGE_GROUP 분포:")
    log(df["AGE_GROUP"].value_counts().sort_index().to_string())

    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5. 기타 카테고리 정제
# ══════════════════════════════════════════════════════════════════════════════
def step5_other(df: pd.DataFrame) -> pd.DataFrame:
    log("\n" + "─"*60)
    log("STEP 5.  기타 카테고리 정제")
    log("─"*60)

    changes = []

    # SEX: 1=남, 2=여 확인
    if "SEX" in df.columns:
        invalid_sex = ~df["SEX"].isin([1.0, 2.0, np.nan])
        if invalid_sex.sum() > 0:
            df.loc[invalid_sex, "SEX"] = np.nan
            changes.append(f"  SEX 이상값 → NaN: {invalid_sex.sum()}건")
        df["SEX_LABEL"] = df["SEX"].map({1.0: "남", 2.0: "여"})
        changes.append("  SEX_LABEL 컬럼 추가 (1→남, 2→여)")

    # YEAR (출생연도) 이상값 처리
    if "YEAR" in df.columns:
        yr = pd.to_numeric(df["YEAR"], errors="coerce")
        invalid_yr = yr.notna() & ~yr.between(1920, 2015)
        if invalid_yr.sum() > 0:
            df.loc[invalid_yr, "YEAR"] = np.nan
            changes.append(f"  YEAR 이상값 → NaN: {invalid_yr.sum()}건 (범위: 1920~2015)")

    # DQ3 (월평균가구소득) 이상값 처리 (0~2000만원 범위 가정)
    if "DQ3" in df.columns:
        inc = pd.to_numeric(df["DQ3"], errors="coerce")
        invalid_inc = inc.notna() & ~inc.between(0, 2000)
        if invalid_inc.sum() > 0:
            df.loc[invalid_inc, "DQ3"] = np.nan
            changes.append(f"  DQ3(소득) 이상값 → NaN: {invalid_inc.sum()}건 (범위: 0~2000만원)")

    # Q16 / Q7 범위 확인 (1~9)
    for col in ["Q16", "Q7"]:
        if col in df.columns:
            val = pd.to_numeric(df[col], errors="coerce")
            invalid = val.notna() & ~val.between(1, 9)
            if invalid.sum() > 0:
                df.loc[invalid, col] = np.nan
                changes.append(f"  {col} 범위 초과 → NaN: {invalid.sum()}건")

    # Q1 (건강상태) 범위 확인 (1~5)
    if "Q1" in df.columns:
        val = pd.to_numeric(df["Q1"], errors="coerce")
        invalid = val.notna() & ~val.between(1, 5)
        if invalid.sum() > 0:
            df.loc[invalid, "Q1"] = np.nan
            changes.append(f"  Q1 범위 초과 → NaN: {invalid.sum()}건")

    log("\n".join(changes) if changes else "  변경사항 없음")

    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6. 컬럼 순서 정리 및 최종 요약
# ══════════════════════════════════════════════════════════════════════════════
PRIORITY_COLS = [
    "SURVEY_YEAR",
    "SEX", "SEX_LABEL", "AGE", "AGE_GROUP", "YEAR", "MON",
    "CODE3",  # 시도
    "CODE4",  # 시군구
    "CODE5",  # 읍면동
    "APT",    # 주거유형
    "Q1",     # 건강상태
    "Q16",    # 규칙적 체육활동 참여 빈도  (핵심 타겟)
    "Q7",     # 규칙적 체육활동 참여 주기
    "DQ1_1",  # 최종학력
    "DQ3",    # 월평균 가구소득
    "DQ4",    # 직업유무
]

def step6_finalize(df: pd.DataFrame) -> pd.DataFrame:
    log("\n" + "─"*60)
    log("STEP 6.  컬럼 정리 및 최종 요약")
    log("─"*60)

    # 우선 컬럼을 앞으로, 나머지는 알파벳 순
    priority = [c for c in PRIORITY_COLS if c in df.columns]
    rest     = sorted([c for c in df.columns if c not in priority])
    df       = df[priority + rest]

    log(f"\n최종 shape: {df.shape}")
    log(f"우선 컬럼 ({len(priority)}개): {priority}")

    log("\n핵심 컬럼 결측률:")
    key = [c for c in PRIORITY_COLS if c in df.columns]
    miss = (df[key].isna().mean() * 100).round(1)
    log(miss.to_string())

    log("\n연도별 행 수:")
    log(df["SURVEY_YEAR"].value_counts().sort_index().to_string())

    return df


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log("=" * 60)
    log("  국민생활체육조사 데이터 표준화")
    log("=" * 60)
    log(f"입력: {IN_PATH}")

    df = pd.read_csv(IN_PATH, dtype=str)
    log(f"원본 shape: {df.shape}\n")

    df = step1_drop_missing(df)
    df = step2_numeric(df)
    df = step3_region(df)
    df = step4_age(df)
    df = step5_other(df)
    df = step6_finalize(df)

    # 저장
    df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    log(f"\n✓ 저장 완료: {OUT_PATH}")
    log(f"  파일 크기: {OUT_PATH.stat().st_size / 1024 / 1024:.1f} MB")

    # 리포트 저장
    RPT_PATH.write_text("\n".join(report_lines), encoding="utf-8")
    log(f"✓ 리포트 저장: {RPT_PATH}")