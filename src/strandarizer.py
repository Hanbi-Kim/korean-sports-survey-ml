"""
국민생활체육조사 연도별 데이터 표준화 (2016~2024)
==================================================
프로젝트 구조:
    korean-sports-survey-ml/
    ├── data/
    │   ├── 1_raw/
    │   ├── 2_codebook/
    │   ├── 4_mapping_table/
    │   └── 5_processed/
    └── src/
        └── strandarizer.py   ← 이 파일

사용법 (프로젝트 루트에서 실행):
    python src/strandarizer.py --year 2023
    python src/strandarizer.py --year 2021 2022 2023
    python src/strandarizer.py              # 전체
"""

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────
# 경로
# ──────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent  # src/ 기준 2단계 위 = 프로젝트 루트
RAW_DIR  = BASE_DIR / "data" / "1_raw"
MAP_DIR  = BASE_DIR / "data" / "4_mapping_table"
CB_FILE  = BASE_DIR / "data" / "2_codebook" / "2025_codebook.xlsx"
OUT_DIR  = BASE_DIR / "data" / "5_processed"
REF_CSV  = OUT_DIR  / "survey_2025_standardized.csv"

HEADER_MAP = {
    2016: 0, 2017: 0,
    2018: 1, 2019: 1, 2020: 1, 2021: 1, 2022: 1,
    2023: 2, 2024: 1,
}

# ──────────────────────────────────────────────────────
# 시도 관련 상수
# ──────────────────────────────────────────────────────

# 2016/2018/2021/2022: AREA = 2자리 코드 (11, 21, 22 ...)
AREA_2DIGIT = {
    "11":"서울특별시", "21":"부산광역시", "22":"대구광역시",
    "23":"인천광역시", "24":"광주광역시", "25":"대전광역시",
    "26":"울산광역시", "29":"세종특별자치시","31":"경기도",
    "32":"강원특별자치도","33":"충청북도","34":"충청남도",
    "35":"전라북도","36":"전라남도","37":"경상북도",
    "38":"경상남도","39":"제주특별자치도",
}

# 2017: AREA = 1자리 코드 (1~17)
AREA_1DIGIT = {
    "1":"서울특별시",  "2":"부산광역시",  "3":"대구광역시",
    "4":"인천광역시",  "5":"광주광역시",  "6":"대전광역시",
    "7":"울산광역시",  "8":"세종특별자치시","9":"경기도",
    "10":"강원특별자치도","11":"충청북도","12":"충청남도",
    "13":"전라북도","14":"전라남도","15":"경상북도",
    "16":"경상남도","17":"제주특별자치도",
}

# 2019: 시도 "01. 서울특별시" → 표준명
SIDO_PREFIX = {
    "01":"서울특별시","02":"부산광역시","03":"대구광역시",
    "04":"인천광역시","05":"광주광역시","06":"대전광역시",
    "07":"울산광역시","08":"세종특별자치시","09":"경기도",
    "10":"강원특별자치도","11":"충청북도","12":"충청남도",
    "13":"전라북도","14":"전라남도","15":"경상북도",
    "16":"경상남도","17":"제주특별자치도",
}

# 2020: SQ5_1_TEXT 자유텍스트 정규화
REGION_RE = [
    (r"서울",                  "서울특별시"),
    (r"부산",                  "부산광역시"),
    (r"대구",                  "대구광역시"),
    (r"인천",                  "인천광역시"),
    (r"광주",                  "광주광역시"),
    (r"대전",                  "대전광역시"),
    (r"울산",                  "울산광역시"),
    (r"세종",                  "세종특별자치시"),
    (r"수원|성남|고양|용인|부천|안산|화성|안양|경기",  "경기도"),
    (r"강원",                  "강원특별자치도"),
    (r"청주|충북|충청북",       "충청북도"),
    (r"천안|아산|충남|충청남",  "충청남도"),
    (r"전주|익산|전북|전라북",  "전라북도"),
    (r"순천|목포|전남|전라남",  "전라남도"),
    (r"포항|경주|경북|경상북",  "경상북도"),
    (r"창원|김해|경남|경상남",  "경상남도"),
    (r"제주",                  "제주특별자치도"),
]

# ──────────────────────────────────────────────────────
# 코드북에 레이블 없는 변수 → 하드코딩
# ──────────────────────────────────────────────────────
HARD_LABELS = {
    "SEX": {1:"남성", 2:"여성"},
    "HTYPE": {1:"아파트", 2:"아파트 외"},
    "APT":   {1:"아파트", 2:"아파트 외"},
}


# ══════════════════════════════════════════════════════
# 매핑 로드
# ══════════════════════════════════════════════════════

def load_mapping(map_file: Path, year: int):
    df_var = pd.read_excel(map_file, sheet_name="변수_매핑", header=1)
    rename_map, drop_cols = {}, set()
    for _, row in df_var.iterrows():
        c_src = str(row.iloc[0]).strip()
        c_dst = str(row.iloc[3]).strip()
        if not c_src or c_src == "nan": continue
        if c_dst and c_dst != "nan": rename_map[c_src] = c_dst
        else: drop_cols.add(c_src)

    # 값_매핑 (header=0, 위치 기반)
    df_val = pd.read_excel(map_file, sheet_name="값_매핑", header=0)
    val_rules = {}
    for _, row in df_val.iterrows():
        c_src = str(row.iloc[0]).strip()
        c_dst = str(row.iloc[1]).strip()
        v_src = str(row.iloc[2]).strip()
        v_dst = str(row.iloc[3]).strip()
        if not c_src or c_src == "nan": continue
        if not c_dst or c_dst == "nan": continue
        if v_src in ("nan","(없음)","") or v_dst in ("nan","(없음)",""): continue
        cur_col = rename_map.get(c_src, c_src)
        val_rules.setdefault(cur_col, [])
        val_rules[cur_col].append((v_src, v_dst))

    # 연도별 지역 코드 변환
    if year in (2016, 2018):
        # AREA = 2자리 코드 (11, 21, 22 ...)
        target = rename_map.get("AREA", "CODE3")
        val_rules[target] = [(k, v) for k, v in AREA_2DIGIT.items()]

    elif year in (2017, 2022):
        # AREA = 1자리 코드 (1~17)
        target = rename_map.get("AREA", "CODE3")
        val_rules[target] = [(k, v) for k, v in AREA_1DIGIT.items()]

    elif year == 2021:
        # LOC1 = 1자리 코드 (1~17)
        target = rename_map.get("LOC1", "CODE3")
        val_rules[target] = [(k, v) for k, v in AREA_1DIGIT.items()]

    elif year == 2024:
        # 강원도 → 강원특별자치도
        val_rules.setdefault("CODE3", [])
        val_rules["CODE3"].append(("강원도", "강원특별자치도"))

    return rename_map, drop_cols, val_rules


# ══════════════════════════════════════════════════════
# 코드북 로드
# ══════════════════════════════════════════════════════

def load_codebook(cb_file: Path):
    df_cb = pd.read_excel(cb_file, sheet_name="코드정의")
    code_map = {}
    for _, row in df_cb.iterrows():
        col       = str(row["변수코드"]).strip()
        code_val  = str(row["코드값"]).strip()
        label_val = str(row["코드레이블"]).strip()
        if not col or col == "nan": continue
        if code_val in ("(연속형/자유값)","(주관식)","(없음)","nan",""): continue
        if not label_val or label_val == "nan": continue  # NaN 레이블 스킵
        code_map.setdefault(col, {})
        try:    code_map[col][int(float(code_val))] = label_val
        except: pass
        code_map[col][code_val] = label_val

    # 하드코딩 레이블 추가 (코드북에 없는 것)
    for col, mapping in HARD_LABELS.items():
        if col not in code_map:
            code_map[col] = {}
        for k, v in mapping.items():
            code_map[col][k] = v
            code_map[col][str(k)] = v

    return code_map


# ══════════════════════════════════════════════════════
# 연도별 전용 전처리
# ══════════════════════════════════════════════════════

def preprocess(df: pd.DataFrame, year: int) -> pd.DataFrame:

    if year == 2019:
        # BIRTH (YYYYMM) → YEAR + MON
        if "BIRTH" in df.columns:
            birth_str = df["BIRTH"].astype(str).str.strip().str.zfill(6)
            valid = birth_str.str.len() == 6
            df["YEAR"] = pd.to_numeric(birth_str.where(valid).str[:4], errors="coerce")
            df["MON"]  = pd.to_numeric(birth_str.where(valid).str[4:6], errors="coerce")
            df = df.drop(columns=["BIRTH"])
            print(f"  BIRTH 분리 완료 (비정상값 {(~valid).sum()}건 NaN)")

        # 시도 "01. 서울특별시" → "서울특별시"
        if "시도" in df.columns:
            def norm_sido(v):
                s = str(v).strip()
                m = re.match(r'^(\d{2})[.\s]', s)
                return SIDO_PREFIX.get(m.group(1), s) if m else s
            df["시도"] = df["시도"].apply(norm_sido)

        # 동읍면부 "1. 동부" → "동부"
        if "동읍면부" in df.columns:
            lmap = {"1. 동부": "동부", "2. 읍면부": "읍/면부"}
            df["동읍면부"] = df["동읍면부"].map(lambda x: lmap.get(str(x).strip(), x))

    elif year == 2020:
        # SQ7 (YYYYMM) → YEAR + MON
        if "SQ7" in df.columns:
            sq7 = df["SQ7"].astype(str).str.strip().str.zfill(6)
            df["YEAR"] = pd.to_numeric(sq7.str[:4], errors="coerce")
            df["MON"]  = pd.to_numeric(sq7.str[4:6], errors="coerce")
            df = df.drop(columns=["SQ7"])
            print("  SQ7 분리 완료")

        # SQ5_1_TEXT 자유텍스트 → 표준 시도명
        if "SQ5_1_TEXT" in df.columns:
            def norm_region(v):
                if pd.isna(v): return v
                s = str(v).strip()
                for pat, name in REGION_RE:
                    if re.search(pat, s): return name
                return v
            before = df["SQ5_1_TEXT"].copy()
            df["SQ5_1_TEXT"] = df["SQ5_1_TEXT"].apply(norm_region)
            changed = (before.astype(str) != df["SQ5_1_TEXT"].astype(str)).sum()
            unmatched = df.loc[
                ~df["SQ5_1_TEXT"].isin([v for _, v in REGION_RE]), "SQ5_1_TEXT"
            ].dropna().unique()
            print(f"  SQ5_1_TEXT 정규화: {changed:,}건"
                  + (f" / ⚠️ 미매칭: {list(unmatched[:5])}" if len(unmatched) else ""))

        # SQ1_1_TEXT 동/읍/면 → 동부/읍면부
        if "SQ1_1_TEXT" in df.columns:
            lmap = {"동": "동부", "읍": "읍/면부", "면": "읍/면부"}
            df["SQ1_1_TEXT"] = df["SQ1_1_TEXT"].map(
                lambda x: lmap.get(str(x).strip(), x)
            )

    return df


# ══════════════════════════════════════════════════════
# 값 변환
# ══════════════════════════════════════════════════════

def apply_val_rules(df: pd.DataFrame, val_rules: dict) -> int:
    total = 0
    for col, rules in val_rules.items():
        if col not in df.columns: continue
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].astype(object)
        for v_src, v_dst in rules:
            mask = df[col].astype(str).str.strip() == str(v_src).strip()
            try:
                nv = int(v_src) if str(v_src).isdigit() else float(v_src)
                mask = mask | (df[col] == nv)
            except: pass
            cnt = int(mask.sum())
            if cnt > 0:
                df.loc[mask, col] = v_dst
                total += cnt
    return total


def apply_codebook(df: pd.DataFrame, code_map: dict) -> int:
    total = 0
    for col, mapping in code_map.items():
        if col not in df.columns: continue
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].astype(object)
        before = df[col].copy()
        df[col] = df[col].map(
            lambda x: mapping.get(x, mapping.get(str(x).strip(), x))
        )
        total += (before.astype(str) != df[col].astype(str)).sum()
    return total


# ══════════════════════════════════════════════════════
# 메인 처리
# ══════════════════════════════════════════════════════

def standardize(year: int) -> bool:
    raw_file = RAW_DIR / f"DATA_{year}년 국민생활체육조사.xlsx"
    map_file = MAP_DIR / f"{year}_mapping.xlsx"

    if not raw_file.exists():
        print(f"  ⚠️  원본 파일 없음: {raw_file.name}"); return False
    if not map_file.exists():
        print(f"  ⚠️  매핑 파일 없음: {map_file.name}"); return False

    try:
        # 매핑 로드
        rename_map, drop_cols, val_rules = load_mapping(map_file, year)
        print(f"  매핑  : 리네임 {len(rename_map)}개 / 제거 {len(drop_cols)}개 / "
              f"값변환 {len(val_rules)}컬럼({sum(len(v) for v in val_rules.values())}건)")

        # 코드북 로드
        code_map = load_codebook(CB_FILE)
        print(f"  코드북: {len(code_map)}개 컬럼 / "
              f"{sum(len(v) for v in code_map.values())}개 레이블")

        # 원본 로드
        header = HEADER_MAP.get(year, 1)
        df = pd.read_excel(raw_file, header=header)
        if str(df.iloc[0, 0]).strip() == "ID":
            df = df.iloc[1:].reset_index(drop=True)
        print(f"  원본  : {df.shape[0]:,}행 × {df.shape[1]}열  (header={header})")

        # 연도별 전처리
        df = preprocess(df, year)

        # 컬럼 제거 & 리네임
        drop_cols.discard("BIRTH")
        drop_cols.discard("SQ7")
        df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")
        df = df.rename(columns=rename_map)
        df = df.loc[:, ~df.columns.duplicated()]

        # 값 변환
        changed_diff  = apply_val_rules(df, val_rules)
        changed_label = apply_codebook(df, code_map)
        print(f"  값변환: {changed_diff:,}건 / 레이블: {changed_label:,}건")

        # 2025 컬럼 순서
        if REF_CSV.exists():
            cols_ref = [c for c in pd.read_csv(REF_CSV, nrows=0).columns if c != "연도"]
            ordered  = [c for c in cols_ref if c in df.columns]
            extra    = [c for c in df.columns if c not in cols_ref]
            df = df[ordered + extra]
            if extra:
                print(f"  ⚠️  {year}에만 있는 컬럼 ({len(extra)}개): {extra[:5]}")

        # 저장
        df.insert(0, "연도", year)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        out_csv = OUT_DIR / f"survey_{year}_standardized.csv"
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        print(f"  저장  : {out_csv.name}  [{df.shape[0]:,}행 × {df.shape[1]}열]")
        return True

    except Exception as e:
        print(f"  ❌ 오류: {e}")
        import traceback; traceback.print_exc()
        return False


# ══════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="국민생활체육조사 연도별 표준화")
    parser.add_argument(
        "--year", type=int, nargs="+",
        help="처리할 연도 (예: --year 2023 / --year 2021 2022 2023 / 미입력 시 전체)"
    )
    args = parser.parse_args()

    if args.year:
        target_years = sorted(args.year)
    else:
        target_years = sorted([
            int(f.stem.replace("_mapping", ""))
            for f in MAP_DIR.glob("*_mapping.xlsx")
            if f.stem.replace("_mapping", "").isdigit()
            and int(f.stem.replace("_mapping", "")) != 2025
        ])
        if not target_years:
            print("❌ 매핑 파일 없음. --year 로 연도를 직접 지정하세요.")
            sys.exit(1)
        print(f"처리 대상: {target_years}\n")

    results = {}
    for year in target_years:
        print(f"\n{'='*50}")
        print(f"[{year}] 표준화 시작")
        print(f"{'='*50}")
        results[year] = standardize(year)

    print(f"\n{'='*50}")
    print("결과 요약")
    print(f"{'='*50}")
    for year in sorted(results):
        print(f"  {year}: {'✅ 완료' if results[year] else '❌ 실패'}")

    if any(not v for v in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()