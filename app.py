"""
무토 스파 통합 관리 시스템 v5
핸드SOS CSV → 매출/순이익 · 수익구조 · 월별트렌드 · CRM · 담당자 분석
"""

import json
import os
import re
import warnings
from calendar import monthrange
from datetime import datetime, time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

warnings.filterwarnings("ignore")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전역 상수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MASTER_PATH          = "spa_master_data.csv"
CONVERSION_PATH      = "spa_conversion_data.json"
DIRECTOR_KEYWORD     = "원장님"
FREELANCER_KEYWORD   = "아로마"
DIRECTOR_MAX_PER_DAY = 4
FREELANCER_BEDS      = 7
DEFAULT_DURATION     = 90
TARGET_UTIL          = 70.0
TARGET_UNIT_PRICE    = 150_000

# ── 운영 기준 상수 (Operating Framework) ────────────────────────────────────
# 매장 단계 기준: (월 차감매출 상한, 단계명, 설명)
# 단계 기준: (차감매출 상한, 단계명, 설명)
# 1.5단계는 매출이 아닌 인력 구조로 감지 — get_store_stage() 참조
STAGE_THRESHOLDS: list[tuple[float, str, str]] = [
    (1_500_000,    "1단계",   "원장 1인 운영 — 노동형 구조, 매출이 원장 시술 건수와 직결"),
    (5_000_000,    "2단계",   "고정 직원 존재 — 구조 운영 시작, 인건비 관리가 핵심"),
    (8_000_000,    "3단계",   "효율 싸움 — 가동률·객단가로 수익성을 올려야 하는 단계"),
    (float('inf'), "4단계",   "시스템 운영 — 구조가 스스로 돌아가는 단계"),
]
# 1.5단계: 원장 중심 + 프리랜서 보조 구조 (프리랜서 투입이 감지될 때 적용)
STAGE_1_5 = (
    "1.5단계",
    "원장 중심 + 프리랜서 보조 구조 — 개인사업 확장 단계. "
    "아직 조직 운영이 아니다. 가동률과 도민 반복이 핵심이며, "
    "프리랜서는 구조가 아니라 수익 확대 수단이다.",
)
# 비용 구조 기준 (매출 대비 적정 비율)
COST_BENCH = {'인건비': 0.30, '임차료': 0.10, '제품비': 0.10, '홍보비': 0.075}
# 제주 고객 구조 목표
JEJU_DOM_TARGET_PCT  = 60   # 도민 60%
JEJU_TOUR_TARGET_PCT = 40   # 관광객 40%

MENU_CATEGORY = {
    "페이셜":  ["페이셜", "facebody"],
    "뱀부":    ["뱀부", "경추", "브레인", "디톡스", "윤곽", "동안"],
    "아로마":  ["아로마", "트리", "스포츠", "건식"],
    "임산부":  ["임산부"],
    "발":      ["발마사지", "풋테라피"],
}

LABOR_TABLE: list[tuple[str, int, int]] = [
    ("아로마",  60,  30_000),
    ("아로마",  90,  45_000),
    ("아로마", 120,  60_000),
    ("스포츠",  60,  25_000),
    ("스포츠",  90,  40_000),
    ("스포츠", 120,  55_000),
    ("임산부",  60,  35_000),
    ("임산부",  90,  50_000),
    ("임산부", 120,  70_000),
    ("발",       0,  30_000),
]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 페이지 설정 & CSS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
st.set_page_config(
    page_title="무토 스파 통합 관리 시스템",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.main { background: #f3f5ff; }
.hd {
    font-size: 1.95rem; font-weight: 800; text-align: center;
    background: linear-gradient(135deg, #2d3561 0%, #a06ab4 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    padding: .3rem 0 .1rem;
}
.sub { text-align: center; color: #aaa; font-size: .82rem; margin-bottom: .7rem; }
.card {
    background: #fff; border-radius: 14px; padding: 1rem 1.25rem;
    box-shadow: 0 2px 16px rgba(0,0,0,.07); margin-bottom: .65rem;
    border-left: 5px solid #667eea;
}
.card-lbl { font-size: .7rem; color: #999; font-weight: 700;
            letter-spacing: .5px; text-transform: uppercase; }
.card-val { font-size: 1.5rem; font-weight: 800; color: #1a1a2e; line-height: 1.2; }
.card-sub { font-size: .7rem; color: #ccc; margin-top: 2px; }
.up   { color: #22c55e; font-weight: 700; }
.down { color: #ef4444; font-weight: 700; }
.gb { background: #eee; border-radius: 8px; height: 15px; overflow: hidden; margin: 5px 0; }
.gf { border-radius: 8px; height: 15px; }
.bx-g  { background: linear-gradient(135deg,#00b894,#00cec9); color:#fff;
          border-radius:14px; padding:1rem 1.3rem; margin:.5rem 0; }
.bx-o  { background: linear-gradient(135deg,#f7971e,#ffd200); color:#1a1a2e;
          border-radius:14px; padding:1rem 1.3rem; margin:.5rem 0; }
.bx-r  { background: linear-gradient(135deg,#ff6b6b,#ee5a24); color:#fff;
          border-radius:14px; padding:1rem 1.3rem; margin:.5rem 0; }
.bx-b  { background: linear-gradient(135deg,#667eea,#764ba2); color:#fff;
          border-radius:14px; padding:1rem 1.3rem; margin:.5rem 0; }
.bx-gr { background: linear-gradient(135deg,#636e72,#b2bec3); color:#fff;
          border-radius:14px; padding:1rem 1.3rem; margin:.5rem 0; }
.msg { background:#fff; border-radius:12px; border:1px solid #e0e4ff;
       padding:.9rem 1.1rem; margin:.4rem 0;
       box-shadow:0 1px 8px rgba(102,126,234,.1); }
.msg-nm { font-size:.78rem; color:#667eea; font-weight:700; margin-bottom:.3rem; }
.msg-bd { font-size:.86rem; color:#333; line-height:1.6; white-space:pre-wrap; }
.pt th { background:#667eea; color:#fff; padding:.35rem .6rem; font-size:.78rem; }
.pt td { padding:.3rem .6rem; font-size:.8rem; border-bottom:1px solid #f0f0f0; }
</style>
""", unsafe_allow_html=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 유틸리티
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def clean_money(s: pd.Series) -> pd.Series:
    s = (s.astype(str)
           .str.replace('"', '', regex=False)
           .str.replace(',', '', regex=False)
           .str.strip()
           .replace(['', 'nan', 'None', 'NaN', '-'], '0'))
    return pd.to_numeric(s, errors='coerce').fillna(0).astype(int)




# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메뉴 분석 엔진
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def classify_menu(menu: str) -> str:
    text = str(menu).lower()
    for cat, keywords in MENU_CATEGORY.items():
        for kw in keywords:
            if kw.lower() in text:
                return cat
    return "기타"


def extract_minutes(menu: str, default: int = DEFAULT_DURATION) -> int:
    text = str(menu)
    m = re.search(r'(\d+)\s*분', text)
    if m: return int(m.group(1))
    m = re.search(r'(\d+)\s*시간', text)
    if m: return int(m.group(1)) * 60
    for n in re.findall(r'\b(\d+)\b', text):
        if 30 <= int(n) <= 180:
            return int(n)
    return default


def labor_cost(menu: str, category: str) -> int:
    if '원장' in str(menu):
        return 0
    if category == "발":
        return 30_000
    mins = extract_minutes(menu)
    for cat, target_min, price in LABOR_TABLE:
        if cat == category:
            if target_min == 0:
                return price
            if target_min == mins:
                return price
    for cat, target_min, price in LABOR_TABLE:
        if cat == category and target_min > 0:
            if abs(target_min - mins) <= 15:
                return price
    return 0


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    menu_col = next((c for c in ('상세메뉴', '메뉴') if c in df.columns), None)
    if menu_col:
        df['메뉴_카테고리'] = df[menu_col].apply(classify_menu)
        df['시술_시간']    = df[menu_col].apply(extract_minutes)
    else:
        df['메뉴_카테고리'] = "기타"
        df['시술_시간']    = DEFAULT_DURATION

    담당 = df['담당'].astype(str).str.strip() if '담당' in df.columns else pd.Series([''] * len(df))
    is_fl = (담당 != DIRECTOR_KEYWORD) & (담당 != '')
    df['인건비'] = 0
    if menu_col:
        df.loc[is_fl, '인건비'] = df.loc[is_fl].apply(
            lambda r: labor_cost(r[menu_col], r['메뉴_카테고리']), axis=1
        )
    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 데이터 파이프라인
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def load_csv(file) -> pd.DataFrame:
    """
    핸드SOS 매출 CSV 로드.
    skiprows=14 (매출상세조회 포맷) 우선 시도,
    날짜 컬럼을 찾지 못하면 skiprows=1 로 재시도.
    인코딩: utf-8-sig → cp949 → euc-kr
    """
    def _try_load(skip: int) -> pd.DataFrame | None:
        for enc in ('utf-8-sig', 'cp949', 'euc-kr'):
            try:
                file.seek(0)
                return pd.read_csv(file, skiprows=skip, encoding=enc)
            except Exception:
                pass
        return None

    df = _try_load(14)
    if df is not None:
        df.columns = [str(c).replace(' ', '').strip() for c in df.columns]
        date_found = any(c in df.columns for c in ('날짜', '일자', '거래일', '거래일자'))
        if not date_found:
            df = None  # 날짜 컬럼 없으면 skiprows=1 로 재시도

    if df is None:
        df = _try_load(1)
        if df is None:
            raise ValueError("CSV 인코딩/포맷 인식 실패")
        df.columns = [str(c).replace(' ', '').strip() for c in df.columns]

    df = df.dropna(how='all')

    # 날짜 컬럼 통일
    for alias in ('일자', '거래일', '거래일자', '날짜'):
        if alias in df.columns:
            df.rename(columns={alias: '날짜'}, inplace=True)
            break
    if '날짜' in df.columns:
        df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
        df.dropna(subset=['날짜'], inplace=True)

    # 금액 컬럼 정제 ('합계'도 지원)
    for col in ('판매가', '결제액', '합계', '현금', '카드', '포인트', '할인액', '미수금'):
        if col in df.columns:
            df[col] = clean_money(df[col])

    # 건수 정제
    if '건수' in df.columns:
        df['건수'] = pd.to_numeric(
            df['건수'].astype(str).str.replace(',', '', regex=False).str.strip(),
            errors='coerce'
        ).fillna(0).astype(int)

    # ⓪ 방문 컬럼 통일 (핸드SOS '방문' → '방문유형')
    if '방문' in df.columns and '방문유형' not in df.columns:
        df.rename(columns={'방문': '방문유형'}, inplace=True)

    # ① 2차상세 NaN → 1차메뉴로 채우기
    if '2차상세' in df.columns and '1차메뉴' in df.columns:
        df['2차상세'] = df['2차상세'].fillna(df['1차메뉴'])

    # ② fillna 이후 2차상세 한 컬럼으로 필터 (비고 포함, 대소문자 무시)
    if '2차상세' in df.columns:
        df = df[~df['2차상세'].astype(str).str.contains(
            '소계|합계|현황|비고|현금|카드', na=False, case=False
        )]
        df = df.dropna(subset=['2차상세'])

    # ③ 실제 판매된 행만 유지 (건수 > 0)
    if '건수' in df.columns:
        df = df[df['건수'] > 0]

    return df.reset_index(drop=True)


def load_master() -> pd.DataFrame | None:
    if not os.path.exists(MASTER_PATH):
        return None
    try:
        df = pd.read_csv(MASTER_PATH, encoding='utf-8-sig')
        if '날짜' in df.columns:
            df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
        for col in ('판매가', '결제액', '현금', '카드', '건수'):
            if col in df.columns:
                df[col] = clean_money(df[col])
        if '방문' in df.columns and '방문유형' not in df.columns:
            df.rename(columns={'방문': '방문유형'}, inplace=True)
        return df
    except Exception as e:
        st.error(f"마스터 DB 오류: {e}")
        return None


def save_master(df: pd.DataFrame):
    df.to_csv(MASTER_PATH, index=False, encoding='utf-8-sig')


def load_conversion() -> dict:
    if not os.path.exists(CONVERSION_PATH):
        return {}
    try:
        with open(CONVERSION_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def save_conversion(data: dict):
    with open(CONVERSION_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def upsert(existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([existing, new], ignore_index=True) \
               if (existing is not None and not existing.empty) else new.copy()
    keys = [c for c in ('날짜', '고객명', '메뉴', '결제액') if c in combined.columns]
    if keys:
        combined.drop_duplicates(subset=keys, keep='last', inplace=True)
    if '날짜' in combined.columns:
        combined.sort_values('날짜', ascending=False, inplace=True)
    return combined.reset_index(drop=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 담당자 필터 헬퍼
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _담당(df: pd.DataFrame) -> pd.Series:
    if '담당' not in df.columns:
        return pd.Series([''] * len(df), index=df.index)
    return df['담당'].astype(str).str.strip()


def mask_dir(df):  return _담당(df) == DIRECTOR_KEYWORD
def mask_fl(df):   return (_담당(df) != DIRECTOR_KEYWORD) & (_담당(df) != '')
def mask_sisl(df):
    if '구분' not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return df['구분'].astype(str).str.contains('시술', na=False)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 이중 가동률 엔진
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def util_director(dir_count: int, working_days: int) -> float:
    max_slots = working_days * DIRECTOR_MAX_PER_DAY
    if max_slots <= 0: return 0.0
    return round(min(dir_count / max_slots * 100, 100), 1)


def util_freelancer(fl_used_min: int, daily_hours: float, working_days: int) -> float:
    total_min = daily_hours * 60 * working_days * FREELANCER_BEDS
    if total_min <= 0: return 0.0
    return round(min(fl_used_min / total_min * 100, 100), 1)


def true_visit_count(df: pd.DataFrame) -> pd.Series:
    """
    고객별 '진짜 방문 횟수'를 계산한다 (Series, index=고객명).

    규칙:
      동일 고객 + 동일 날짜 + 시간 차이 60분 이내 → 1회 방문으로 처리.
      날짜 컬럼에 시간 정보가 없으면(모두 00:00:00) 날짜 기준 중복 제거.

    이 함수를 쓰면 동반 방문·동시 시술이 여러 건으로 잘못 집계되는 문제를
    방지할 수 있다.
    """
    if df.empty or '고객명' not in df.columns or '날짜' not in df.columns:
        return pd.Series(dtype=int)

    work = df[['고객명', '날짜']].copy()
    work['날짜'] = pd.to_datetime(work['날짜'], errors='coerce')
    work = work.dropna(subset=['날짜'])
    if work.empty:
        return pd.Series(dtype=int)

    # 시간 정보 포함 여부 판단 (hour 또는 minute 가 0이 아닌 행이 있으면 time info 있음)
    has_time = bool(work['날짜'].dt.hour.any() or work['날짜'].dt.minute.any())

    if not has_time:
        # 시간 정보 없음 → 날짜만으로 중복 제거
        work['_date'] = work['날짜'].dt.date
        return (
            work.drop_duplicates(subset=['고객명', '_date'])
            .groupby('고객명')
            .size()
        )

    # 시간 정보 있음 → 60분 세션 윈도우로 방문 분리
    work = work.sort_values(['고객명', '날짜'])
    work['_date'] = work['날짜'].dt.date

    visit_counts: dict[str, int] = {}
    for cust, cust_df in work.groupby('고객명', sort=False):
        count = 0
        for _date, date_df in cust_df.groupby('_date', sort=False):
            times = date_df['날짜'].sort_values().tolist()
            count += 1                    # 하루 첫 기록 = 방문 1회
            session_start = times[0]
            for t in times[1:]:
                diff_min = (t - session_start).total_seconds() / 60
                if diff_min > 60:         # 60분 초과 → 새 방문 세션
                    count += 1
                    session_start = t
        visit_counts[cust] = count

    return pd.Series(visit_counts)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 핵심 지표 계산
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def calc_core(df: pd.DataFrame, daily_hours: float, working_days: int,
              monthly_rent: int, monthly_material: int) -> dict:
    if df.empty:
        return {}
    df = enrich(df)
    sisl  = mask_sisl(df)
    is_dr = mask_dir(df)
    is_fl = mask_fl(df)
    sisl_df = df[sisl]
    dir_df  = df[sisl & is_dr]
    fl_df   = df[sisl & is_fl]

    차감_매출 = int(sisl_df['판매가'].sum()) if '판매가' in df.columns else 0
    결제_매출 = int(df['결제액'].sum())       if '결제액' in df.columns else 0
    dir_cnt   = len(dir_df)
    fl_used_min = int(fl_df['시술_시간'].sum())
    fl_wages  = int(df.loc[sisl & is_fl, '인건비'].sum())
    net_profit = 차감_매출 - monthly_rent - monthly_material - fl_wages
    avg_unit  = int(차감_매출 / len(sisl_df)) if len(sisl_df) > 0 else 0
    unique_cust = df['고객명'].nunique() if '고객명' in df.columns else 0
    # 진짜 방문 횟수 기준 평균 (60분 윈도우 세션 기준)
    _vc_core = true_visit_count(df)
    avg_visits = round(_vc_core.mean(), 2) if len(_vc_core) > 0 else 0
    total_hours = daily_hours * working_days
    rev_per_h   = int(차감_매출 / total_hours) if total_hours > 0 else 0
    avg_min     = round(float(sisl_df['시술_시간'].mean()), 1) if len(sisl_df) > 0 else DEFAULT_DURATION

    # 신규 / 재방 / 손님 매출
    신규_매출 = 재방_매출 = 손님_매출 = 0
    신규_건수 = 재방_건수 = 손님_건수 = 0
    신규_명수 = 재방_명수 = 손님_명수 = 0
    if '방문유형' in df.columns and '결제액' in df.columns:
        _신규 = df[df['방문유형'] == '신규']
        _재방 = df[df['방문유형'] == '재방']
        _손님 = df[df['방문유형'] == '손님']
        신규_매출 = int(_신규['결제액'].sum())
        재방_매출 = int(_재방['결제액'].sum())
        손님_매출 = int(_손님['결제액'].sum())
        신규_건수 = len(_신규)
        재방_건수 = len(_재방)
        손님_건수 = len(_손님)
        if '고객명' in df.columns:
            신규_명수 = _신규['고객명'].nunique()
            재방_명수 = _재방['고객명'].nunique()
            손님_명수 = _손님['고객명'].nunique()

    return {
        '차감_매출':   차감_매출,
        '결제_매출':   결제_매출,
        '미차감':      결제_매출 - 차감_매출,
        '시술_건수':   len(sisl_df),
        '원장_건수':   dir_cnt,
        'fl_used_min': fl_used_min,
        'fl_wages':    fl_wages,
        'net_profit':  net_profit,
        'avg_unit':    avg_unit,
        'avg_visits':  avg_visits,
        'rev_per_h':   rev_per_h,
        'avg_min':     avg_min,
        'unique_cust': unique_cust,
        'dir_util':    util_director(dir_cnt, working_days),
        'fl_util':     util_freelancer(fl_used_min, daily_hours, working_days),
        'df_enriched': df,
        '신규_매출':   신규_매출,
        '재방_매출':   재방_매출,
        '손님_매출':   손님_매출,
        '신규_건수':   신규_건수,
        '재방_건수':   재방_건수,
        '손님_건수':   손님_건수,
        '신규_명수':   신규_명수,
        '재방_명수':   재방_명수,
        '손님_명수':   손님_명수,
    }


def calc_revenue_breakdown(df: pd.DataFrame) -> dict:
    """
    핸드SOS CSV 기준 매출 항목별 분해.

    컬럼 매핑:
      결제액  → 고객이 실제 지불한 금액  → 총영업매출 근거
      판매가  → 서비스 차감/정가 금액    → 차감매출 근거
      구분    → 시술/점판/정액권/충전/환불 등 항목 분류

    세 가지 매출 정의:
      총영업매출 = 결제액 전체 합  (핸드SOS 총영업합계에 해당)
      실매출     = 결제액 중 시술+점판 합  (실제 서비스·상품 제공분)
      차감매출   = 판매가 중 구분='시술' 합  (실제 차감된 매출 — 현재 판단 기준)
    """
    result = {
        '총영업매출': 0,
        '실매출':    0,
        '차감매출':  0,
        '항목별':    {},   # {구분명: 결제액합계}
        '구분목록':  [],
        '사용컬럼':  {},
    }
    if df.empty:
        return result

    pay_col  = '결제액' if '결제액' in df.columns else None
    판가_col = '판매가' if '판매가' in df.columns else None
    구분_col = '구분'   if '구분'   in df.columns else None

    result['사용컬럼'] = {
        '총영업매출 계산': f'`{pay_col}` 컬럼 전체 합' if pay_col else '결제액 컬럼 없음',
        '차감매출 계산':   (f'`{판가_col}` 컬럼, `{구분_col}`=시술 필터 적용'
                            if (판가_col and 구분_col) else
                            f'`{판가_col}` 전체 합 (구분 컬럼 없음)' if 판가_col else '판매가 컬럼 없음'),
        '실매출 계산':     (f'`{pay_col}` 컬럼, `{구분_col}` IN (시술, 점판) 필터'
                            if (pay_col and 구분_col) else '구분 컬럼 없어 총영업매출과 동일'),
    }

    if pay_col:
        result['총영업매출'] = int(df[pay_col].sum())

    if 판가_col:
        if 구분_col:
            sisl_mask = df[구분_col].astype(str).str.contains('시술', na=False)
            result['차감매출'] = int(df.loc[sisl_mask, 판가_col].sum())
        else:
            result['차감매출'] = int(df[판가_col].sum())

    if 구분_col and pay_col:
        unique_구분 = sorted(df[구분_col].astype(str).dropna().unique().tolist())
        result['구분목록'] = unique_구분
        for 구분_val in unique_구분:
            mask = df[구분_col].astype(str) == 구분_val
            result['항목별'][구분_val] = int(df.loc[mask, pay_col].sum())
        # 실매출 = 시술 + 점판 결제액 (정액권/충전 등 선결제 제외)
        for k, v in result['항목별'].items():
            if '시술' in k or '점판' in k:
                result['실매출'] += v
    else:
        # 구분 구분 불가 → 총영업매출로 대체
        result['실매출'] = result['총영업매출']

    return result


def monthly_summary(df: pd.DataFrame, daily_hours: float, working_days: int) -> pd.DataFrame:
    if df.empty or '날짜' not in df.columns:
        return pd.DataFrame()
    df = enrich(df).copy()
    df['연월'] = df['날짜'].dt.to_period('M')
    rows = []
    for period, grp in df.groupby('연월'):
        sisl = mask_sisl(grp)
        g    = grp[sisl]
        dr   = g[mask_dir(g)]
        fl   = g[mask_fl(g)]
        차감 = int(g['판매가'].sum()) if '판매가' in g.columns else 0
        fl_wages = int(g.loc[mask_fl(g), '인건비'].sum()) if '인건비' in g.columns else 0
        신규_건수 = 재방_건수 = 0
        if '방문유형' in grp.columns:
            신규_건수 = int((grp['방문유형'] == '신규').sum())
            재방_건수 = int((grp['방문유형'] == '재방').sum())
        _총 = 신규_건수 + 재방_건수
        rows.append({
            '연월':            str(period),
            '차감_매출':       차감,
            '시술_건수':       len(g),
            '원장_건수':       len(dr),
            'fl_건수':         len(fl),
            'fl_사용분':       int(fl['시술_시간'].sum()) if '시술_시간' in fl.columns else 0,
            'fl_인건비':       fl_wages,
            '원장_가동률':     util_director(len(dr), working_days),
            '프리랜서_가동률': util_freelancer(
                int(fl['시술_시간'].sum()) if '시술_시간' in fl.columns else 0,
                daily_hours, working_days
            ),
            '신규_건수':       신규_건수,
            '재방_건수':       재방_건수,
            '신규_비율':       round(신규_건수 / _총 * 100, 1) if _총 > 0 else 0,
            '재방_비율':       round(재방_건수 / _총 * 100, 1) if _총 > 0 else 0,
        })
    return pd.DataFrame(rows).sort_values('연월').reset_index(drop=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CRM 분석
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_dormant(df: pd.DataFrame, months: int = 3, top_n: int = 10) -> pd.DataFrame:
    """매출 DB 기반 미방문 고객 추출 (작살낚시용)"""
    if df.empty or '고객명' not in df.columns or '날짜' not in df.columns:
        return pd.DataFrame()
    cutoff = pd.Timestamp.now() - pd.DateOffset(months=months)
    agg: dict = {'최근_방문': ('날짜', 'max')}
    if '결제액' in df.columns:
        agg['총_결제액'] = ('결제액', 'sum')
    if '메뉴' in df.columns:
        agg['주요_메뉴'] = ('메뉴', lambda x: x.mode()[0] if len(x) > 0 else '-')
    g = df.groupby('고객명').agg(**agg).reset_index()
    # 진짜 방문 횟수 (60분 윈도우 기준)
    vc = true_visit_count(df)
    g['총_방문'] = g['고객명'].map(vc).fillna(0).astype(int)
    d = g[g['최근_방문'] < cutoff].copy()
    if d.empty:
        return d
    d['미방문_일수'] = (pd.Timestamp.now() - d['최근_방문']).dt.days
    d = d.sort_values('총_결제액' if '총_결제액' in d.columns else '미방문_일수',
                      ascending=False).head(top_n)
    d['최근_방문'] = d['최근_방문'].dt.strftime('%Y-%m-%d')
    return d.reset_index(drop=True)


def make_message(row: pd.Series) -> str:
    name   = str(row.get('고객명', row.get('이름', '고객')))
    days   = int(row.get('미방문_일수', 90))
    menu   = str(row.get('주요_메뉴', '아로마 마사지'))
    total  = int(row.get('총_결제액', row.get('영업금액', 0)))
    visits = int(row.get('총_방문', row.get('방문횟수', 1)))
    months_away = days // 30

    now = datetime.now()
    season_copy = {
        (3,4,5):   "따뜻한 봄바람과 함께 몸의 피로를 풀어드리고 싶어요.",
        (6,7,8):   "무더운 여름, 시원한 스파에서 재충전하세요.",
        (9,10,11): "쌀쌀해지는 가을, 따뜻한 핫스톤 케어로 녹여드릴게요.",
        (12,1,2):  "추운 겨울, 따뜻한 스파에서 온몸의 긴장을 풀어드려요.",
    }
    s_copy = next((v for k, v in season_copy.items() if now.month in k), "")

    if total >= 1_000_000 or visits >= 10:
        tier, disc = "VIP", "15%"
        opening = f"저희 스파를 {visits}번이나 사랑해 주신 {name}님께 특별히 먼저 연락드려요."
    elif total >= 500_000 or visits >= 5:
        tier, disc = "단골", "10%"
        opening = f"항상 좋은 에너지 주시는 {name}님, 잘 지내고 계신가요?"
    else:
        tier, disc = "일반", "10%"
        opening = f"안녕하세요 {name}님! 방문해 주셔서 감사했습니다."

    menu_line = f"지난번 [{menu}] 이후 몸 상태는 어떠세요?" if menu not in ('-', 'nan') else ""
    return f"""안녕하세요 {name}님 😊
{opening}

벌써 {months_away}개월이 지났네요... {s_copy}
{menu_line}

이번 달 {name}님을 위한 {tier} 특별 혜택을 준비했어요:
  ✨ 재방문 시 아로마 90분 코스 → {disc} 특별 할인
  🎁 방문 당일 미니 홈케어 키트 증정

예약은 카카오톡 또는 전화로 편하게 연락 주세요!
항상 최고의 케어로 맞이할게요 🌿""".strip()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 15일 중간 점검
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def midpoint_forecast(df: pd.DataFrame, sel_year: int, sel_month: int,
                      target_rev: int, daily_hours: float, working_days: int) -> dict:
    today = datetime.now()
    days_in_month = monthrange(sel_year, sel_month)[1]
    elapsed = min(
        today.day if (today.year == sel_year and today.month == sel_month)
        else days_in_month, days_in_month
    )
    remain = days_in_month - elapsed
    if df.empty or elapsed == 0:
        return {}

    df = enrich(df)
    sisl = mask_sisl(df)
    curr_rev  = int(df.loc[sisl, '판매가'].sum()) if '판매가' in df.columns else 0
    curr_cnt  = int(sisl.sum())
    daily_rev = curr_rev / elapsed
    daily_cnt = curr_cnt / elapsed
    proj_rev  = int(curr_rev + daily_rev * remain)
    proj_cnt  = int(curr_cnt + daily_cnt * remain)

    dir_cnt_now = int((mask_dir(df) & sisl).sum())
    fl_min_now  = int(df.loc[mask_fl(df) & sisl, '시술_시간'].sum())
    proj_dir_cnt = int(dir_cnt_now + (dir_cnt_now / elapsed) * remain)
    proj_fl_min  = int(fl_min_now  + (fl_min_now  / elapsed) * remain)

    return {
        'elapsed': elapsed, 'remain': remain, 'days_in_month': days_in_month,
        'curr_rev': curr_rev, 'proj_rev': proj_rev,
        'rev_pct': round(proj_rev / target_rev * 100, 1) if target_rev else 0,
        'curr_cnt': curr_cnt, 'proj_cnt': proj_cnt,
        'daily_rev': int(daily_rev),
        'proj_dir_util': util_director(proj_dir_cnt, working_days),
        'proj_fl_util':  util_freelancer(proj_fl_min, daily_hours, working_days),
        'on_track': proj_rev >= target_rev,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 렌더링 헬퍼
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def card(label, value, sub='', color='#667eea', delta=None) -> str:
    d = ''
    if delta is not None:
        arr = '▲' if delta > 0 else ('▼' if delta < 0 else '─')
        cls = 'up' if delta > 0 else ('down' if delta < 0 else '')
        d = f'<span class="{cls}">{arr} {abs(delta):.1f}%</span>'
    mom_div = f'<div style="font-size:.78rem;margin-top:3px">{d} MoM</div>' if d else ''
    return f"""<div class="card" style="border-left-color:{color}">
  <div class="card-lbl">{label}</div>
  <div class="card-val">{value}</div>
  {mom_div}
  <div class="card-sub">{sub}</div>
</div>"""


def gauge(pct: float, color: str = '#667eea') -> str:
    w = min(100, max(0, pct))
    return f'<div class="gb"><div class="gf" style="width:{w}%;background:{color}"></div></div>'


def mom_pct(curr, prev) -> float | None:
    return None if not prev else (curr - prev) / abs(prev) * 100


def no_data():
    st.markdown("""<div style="text-align:center;padding:5rem;color:#bbb">
      <div style="font-size:3.5rem">🌿</div>
      <h3 style="color:#999">사이드바에서 CSV 파일을 업로드해주세요</h3>
    </div>""", unsafe_allow_html=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 액션 플랜 엔진
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def build_action_stats(df: pd.DataFrame, c: dict, target_revenue: int,
                       util_data: dict | None = None) -> dict:
    """액션 플랜 판단에 필요한 통계를 한 번에 집계."""
    ud = util_data or {}
    s = {
        'total_cust': 0, 'one_time_n': 0, 'early_n': 0, 'repeat_n': 0, 'vip_n': 0,
        'one_time_pct': 0, 'vip_pct': 0, 'revisit_rate': 0,
        'dormant_30': 0, 'dormant_60': 0, 'dormant_90': 0,
        # 베드 구조 기반 가동률 우선, 없으면 기존 calc_core 값
        'dir_util': ud.get('dir_util', c.get('dir_util', 0)),
        'fl_util':  ud.get('fl_util',  c.get('fl_util',  0)),
        'total_util': ud.get('total_util', 0),
        'net_profit': c.get('net_profit', 0), 'avg_unit': c.get('avg_unit', 0),
        'revenue': c.get('차감_매출', 0), 'target_revenue': target_revenue,
        'dir_cnt': c.get('원장_건수', 0), 'total_cnt': c.get('시술_건수', 0),
        'fl_wages': c.get('fl_wages', 0),
        'max_revenue': ud.get('max_revenue', 0),
        'rev_potential': ud.get('rev_potential', 0),
        'avg_duration': ud.get('avg_duration', 90),
        # 운영 기준 필드
        'payment':  c.get('결제_매출', 0),
        'undocked': c.get('미차감', 0),
    }
    # ── 운영 기준 자동 판단 ────────────────────────────────────────────────────
    s['store_stage'], s['store_stage_desc'] = get_store_stage(s['revenue'], s['fl_wages'])
    s['deduction_risk']  = get_deduction_risk(s['payment'], s['revenue'])
    s['revenue_decompose'] = decompose_revenue_issue(
        s['revenue'], target_revenue, s['total_cnt'], s['avg_unit'])
    # 인건비 비율 경고
    s['labor_ratio'] = s['fl_wages'] / s['revenue'] if s['revenue'] > 0 else 0
    s['labor_over']  = s['labor_ratio'] > COST_BENCH['인건비']

    if df.empty or '고객명' not in df.columns or '날짜' not in df.columns:
        return s

    _vc_agg: dict = {'최근방문': ('날짜', 'max')}
    if '결제액' in df.columns:
        _vc_agg['총결제'] = ('결제액', 'sum')
    else:
        _vc_agg['총결제'] = ('날짜', 'count')
    g = df.groupby('고객명').agg(**_vc_agg).reset_index()
    # 진짜 방문 횟수 (60분 윈도우 기준) 병합
    _vc = true_visit_count(df)
    g['방문수'] = g['고객명'].map(_vc).fillna(0).astype(int)

    total = len(g)
    s['total_cust'] = total
    if total == 0:
        return s

    one  = g[g['방문수'] == 1]
    earl = g[(g['방문수'] >= 2) & (g['방문수'] <= 3)]
    rep  = g[(g['방문수'] >= 4) & (g['방문수'] <= 9)]
    vip  = g[g['방문수'] >= 10]

    s['one_time_n']   = len(one)
    s['early_n']      = len(earl)
    s['repeat_n']     = len(rep)
    s['vip_n']        = len(vip)
    s['one_time_pct'] = len(one) / total * 100
    s['vip_pct']      = len(vip) / total * 100
    s['revisit_rate'] = (total - len(one)) / total * 100

    now = pd.Timestamp.now()
    g['휴면일'] = (now - g['최근방문']).dt.days
    s['dormant_30'] = int(((g['휴면일'] >= 30) & (g['휴면일'] < 60)).sum())
    s['dormant_60'] = int(((g['휴면일'] >= 60) & (g['휴면일'] < 90)).sum())
    s['dormant_90'] = int((g['휴면일'] >= 90).sum())
    if '방문유형' in df.columns and '결제액' in df.columns:
        신규 = df[df['방문유형'] == '신규']
        재방 = df[df['방문유형'] == '재방']
        손님 = df[df['방문유형'] == '손님']
        s['신규_매출'] = int(신규['결제액'].sum())
        s['재방_매출'] = int(재방['결제액'].sum())
        s['손님_매출'] = int(손님['결제액'].sum())
        s['신규_건수'] = len(신규)
        s['재방_건수'] = len(재방)
    return s


# ── 운영 기준 헬퍼 함수 (Operating Framework) ────────────────────────────────
def _is_peak_season(month: int) -> bool:
    """성수기: 4~10월 / 비수기: 11~3월"""
    return 4 <= month <= 10


def get_store_stage(revenue: float, fl_wages: float = 0) -> tuple[str, str]:
    """
    매장 단계 판단 — 차감매출 + 인력 구조 기반
    프리랜서는 고정 인력이 아니므로 단계 상향 기준에서 제외.
    프리랜서 인건비가 있으면 1.5단계(원장 중심+프리랜서 보조) 적용.
    """
    # 프리랜서 투입이 있으면 → 1.5단계 고정
    # (2단계 이상은 고정 직원이 있을 때만 적용)
    if fl_wages > 0:
        return STAGE_1_5

    # 프리랜서 없음 → 차감매출 기준 단계 판단
    for threshold, stage, desc in STAGE_THRESHOLDS:
        if revenue < threshold:
            return stage, desc
    return "4단계", "시스템 운영 — 구조가 스스로 돌아가는 단계"


def get_deduction_risk(payment: float, deduction: float) -> dict:
    """결제 vs 차감 비율 분석 — 미차감 누적 위험 감지"""
    if payment <= 0:
        return {'risk': False, 'ratio': 1.0, 'gap': 0,
                'label': '데이터 없음', 'color': '#999',
                'msg': '결제 데이터를 확인할 수 없습니다.'}
    ratio = deduction / payment
    gap   = payment - deduction
    if ratio >= 0.85:
        label, color = "건강", "#22c55e"
        grade_msg = "결제된 매출이 대부분 실제 차감으로 이어지고 있어 안정적인 구조입니다."
    elif ratio >= 0.70:
        label, color = "주의", "#f39c12"
        grade_msg = "결제와 차감 사이에 간격이 있어 관리가 필요합니다. 미차감 고객을 점검해야 합니다."
    else:
        label, color = "위험", "#ef4444"
        grade_msg = "결제 대비 실제 차감이 낮습니다. 미차감이 쌓이면 나중에 공짜로 일하는 구조가 됩니다."
    msg = (
        f"결제 ₩{payment:,} → 차감 ₩{deduction:,} (차감률 {ratio*100:.1f}%)"
        + (f" — 미차감 ₩{gap:,}이 쌓여 있다. 이건 매출이 아니라 아직 갚아야 할 약속이다."
           if ratio < 0.85 else " — 차감률 양호.")
    )
    return {'risk': ratio < 0.85, 'ratio': ratio, 'gap': gap,
            'label': label, 'color': color, 'msg': msg,
            'grade_msg': grade_msg}


def decompose_revenue_issue(revenue: float, target: float,
                             total_cnt: int, avg_unit: float) -> dict | None:
    """
    매출 공식 분해: 차감매출 = 관리횟수 × 객단가
    목표 미달 시 관리횟수 문제인지 객단가 문제인지 판단
    """
    if target <= 0 or revenue >= target * 0.95:
        return None
    gap         = target - revenue
    needed_cnt  = int(target / avg_unit)  if avg_unit   > 0 else 0
    cnt_gap     = max(0, needed_cnt  - total_cnt)
    needed_unit = int(target / total_cnt) if total_cnt  > 0 else 0
    unit_gap    = max(0, needed_unit - avg_unit)
    cnt_pct     = cnt_gap  / max(total_cnt, 1) * 100
    unit_pct    = unit_gap / max(avg_unit,  1) * 100
    primary     = "관리횟수" if cnt_pct <= unit_pct else "객단가"
    return {
        'gap': gap, 'primary': primary,
        'current_cnt':  total_cnt,  'needed_cnt':  needed_cnt,  'cnt_gap':  cnt_gap,  'cnt_pct':  cnt_pct,
        'current_unit': avg_unit,   'needed_unit': needed_unit, 'unit_gap': unit_gap, 'unit_pct': unit_pct,
    }


def generate_action_diagnosis(s: dict) -> str:
    """현재 상태 총진단 — 3~5문장, 성수기/비수기 맥락 포함"""
    month      = s.get('sel_month', pd.Timestamp.now().month)
    peak       = _is_peak_season(month)
    season     = "성수기" if peak else "비수기"
    net        = s['net_profit']
    dir_util   = s['dir_util']
    repeat_n   = s['repeat_n']
    vip_n      = s['vip_n']
    dorm90     = s['dormant_90']
    rev        = s['revenue']
    target     = s['target_revenue']
    total      = s['total_cust']

    lines = []

    if peak:
        lines.append(f"현재 {month}월은 성수기({season})로, 관광객 유입이 활발한 시기다.")
    else:
        lines.append(f"현재 {month}월은 비수기({season})로, 도민 반복 고객이 매출의 중심이 되어야 하는 시기다.")

    if dir_util >= 70:
        lines.append(f"원장 가동률이 {dir_util:.0f}%로 안정적이며, 시술 수요가 잘 채워지고 있다.")
    elif dir_util >= 50:
        lines.append(f"원장 가동률이 {dir_util:.0f}%로 개선 여지가 있다. 예약 빈 시간대를 도민 고정 고객으로 채우는 구조가 필요하다.")
    else:
        lines.append(f"원장 가동률이 {dir_util:.0f}%로 낮다. 도민 고정 고객 기반이 아직 약하거나 재방문 예약 구조가 작동하지 않는 상태다.")

    dom_n = repeat_n + vip_n
    if dom_n >= 10:
        lines.append(f"반복 고객(4회 이상) {dom_n}명이 안정적인 도민 기반을 형성하고 있다.")
    elif dom_n >= 5:
        lines.append(f"반복 고객(4회 이상) {dom_n}명이 있지만, 도민 기반을 더 두텁게 만들어야 성수기·비수기 편차를 줄일 수 있다.")
    else:
        lines.append(f"반복 고객(4회 이상)이 {dom_n}명에 불과하다. 도민 고정층이 얇아 비수기 매출이 불안정해질 수 있다.")

    if net < 0:
        lines.append(f"현재 순이익이 적자({net:,}원)로, 비용 구조를 먼저 점검해야 한다.")
    elif target > 0 and rev >= target:
        lines.append(f"이번 달 매출이 목표({target:,}원)를 달성했다.")
    elif target > 0 and rev >= target * 0.8:
        lines.append(f"이번 달 매출이 목표의 {rev/target*100:.0f}% 수준으로 근접해 있다.")
    elif target > 0:
        lines.append(f"이번 달 매출이 목표의 {rev/target*100:.0f}% 수준으로, 도민 재방문 구조 강화가 우선 과제다.")

    if not peak and dorm90 >= 5:
        lines.append(f"비수기인 만큼 90일+ 휴면 고객 {dorm90}명을 지금 복귀시키는 것이 이 시기 가장 효과적인 매출 전략이다.")

    return " ".join(lines)


def generate_customer_structure(s: dict) -> dict:
    """
    고객 구조 분석 — 방문 횟수 기반 도민/관광객 추정
    4회+ = 도민 고정 추정 / 1~3회 = 관광객 or 도민 초기 혼재
    목표 비율: 도민 60 : 관광객 40
    """
    total    = s['total_cust']
    one_n    = s['one_time_n']
    early_n  = s['early_n']
    repeat_n = s['repeat_n']
    vip_n    = s['vip_n']
    month    = s.get('sel_month', pd.Timestamp.now().month)
    peak     = _is_peak_season(month)

    if total == 0:
        return {'summary': '고객 데이터 없음', 'items': [], 'dom_pct': 0, 'tour_pct': 0,
                'grade': '-', 'color': '#999', 'dom_est': 0, 'tour_est': 0}

    dom_est  = repeat_n + vip_n
    tour_est = one_n + early_n
    dom_pct  = dom_est / total * 100
    tour_pct = tour_est / total * 100

    if dom_pct >= 55:
        grade, color = "양호", "#22c55e"
        msg = f"도민 고정 고객 비중({dom_pct:.0f}%)이 안정적으로 목표(60%)에 근접해 있다."
    elif dom_pct >= 35:
        grade, color = "주의", "#f39c12"
        msg = f"도민 고정 고객 비중({dom_pct:.0f}%)이 목표(60%)에 다소 못 미친다. 반복 방문 구조 강화가 필요하다."
    else:
        grade, color = "위험", "#ef4444"
        if peak:
            msg = (f"성수기라 관광객 비중이 높아 보이지만({tour_pct:.0f}%), "
                   f"비수기 대비를 위해 도민 기반({dom_pct:.0f}%)을 지금부터 쌓아야 한다.")
        else:
            msg = (f"비수기에 도민 고정 고객 비중({dom_pct:.0f}%)이 낮은 것은 경고 신호다. "
                   f"도민 재방문 구조가 작동하지 않고 있다.")

    return {
        'dom_pct': dom_pct, 'tour_pct': tour_pct,
        'grade': grade, 'color': color, 'summary': msg,
        'dom_est': dom_est, 'tour_est': tour_est,
        'items': [
            {'label': '도민 고정 추정 (4회+)',     'n': dom_est,  'pct': dom_pct},
            {'label': '초기/관광객 혼재 (1~3회)',  'n': tour_est, 'pct': tour_pct},
        ],
    }


def generate_top_issues(s: dict) -> list:
    """
    주요 문제/기회 3개 — 우선순위 고정:
    1순위: 2~3회 고객 단골 전환 (성장 레버)
    2순위: 30~60일 미방문 (관계 끊기기 전)
    3순위: 정액권 차감률 이탈
    후순위: 90일+ 미방문 (복구 대상, 성장 레버 아님)
    """
    p1 = []   # 1순위: 단골 전환 레버
    p2 = []   # 2순위: 30~60일 미방문
    p3 = []   # 3순위: 정액권/차감률
    p4 = []   # 후순위: 가동률·객단가·비용 등
    p5 = []   # 최후순위: 90일+ 미방문

    dir_util = s['dir_util']
    net      = s['net_profit']
    avg_u    = s['avg_unit']
    dorm30   = s['dormant_30']
    dorm60   = s['dormant_60']
    dorm90   = s['dormant_90']
    repeat_n = s['repeat_n']
    vip_n    = s['vip_n']
    early_n  = s['early_n']
    total    = s['total_cust']
    rev      = s['revenue']
    fl_wages = s.get('fl_wages', 0)
    month    = s.get('sel_month', pd.Timestamp.now().month)
    peak     = _is_peak_season(month)
    ded      = s.get('deduction_risk', {})
    ded_ratio = ded.get('ratio', 1.0)
    ded_gap   = ded.get('gap', 0)

    # ── 1순위: 단골 전환 레버 ──────────────────────────────────────────────
    if early_n >= 2:
        p1.append(
            f"🔥 단골 전환 대상 {early_n}명 (2~3회 고객) — "
            f"이미 서비스를 경험하고 다시 온 사람들이다. "
            f"지금 4회 전환을 못 잡으면 관광객처럼 1~3회로 끝난다. "
            f"이게 현재 가장 큰 성장 레버다."
        )

    # ── 2순위: 30~60일 미방문 ──────────────────────────────────────────────
    if dorm30 + dorm60 >= 3:
        p2.append(
            f"⏰ 30~60일 미방문 고객 {dorm30 + dorm60}명 — "
            f"관계가 완전히 끊어지기 전 단계. "
            f"지금 연락하면 복귀율이 가장 높은 구간이다."
        )

    # ── 3순위: 정액권/차감률 ───────────────────────────────────────────────
    if ded_ratio < 0.70:
        p3.append(
            f"💳 차감률 {ded_ratio*100:.1f}% — 위험 구간. "
            f"미차감 ₩{ded_gap:,}이 쌓여 있다. 신규보다 기존 미차감 소진이 먼저다."
        )
    elif ded_ratio < 0.85:
        p3.append(
            f"💳 차감률 {ded_ratio*100:.1f}% — 주의 구간. "
            f"미차감 ₩{ded_gap:,}. 정액권 고객 방문 주기 이탈 중일 수 있다."
        )

    # ── 후순위: 가동률·비용·도민 기반 ────────────────────────────────────
    if dir_util < 55:
        p4.append(f"원장 가동률 {dir_util:.0f}% — 도민 고정 고객 정기 예약으로 빈 슬롯을 채워야 한다.")
    if net < 0:
        p4.append(f"순이익 적자({net:,}원) — 프리랜서 인건비 비중과 원장 시술 비중을 먼저 확인해야 한다.")
    elif avg_u < TARGET_UNIT_PRICE:
        p4.append(f"평균 객단가 ₩{avg_u:,} — 목표(₩{TARGET_UNIT_PRICE:,}) 미달. 제안 방식 점검 필요.")
    if fl_wages > 0 and rev > 0 and fl_wages / rev > 0.3:
        p4.append(f"프리랜서 인건비 매출의 {fl_wages/rev*100:.0f}% — 원장 시술 비중을 높이면 순이익이 올라간다.")
    dom_n = repeat_n + vip_n
    if total > 5 and dom_n / max(total, 1) < 0.3 and not peak:
        p4.append(f"도민 고정 고객(4회+) {dom_n}명({dom_n/total*100:.0f}%) — 비수기 매출 기반이 얇다.")

    # ── 최후순위: 90일+ 미방문 ─────────────────────────────────────────────
    if dorm90 >= 8:
        p5.append(
            f"90일+ 미방문 {dorm90}명 — 복귀 가능한 구간이나 최우선 성장 레버는 아니다. "
            f"단골 전환·30~60일 관리 후 여력이 생기면 접근하라."
        )

    combined = p1 + p2 + p3 + p4 + p5
    return combined[:3] if combined else [
        "현재 데이터 범위 내 특이 이슈 없음 — 2~3회 고객 단골 전환과 방문 주기 유지를 지속 관리하면 된다."
    ]


def generate_operational_interpretation(s: dict) -> list:
    """운영 구조 해석 — 제주 관광지 스파 맥락으로 숫자의 의미를 설명"""
    lines        = []
    month        = s.get('sel_month', pd.Timestamp.now().month)
    peak         = _is_peak_season(month)
    dir_util     = s['dir_util']
    net          = s['net_profit']
    dorm90       = s['dormant_90']
    early_n      = s['early_n']
    fl_wages     = s.get('fl_wages', 0)
    avg_u        = s['avg_unit']
    avg_duration = s.get('avg_duration', 90)
    repeat_n     = s['repeat_n']
    vip_n        = s['vip_n']
    rev          = s['revenue']

    dom_n = repeat_n + vip_n
    if peak:
        lines.append(
            f"성수기({month}월)엔 관광객이 많아 1회성 고객 비중이 높아 보이는 것은 정상이다. "
            f"이 시기에 중요한 건 관광객으로 가동률을 채우면서, "
            f"도민 고정 고객(4회+, 현재 {dom_n}명)의 예약 루틴이 흔들리지 않는지 확인하는 것이다."
        )
    else:
        if dom_n >= 10:
            lines.append(
                f"비수기({month}월)에 도민 반복 고객(4회+) {dom_n}명이 가동률을 지지하고 있다. "
                f"이 고객들의 예약 주기가 유지되는 한 비수기 매출은 방어 가능하다."
            )
        else:
            lines.append(
                f"비수기({month}월)에 도민 고정 고객(4회+)이 {dom_n}명으로 적다. "
                f"관광객 유입이 줄어드는 이 시기에 도민 기반이 약하면 매출이 급격히 떨어진다. "
                f"지금 당장 도민 재방문 구조를 만드는 게 최우선이다."
            )

    if avg_duration >= 110:
        lines.append(
            f"평균 시술시간이 {avg_duration}분으로 길다. "
            f"베드당 하루 최대 회전 수가 줄기 때문에 객단가를 높은 수준으로 유지해야 매출 구조가 성립한다. "
            f"고가 코스 비중이 낮다면 메뉴 구성을 재검토해야 한다."
        )
    elif avg_duration <= 65:
        lines.append(
            f"평균 시술시간이 {avg_duration}분으로 짧다. "
            f"회전율은 높지만 객단가(₩{avg_u:,})가 낮을 가능성이 있다. "
            f"60분 고객에게 90분 코스를 권유하는 것만으로도 월 매출 구조가 달라질 수 있다."
        )

    if early_n >= 5:
        lines.append(
            f"2~3회 초기 고객 {early_n}명이 4회 고정으로 넘어가는 게 지금 가장 중요한 구조 과제다. "
            f"이 고객들은 이미 서비스를 경험하고 반응한 사람들이다 — "
            f"지금 잡지 않으면 관광객처럼 1~3회로 끝나고 이탈한다."
        )

    if dorm90 >= 5:
        lines.append(
            f"90일+ 미방문 {dorm90}명은 '이탈'이 아니라 '연락이 끊긴 것'이다. "
            f"도민 고객이 대부분이라면 메시지 하나로 절반 이상 복귀시킬 수 있다. "
            f"새 고객을 찾는 것보다 이 고객들을 먼저 살리는 게 훨씬 빠르고 싸다."
        )

    if dir_util < 60:
        lines.append(
            f"원장 가동률 {dir_util:.0f}%의 공백은 '수요 없음'이 아니라 "
            f"'도민 고정 고객의 정기 예약이 그 시간대를 채우지 못하는 구조'에서 온다. "
            f"어느 요일·시간대가 비는지 확인하고, 그 슬롯에 고정 예약 고객을 배치하는 루틴을 만들어야 한다."
        )

    if net < 0:
        lines.append(
            "순이익 적자 상태에서 광고나 프리랜서 확대는 구멍을 키운다. "
            "원장 시술 비중과 단위당 수익성을 먼저 점검해야 한다."
        )
    elif fl_wages > 0 and rev > 0 and fl_wages / rev > 0.3:
        lines.append(
            f"프리랜서 인건비가 매출의 {fl_wages/rev*100:.0f}%다. "
            f"원장이 직접 받는 비중이 높아질수록 같은 매출에서 순이익이 올라간다 — "
            f"원장 가동률 개선이 수익성 개선과 직결된다."
        )

    # 차감률 해석 — 총매출 착시 경고
    ded = s.get('deduction_risk', {})
    ded_ratio = ded.get('ratio', 1.0)
    ded_gap   = ded.get('gap', 0)
    payment   = s.get('payment', 0)
    if ded_ratio < 0.70:
        lines.append(
            f"차감률이 {ded_ratio*100:.1f}%로 위험 구간이다. "
            f"총매출(₩{payment:,})은 들어온 돈이지만, 실제 일한 양은 차감매출(₩{rev:,})이다. "
            f"미차감 ₩{ded_gap:,}은 자산이 아니라 아직 갚아야 할 약속 — "
            f"지금 신규를 늘리기 전에 미차감 소진을 먼저 관리해야 한다."
        )
    elif ded_ratio < 0.85:
        lines.append(
            f"차감률 {ded_ratio*100:.1f}% — 주의 구간이다. "
            f"미차감 ₩{ded_gap:,}이 쌓여 있다. "
            f"정액권 고객의 방문 주기가 이탈 중이거나, 선결제 후 미이용 고객이 늘고 있다는 신호다. "
            f"지금은 결제보다 차감 관리가 중요하다."
        )

    lines.append(
        "제주 스파의 구조는 '관광객으로 채우고, 도민으로 쌓는' 이중 구조다. "
        "관광객은 가동률을 채우지만 쌓이지 않는다. 쌓이는 건 도민이다 — "
        "도민 반복 고객이 두꺼워질수록 비수기가 무서워지지 않는다."
    )

    return lines[:6]


def generate_weekly_action_plan(s: dict) -> list:
    """이번 주 액션 플랜 — 무엇/왜/대상/기대효과 4항목"""
    # 우선순위별 플랜 버킷
    p1, p2, p3, p4, p5 = [], [], [], [], []

    dorm30   = s['dormant_30']
    dorm60   = s['dormant_60']
    dorm90   = s['dormant_90']
    early_n  = s['early_n']
    dir_util = s['dir_util']
    repeat_n = s['repeat_n']
    vip_n    = s['vip_n']
    month    = s.get('sel_month', pd.Timestamp.now().month)
    peak     = _is_peak_season(month)
    ded      = s.get('deduction_risk', {})
    ded_ratio = ded.get('ratio', 1.0)
    ded_gap   = ded.get('gap', 0)

    # ── 1순위: 2~3회 → 4회 단골 전환 (성장 레버) ──────────────────────────
    if early_n >= 2:
        p1.append({
            '무엇':   f'🔥 단골 전환 집중 — 2~3회 고객 {early_n}명에게 4회 예약 또는 패키지 제안',
            '왜':     '이미 서비스를 경험하고 다시 온 사람들이다. 4회를 넘기면 도민 고정 고객으로 전환된다. '
                      '지금이 결정적 시점이며, 이게 가장 빠른 성장 레버다.',
            '대상':   f'{early_n}명 전수 — 최근 방문 후 미예약자를 오늘 바로 연락',
            '기대효과': '도민 고정 고객 +α명 확보, 비수기 가동률 방어선 직접 강화',
        })

    # ── 2순위: 30~60일 미방문 (관계 끊기기 전) ───────────────────────────
    if dorm30 + dorm60 >= 2:
        p2.append({
            '무엇':   f'⏰ 30~60일 미방문 고객 {dorm30 + dorm60}명 — 이번 주 안에 개인 연락',
            '왜':     '관계가 완전히 끊어지기 전 단계다. 이 구간의 복귀율이 가장 높다. '
                      '90일을 넘기면 회수율이 급격히 떨어진다.',
            '대상':   f'{dorm30 + dorm60}명 — 오늘 리스트 뽑아 하루 3명씩',
            '기대효과': '30~40% 복귀 시 즉각적인 매출 회복, 단골 전환으로 이어질 수 있음',
        })

    # ── 3순위: 정액권/차감률 주기 이탈 ──────────────────────────────────
    if ded_ratio < 0.70:
        p3.append({
            '무엇':   '💳 미차감 고객 긴급 점검 및 방문 유도',
            '왜':     f'차감률 {ded_ratio*100:.1f}% — 위험 구간. 미차감 ₩{ded_gap:,}이 쌓여 있다. '
                      f'신규 결제보다 기존 미차감 소진이 먼저다.',
            '대상':   '잔여 횟수·선결제 후 미방문 고객 전수 — 오늘 리스트 추출',
            '기대효과': '미차감 소진 → 실차감매출 증가 → 운영 부담 감소',
        })
    elif ded_ratio < 0.85:
        p3.append({
            '무엇':   '💳 정액권·잔여 고객 방문 주기 이탈자 연락',
            '왜':     f'차감률 {ded_ratio*100:.1f}% — 주의 구간. 미차감 ₩{ded_gap:,}. '
                      f'정기 고객이 슬그머니 이탈 중일 수 있다.',
            '대상':   '잔여 횟수 보유 고객 중 최근 30일+ 미방문자 우선',
            '기대효과': '차감률 회복 → 총매출 착시 해소, 실매출 구조 안정화',
        })

    # ── 4순위: 원장 가동률 루틴 ──────────────────────────────────────────
    if dir_util < 65:
        p4.append({
            '무엇':   '원장 시술 종료 시 다음 방문 예약을 당일 확정하는 루틴 적용',
            '왜':     f'원장 가동률 {dir_util:.0f}% — 빈 슬롯을 채우는 가장 빠른 방법은 당일 재예약이다.',
            '대상':   '원장 담당 고객 전체, 오늘부터 매 시술 마칠 때마다 적용',
            '기대효과': '예약 공백 감소, 원장 가동률 5~10%p 개선 가능',
        })

    dom_n = repeat_n + vip_n
    if not peak and dom_n < 10:
        p4.append({
            '무엇':   '반복 고객(4회+) 이달 예약 현황 확인',
            '왜':     f'비수기에 도민 고정 고객 {dom_n}명의 예약 공백 없이 유지되는지 점검 필요.',
            '대상':   f'반복 고객 {dom_n}명 전수 — 이달 내 예약 없는 고객 우선',
            '기대효과': '비수기 가동률 최소 방어선 확보',
        })

    # ── 5순위: 90일+ 미방문 (복구 대상, 최우선 아님) ────────────────────
    if dorm90 >= 5:
        p5.append({
            '무엇':   f'90일+ 미방문 고객 {dorm90}명 중 고액 결제자 위주로 연락 (여력 시)',
            '왜':     '복귀 가능한 구간이나 최우선 성장 레버는 아니다. '
                      '단골 전환·단기 미방문 관리 후 여력이 생기면 접근하라.',
            '대상':   f'{dorm90}명 중 총결제액 상위 {max(dorm90//3, 2)}명부터',
            '기대효과': '일부 고가 고객 복귀 가능, 기대치 낮게 접근',
        })

    combined = p1 + p2 + p3 + p4 + p5
    return combined[:3]


def generate_monthly_action_plan(s: dict) -> list:
    """
    이번 달 구조 개선 과제 — 우선순위:
    1순위: 2~3회 → 4회 단골 전환 시스템
    2순위: 정액권/차감률 관리
    3순위: 원장 가동률 구조
    후순위: 90일+ 휴면 복귀 캠페인 (복구 대상, 성장 레버 아님)
    """
    p1, p2, p3, p4, p5 = [], [], [], [], []

    dir_util = s['dir_util']
    avg_u    = s['avg_unit']
    dorm90   = s['dormant_90']
    fl_wages = s.get('fl_wages', 0)
    rev      = s['revenue']
    repeat_n = s['repeat_n']
    vip_n    = s['vip_n']
    early_n  = s['early_n']
    total    = s['total_cust']
    month    = s.get('sel_month', pd.Timestamp.now().month)
    peak     = _is_peak_season(month)
    ded      = s.get('deduction_risk', {})
    ded_ratio = ded.get('ratio', 1.0)
    ded_gap   = ded.get('gap', 0)

    # ── 1순위: 2~3회 → 4회 단골 전환 시스템 구축 ─────────────────────────
    if early_n >= 2:
        p1.append({
            '제목': '🔥 2~3회 고객 단골 전환 시스템 구축',
            '이유': f'2~3회 고객 {early_n}명이 4회를 넘기면 도민 고정 고객으로 전환된다. '
                    f'이 전환이 이번 달 가장 중요한 성장 레버다. '
                    f'90일+ 휴면 관리보다 이 구간이 먼저다.',
            '방향': '시술 종료 시 4회 패키지 제안 루틴 / 2~3회 고객 전용 "다음 예약 확정" 스크립트 운영',
        })
    else:
        # early_n이 적어도 도민 기반 강화는 유지
        dom_n = repeat_n + vip_n
        if dom_n / max(total, 1) < 0.4:
            p1.append({
                '제목': '도민 고정 고객 기반 확대',
                '이유': f'반복 고객(4회+)이 전체의 {dom_n/max(total,1)*100:.0f}%로 낮다. '
                        f'도민 기반 없이는 비수기 매출을 방어할 수 없다.',
                '방향': '4회 도달 고객에게 정기 예약 제안 / VIP 카드 도입 검토',
            })

    # ── 2순위: 정액권/차감률 관리 ─────────────────────────────────────────
    if ded_ratio < 0.70:
        p2.append({
            '제목': '💳 미차감 소진 집중 관리 체계 구축',
            '이유': f'차감률 {ded_ratio*100:.1f}% — 위험 구간. ₩{ded_gap:,}이 미소진 상태. '
                    f'신규 결제를 늘려도 부담만 쌓인다.',
            '방향': '잔여 고객 방문 유도 캠페인 + 차감 속도 모니터링 루틴 수립',
        })
    elif ded_ratio < 0.85:
        p2.append({
            '제목': '💳 차감률 85% 회복 — 정액권 고객 주기 관리',
            '이유': f'차감률 {ded_ratio*100:.1f}% — 주의 구간. '
                    f'미차감 ₩{ded_gap:,}이 미래 노동으로 쌓이고 있다.',
            '방향': '정기 고객 방문 주기 이탈 탐지 + 잔여 횟수 보유 고객 선제 연락',
        })

    # ── 3순위: 원장 가동률 구조 ──────────────────────────────────────────
    if dir_util < 65:
        p3.append({
            '제목': '원장 예약 공백 시간대 분석 및 도민 배치',
            '이유': f'원장 가동률 {dir_util:.0f}% — 빈 시간대를 파악하고 도민 고정 고객을 그 슬롯에 배치하는 구조를 만들어야 한다.',
            '방향': '요일·시간대별 가동률 확인 → 낮은 슬롯에 고정 고객 정기 예약 우선 배치',
        })

    if peak:
        p3.append({
            '제목': '성수기 관광객 → 도민 전환 가능 고객 식별',
            '이유': '성수기 관광객 중 제주 거주자가 있다면 도민 고객으로 연결될 수 있다.',
            '방향': '첫 방문 시 거주지 확인 → 제주 거주 고객에게 정기 패키지 별도 안내',
        })

    if avg_u < TARGET_UNIT_PRICE:
        p3.append({
            '제목': '객단가 구조 개선 (업셀링 루틴 도입)',
            '이유': f'평균 객단가 ₩{avg_u:,}가 목표(₩{TARGET_UNIT_PRICE:,})에 못 미친다.',
            '방향': '60분 예약 고객에게 90분 코스 권유 / 고가 패키지 메뉴판 상단 노출',
        })

    if fl_wages > 0 and rev > 0 and fl_wages / rev > 0.25:
        p3.append({
            '제목': '원장 시술 비중 확대로 인건비 구조 개선',
            '이유': f'프리랜서 인건비 비중 {fl_wages/rev*100:.0f}% — 원장 시술을 늘리면 순이익이 올라간다.',
            '방향': '원장 선호 고객군 분리 관리 + 원장 시간대에 객단가 높은 고객 우선 배치',
        })

    # ── 후순위: 90일+ 휴면 복귀 (복구 대상) ─────────────────────────────
    if dorm90 >= 5:
        p5.append({
            '제목': '90일+ 휴면 고객 복귀 캠페인 (여력 시 실행)',
            '이유': f'90일+ 미방문 {dorm90}명은 복귀 가능한 구간이나 최우선 성장 레버는 아니다. '
                    f'단골 전환과 30~60일 미방문 관리 후 여력이 생기면 접근하라.',
            '방향': '마지막 시술 메뉴 기반 맞춤 메시지 + 재방문 시 소정의 혜택 제공',
        })

    combined = p1 + p2 + p3 + p5
    return combined[:3]


def generate_donts(s: dict) -> list:
    """하지 말아야 할 것 — 제주 스파 맥락, 1회성 문제 프레임 없음"""
    donts    = []
    dir_util = s['dir_util']
    net      = s['net_profit']
    rev      = s['revenue']
    target   = s['target_revenue']
    avg_u    = s['avg_unit']
    one_pct  = s['one_time_pct']
    month    = s.get('sel_month', pd.Timestamp.now().month)
    peak     = _is_peak_season(month)

    if one_pct >= 40 and not peak:
        donts.append(
            "1회 방문 비중이 높다고 신규 유입을 줄이거나 할인 쿠폰을 남발하지 마라 — "
            "관광객 1회성은 제주 스파의 정상 구조다. 문제는 도민 반복 고객이 그만큼 쌓이고 있는지다."
        )

    if not peak and target > 0 and rev < target * 0.8:
        donts.append(
            "비수기에 광고비부터 늘리지 마라 — 관광객이 줄어드는 비수기에 광고 효율은 최저다. "
            "지금 있는 도민 고객과 휴면 고객을 먼저 살려야 한다."
        )

    if dir_util < 55:
        donts.append(
            f"원장 가동률이 {dir_util:.0f}%인 상태에서 프리랜서를 먼저 늘리지 마라 — "
            "원장 시간도 못 채우는데 인건비를 더 쓰면 적자 폭만 커진다."
        )

    if net < 0:
        donts.append(
            "순이익 적자 상태에서 유입 마케팅부터 하지 마라 — "
            "들어오는 고객도 못 남기는 구조에서 광고는 밑 빠진 독이다. 비용 구조를 먼저 봐야 한다."
        )

    donts.append(
        "'도민 고객이 알아서 다시 오겠지'라고 기다리지 마라 — "
        "재방문은 기다리는 게 아니라 설계하는 것이다. 예약 루틴과 연락 구조가 없으면 도민도 끊긴다."
    )

    if avg_u < TARGET_UNIT_PRICE:
        donts.append(
            "객단가가 낮다고 메뉴 가격을 갑자기 올리지 마라 — "
            "제안 방식과 코스 구성을 바꾸는 게 먼저다. 가격 인상은 마지막 수단이다."
        )

    return donts[:3]


def generate_final_summary(s: dict) -> str:
    """최종 요약 — 2~3문장, 제주 스파 맥락"""
    month    = s.get('sel_month', pd.Timestamp.now().month)
    peak     = _is_peak_season(month)
    net      = s['net_profit']
    dir_util = s['dir_util']
    dorm90   = s['dormant_90']
    repeat_n = s['repeat_n']
    vip_n    = s['vip_n']
    dom_n    = repeat_n + vip_n

    if net < 0:
        return (
            "매출이 없는 게 아니다 — 남는 구조가 없는 것이다. "
            "지금은 고객을 더 받기 전에 원장 시술 비중과 비용 구조를 먼저 정리해야 한다."
        )
    if not peak and dom_n < 10:
        return (
            f"비수기({month}월)에 도민 고정 고객이 얇으면 매출을 지킬 수 없다. "
            f"지금 해야 할 것은 하나다 — 휴면 고객 복귀와 2~3회 고객의 4회 전환. "
            f"이것만 되어도 비수기가 달라진다."
        )
    if peak and dom_n < 10:
        return (
            f"성수기({month}월)엔 관광객이 가동률을 채워준다. "
            f"하지만 비수기를 버티는 건 도민이다 — 지금이 도민 기반을 쌓을 기회다. "
            f"관광객은 채워주지만, 쌓아주지는 않는다."
        )
    if dir_util < 60:
        return (
            f"기반은 만들어지고 있다. 원장 가동률 {dir_util:.0f}%의 공백만 채우면 된다. "
            f"도민 고정 고객의 정기 예약이 그 빈자리를 채우는 구조를 이번 달 안에 만들어야 한다."
        )
    if dorm90 >= 10:
        return (
            f"전반적으로 안정적이지만 {dorm90}명의 휴면 고객이 쌓여 있다. "
            f"이미 있는 고객 자산을 방치하는 것은 낭비다 — 지금 연락하면 충분히 살릴 수 있다."
        )
    return (
        "지금 구조는 나쁘지 않다. "
        "도민 반복 고객이 두꺼워질수록 비수기가 무서워지지 않는다 — "
        "잘 오는 고객의 패턴을 분석하고, 그 구조를 다른 고객에게 이식하는 게 다음 단계다."
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 평균 시술시간 자동 계산
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def calculate_avg_duration(df: pd.DataFrame) -> tuple[int, str]:
    """
    데이터에서 평균 시술시간(분)을 자동 계산.
    반환: (평균_분, 계산_기준_설명)

    우선순위:
    1. 시작~종료 시간 컬럼 존재 → 차이값 평균
    2. 소요시간/프로그램시간 컬럼 → 해당값 평균
    3. 없으면 기본값 90분
    이상치 제외 기준: 30분 이하 또는 180분 초과
    """
    DEFAULT = 90

    if df.empty:
        return DEFAULT, "기본값 사용 (데이터 없음)"

    # 1순위: 시작~종료 시간 컬럼
    start_col = next((c for c in df.columns if any(k in c for k in ('시작', '시작시간', 'start'))), None)
    end_col   = next((c for c in df.columns if any(k in c for k in ('종료', '종료시간', '완료', 'end'))), None)

    if start_col and end_col:
        try:
            s = pd.to_datetime(df[start_col], errors='coerce')
            e = pd.to_datetime(df[end_col],   errors='coerce')
            mins = (e - s).dt.total_seconds() / 60
            mins = mins[(mins > 30) & (mins <= 180)]
            if len(mins) >= 3:
                return int(round(mins.mean())), f"시작~종료 시간 기반 ({len(mins)}건 평균)"
        except Exception:
            pass

    # 2순위: 소요시간/프로그램시간 컬럼
    dur_col = next((c for c in df.columns
                    if any(k in c for k in ('소요시간', '시술시간', '프로그램시간', '소요분', '시간(분)'))), None)
    if dur_col:
        try:
            mins = pd.to_numeric(df[dur_col], errors='coerce')
            mins = mins[(mins > 30) & (mins <= 180)]
            if len(mins) >= 3:
                return int(round(mins.mean())), f"소요시간 컬럼 기반 ({len(mins)}건 평균)"
        except Exception:
            pass

    # 3순위: extract_minutes 결과 활용 (기존 시술_시간 컬럼)
    if '시술_시간' in df.columns:
        try:
            mins = pd.to_numeric(df['시술_시간'], errors='coerce')
            mins = mins[(mins > 30) & (mins <= 180)]
            if len(mins) >= 3:
                return int(round(mins.mean())), f"메뉴명 추출 기반 ({len(mins)}건 평균)"
        except Exception:
            pass

    return DEFAULT, "기본값 사용 (관련 컬럼 없음)"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 가동률 계산 엔진 (베드 구조 기반)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def calc_util_new(c: dict, dir_max_per_day: int, working_days: int,
                  fl_single: int, fl_double: int, fl_triple: int,
                  daily_hours: float, avg_duration: int, avg_unit_price: int) -> dict:
    """
    베드 구조 기반 가동률 계산.
    - 원장: 하루 최대 시술 수 × 영업일수
    - 프리랜서: 총 베드 수 × (하루 운영시간(분) / 평균 시술시간)
    """
    fl_beds   = fl_single * 1 + fl_double * 2 + fl_triple * 3
    dir_max   = dir_max_per_day * working_days
    fl_max    = int(fl_beds * (daily_hours * 60 / avg_duration) * working_days) if avg_duration > 0 else 0
    total_max = dir_max + fl_max

    actual     = c.get('시술_건수', 0)
    dir_actual = c.get('원장_건수', 0)
    fl_actual  = max(0, actual - dir_actual)

    dir_util   = round(dir_actual / dir_max   * 100, 1) if dir_max   > 0 else 0.0
    fl_util    = round(fl_actual  / fl_max    * 100, 1) if fl_max    > 0 else 0.0
    total_util = round(actual     / total_max * 100, 1) if total_max > 0 else 0.0

    max_revenue      = total_max * avg_unit_price
    actual_revenue   = c.get('차감_매출', 0)
    rev_potential    = round(actual_revenue / max_revenue * 100, 1) if max_revenue > 0 else 0.0

    # 가동률 판단
    def _grade(u):
        if u >= 70: return "안정", "#22c55e"
        if u >= 50: return "주의", "#f39c12"
        return "위험", "#ef4444"

    dir_grade,   dir_color   = _grade(dir_util)
    fl_grade,    fl_color    = _grade(fl_util)
    total_grade, total_color = _grade(total_util)

    return {
        'fl_beds': fl_beds, 'fl_single': fl_single, 'fl_double': fl_double, 'fl_triple': fl_triple,
        'dir_max': dir_max, 'fl_max': fl_max, 'total_max': total_max,
        'dir_actual': dir_actual, 'fl_actual': fl_actual, 'actual': actual,
        'dir_util': dir_util, 'fl_util': fl_util, 'total_util': total_util,
        'dir_grade': dir_grade, 'dir_color': dir_color,
        'fl_grade': fl_grade,   'fl_color': fl_color,
        'total_grade': total_grade, 'total_color': total_color,
        'max_revenue': max_revenue, 'actual_revenue': actual_revenue,
        'rev_potential': rev_potential,
        'avg_unit_price': avg_unit_price,
        'avg_duration': avg_duration,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메인 앱
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    st.markdown('<div class="hd">🌿 무토 스파 통합 관리 시스템</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub">핸드SOS 매출 · 이중 가동률 · 수익구조 · 월별트렌드 · CRM</div>',
                unsafe_allow_html=True)
    st.divider()

    # ══════════════════════════════════════════════════════════════════════════
    # 사이드바
    # ══════════════════════════════════════════════════════════════════════════
    with st.sidebar:
        st.markdown("## 📂 데이터 업로드")

        # 매출 CSV (복수)
        st.markdown("**📊 매출 파일** (여러 개 동시 업로드 가능)")
        uploaded_sales = st.file_uploader(
            "핸드SOS 매출상세조회 CSV",
            type=['csv'],
            accept_multiple_files=True,
            key="sales_uploader"
        )
        if uploaded_sales:
            frames = []
            for f in uploaded_sales:
                try:
                    frames.append(load_csv(f))
                    st.success(f"✅ {f.name}")
                except Exception as e:
                    st.error(f"❌ {f.name}: {e}")
            if frames:
                with st.spinner("DB 갱신 중..."):
                    new_df  = pd.concat(frames, ignore_index=True)
                    master  = upsert(load_master(), new_df)
                    save_master(master)
                st.success(f"💾 총 {len(master):,}건 저장")

        st.divider()
        st.markdown("## ⚙️ 운영 환경 설정")

        with st.expander("⏰ 시간 설정", expanded=True):
            open_h  = st.time_input("오픈 시간",  value=time(10, 0))
            close_h = st.time_input("마감 시간",  value=time(21, 0))
            daily_hours = max(1.0,
                (datetime.combine(datetime.today(), close_h)
                 - datetime.combine(datetime.today(), open_h)).seconds / 3600)
            working_days = st.number_input("한 달 운영일수", 1, 31, 25, 1)
            st.caption(f"일 {daily_hours:.1f}h · 월 {working_days}일 운영")

        with st.expander("🛏️ 베드 구조 설정", expanded=True):
            st.markdown("**원장 구조**")
            dir_rooms       = st.number_input("원장 관리실 개수", 1, 5, 1, 1)
            dir_max_per_day = st.number_input("원장 하루 최대 시술 수", 1, 10, 4, 1)
            st.markdown("**프리랜서 구조**")
            fl_single = st.number_input("1인실 개수", 0, 10, 2, 1)
            fl_double = st.number_input("2인실 개수", 0, 10, 1, 1)
            fl_triple = st.number_input("3인실 개수", 0, 10, 1, 1)
            fl_beds_total = fl_single * 1 + fl_double * 2 + fl_triple * 3
            st.markdown("**객단가 기준**")
            avg_unit_price = st.number_input("평균 객단가 (₩)", 0, 1_000_000, 150_000, 10_000, format="%d")
            st.caption(
                f"총 베드 {fl_beds_total}개 (1인×{fl_single} + 2인×{fl_double} + 3인×{fl_triple})\n"
                f"평균 시술시간은 데이터에서 자동 계산됩니다."
            )

        with st.expander("💰 비용 설정", expanded=True):
            monthly_rent     = st.number_input("월 임대료 (₩)", 0, 20_000_000, 1_500_000, 100_000, format="%d")
            monthly_material = st.number_input("월 총 재료·비품비 (₩)", 0, 5_000_000, 300_000, 50_000, format="%d")
            target_revenue   = st.number_input("월 목표 차감 매출 (₩)", 0, 50_000_000, 10_000_000, 500_000, format="%d")

        with st.expander("🎯 전환율 입력", expanded=False):
            _conv_data = load_conversion()
            _conv_key = sel_period if 'sel_period' in dir() else ""
            _prev_new  = _conv_data.get(_conv_key, {}).get('신규', 0) if _conv_key else 0
            _prev_re   = _conv_data.get(_conv_key, {}).get('재티켓', 0) if _conv_key else 0
            st.caption("신규 티켓 전환")
            ticket_bought = st.number_input("신규 → 티켓 구매 (명)", 0, 500, _prev_new, 1, format="%d")
            st.caption("재티켓 (기존 고객 재구매)")
            reticket_bought = st.number_input("기존 고객 → 재티켓 구매 (명)", 0, 500, _prev_re, 1, format="%d")
            if st.button("💾 저장", key="save_conv") and _conv_key:
                _conv_data[_conv_key] = {'신규': int(ticket_bought), '재티켓': int(reticket_bought)}
                save_conversion(_conv_data)
                st.success("저장됨")

        with st.expander("💆 프리랜서 단가표", expanded=False):
            st.markdown("""
<table class="pt" style="width:100%;border-collapse:collapse">
<tr><th>카테고리</th><th>시간</th><th>단가</th></tr>
<tr><td>아로마</td><td>60분</td><td>₩40,000</td></tr>
<tr><td>아로마</td><td>90분</td><td>₩50,000</td></tr>
<tr><td>아로마</td><td>120분</td><td>₩65,000</td></tr>
<tr><td>스포츠/건식</td><td>60분</td><td>₩30,000</td></tr>
<tr><td>스포츠/건식</td><td>90분</td><td>₩40,000</td></tr>
<tr><td>발</td><td>-</td><td>₩30,000</td></tr>
</table>""", unsafe_allow_html=True)

        st.divider()

        master_df = load_master()
        if master_df is not None and not master_df.empty:
            st.markdown("## 📅 분석 기간")
            periods    = sorted(master_df['날짜'].dt.to_period('M').unique(), reverse=True)
            sel_period = st.selectbox("분석 월", [str(p) for p in periods], index=0)
            sel_year   = int(sel_period[:4])
            sel_month  = int(sel_period[5:])
            st.divider()
            st.metric("총 누적 레코드", f"{len(master_df):,}건")
            st.metric("데이터 기간",
                      f"{master_df['날짜'].min().strftime('%Y.%m')} ~ "
                      f"{master_df['날짜'].max().strftime('%Y.%m')}")
            st.divider()

            # ── 월별 데이터 삭제 ─────────────────────────────────────────────
            st.markdown("#### 🗑️ 월별 데이터 삭제")
            del_options = [str(p) for p in
                           sorted(master_df['날짜'].dt.to_period('M').unique(), reverse=True)]
            del_period_sel = st.selectbox("삭제할 월 선택", del_options,
                                          key="del_period_sel")

            if st.button("선택 월 데이터 삭제", use_container_width=True,
                         key="btn_del_month"):
                st.session_state['confirm_del'] = del_period_sel

            if st.session_state.get('confirm_del') == del_period_sel:
                st.warning(f"⚠️ {del_period_sel} 데이터를 삭제하시겠습니까?")
                col_yes, col_no = st.columns(2)
                with col_yes:
                    if st.button("✅ 확인", use_container_width=True,
                                 key="btn_del_confirm"):
                        dy = int(del_period_sel[:4])
                        dm = int(del_period_sel[5:])
                        kept = master_df[
                            ~((master_df['날짜'].dt.year  == dy) &
                              (master_df['날짜'].dt.month == dm))
                        ]
                        if kept.empty:
                            if os.path.exists(MASTER_PATH):
                                os.remove(MASTER_PATH)
                        else:
                            kept.to_csv(MASTER_PATH, index=False,
                                        encoding='utf-8-sig')
                        st.session_state.pop('confirm_del', None)
                        st.rerun()
                with col_no:
                    if st.button("❌ 취소", use_container_width=True,
                                 key="btn_del_cancel"):
                        st.session_state.pop('confirm_del', None)
                        st.rerun()

            st.divider()
            # ── 전체 초기화 ──────────────────────────────────────────────────
            if st.button("🗑️ DB 전체 초기화", use_container_width=True):
                if os.path.exists(MASTER_PATH):
                    os.remove(MASTER_PATH)
                st.rerun()
        else:
            no_data(); return

    # ── 데이터 준비 ────────────────────────────────────────────────────────────
    master_df = load_master()
    if master_df is None or master_df.empty:
        no_data(); return

    curr_df = master_df[
        (master_df['날짜'].dt.year == sel_year) &
        (master_df['날짜'].dt.month == sel_month)
    ].copy()

    prev_m  = sel_month - 1 if sel_month > 1 else 12
    prev_y  = sel_year      if sel_month > 1 else sel_year - 1
    prev_df = master_df[
        (master_df['날짜'].dt.year == prev_y) &
        (master_df['날짜'].dt.month == prev_m)
    ].copy()

    c = calc_core(curr_df, daily_hours, working_days, monthly_rent, monthly_material)
    p = calc_core(prev_df, daily_hours, working_days, monthly_rent, monthly_material)
    enriched_df = c.get('df_enriched', curr_df)

    # 평균 시술시간 자동 계산 (enrich 결과 포함된 enriched_df 사용)
    avg_duration, dur_source = calculate_avg_duration(enriched_df)

    # 베드 구조 기반 가동률 계산
    util_data = calc_util_new(
        c, dir_max_per_day, working_days,
        fl_single, fl_double, fl_triple,
        daily_hours, avg_duration, avg_unit_price,
    )

    # ── 탭 ────────────────────────────────────────────────────────────────────
    tab_sales, tab_profit, tab_trend, tab_crm, tab_staff, tab_util, tab_action = st.tabs([
        "💰 매출/순이익 분석",
        "✅ 수익 구조 분석",
        "📈 월별 트렌드",
        "🎣 CRM/작살낚시",
        "👤 담당자/프로그램 분석",
        "📊 가동률 분석",
        "🧠 액션 플랜",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # 탭 1 — 매출/순이익 분석 (핵심 KPI + 가동률 + 15일 점검)
    # ══════════════════════════════════════════════════════════════════════════
    with tab_sales:
        st.markdown(f"### {sel_period} 핵심 지표")

        r1c1, r1c2, r1c3, r1c4 = st.columns(4)
        r1c1.markdown(card(
            "차감 매출 (시술)", f"₩{c.get('차감_매출',0):,}",
            f"전월 ₩{p.get('차감_매출',0):,}", "#667eea",
            mom_pct(c.get('차감_매출',0), p.get('차감_매출',0))
        ), unsafe_allow_html=True)

        au = c.get('avg_unit', 0)
        au_ok = au >= TARGET_UNIT_PRICE
        r1c2.markdown(card(
            "평균 객단가",
            f'<span style="color:{"#22c55e" if au_ok else "#ef4444"}">₩{au:,}</span>',
            f'{"✅ 목표 달성" if au_ok else f"⚠️ 목표 ₩{TARGET_UNIT_PRICE:,}"}',
            "#22c55e" if au_ok else "#ef4444",
            mom_pct(au, p.get('avg_unit', 0))
        ), unsafe_allow_html=True)

        r1c3.markdown(card(
            "시간당 매출", f"₩{c.get('rev_per_h',0):,}",
            f"총 영업 {daily_hours*working_days:.0f}h 기준", "#00b894",
            mom_pct(c.get('rev_per_h',0), p.get('rev_per_h',0))
        ), unsafe_allow_html=True)

        r1c4.markdown(card(
            "AI 평균 시술 시간", f"{c.get('avg_min', DEFAULT_DURATION):.0f}분",
            f"시술 {c.get('시술_건수',0)}건 기준 자동 추출", "#a29bfe"
        ), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        r2c1, r2c2, r2c3, r2c4 = st.columns(4)

        np_v = c.get('net_profit', 0)
        np_c = "#22c55e" if np_v >= 0 else "#ef4444"
        np_rate = f"{np_v/c['차감_매출']*100:.1f}%" if c.get('차감_매출') else "-"
        r2c1.markdown(card(
            "월 순이익",
            f'<span style="color:{np_c}">₩{np_v:,}</span>',
            f"이익률 {np_rate}", np_c,
            mom_pct(np_v, p.get('net_profit', 0))
        ), unsafe_allow_html=True)

        r2c2.markdown(card(
            "프리랜서 총 인건비", f"₩{c.get('fl_wages',0):,}",
            f"시술 {c.get('시술_건수',0) - c.get('원장_건수',0)}건 정산", "#f39c12"
        ), unsafe_allow_html=True)

        r2c3.markdown(card(
            "1인당 평균 방문", f"{c.get('avg_visits',0):.2f}회",
            f"고객 {c.get('unique_cust',0)}명 / 시술 {c.get('시술_건수',0)}건", "#fd79a8"
        ), unsafe_allow_html=True)

        미차감 = c.get('미차감', 0)
        mc_c = "#ef4444" if 미차감 > 500_000 else "#22c55e"
        r2c4.markdown(card(
            "미차감 부채",
            f'<span style="color:{mc_c}">₩{미차감:,}</span>',
            "결제 매출 − 차감 매출", mc_c
        ), unsafe_allow_html=True)

        # ── 💳 결제 vs 차감 분석 ───────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 💳 결제 vs 차감 분석")
        st.caption("총매출은 참고용 / 차감매출은 판단용 — 해석은 차감률 중심으로 합니다.")

        _pay  = c.get('결제_매출', 0)
        _ded  = c.get('차감_매출', 0)
        _und  = c.get('미차감', 0)
        _rate = _ded / _pay * 100 if _pay > 0 else 0.0
        _und_rate = 100 - _rate if _pay > 0 else 0.0
        _prev_pay = p.get('결제_매출', 0)
        _prev_ded = p.get('차감_매출', 0)
        _prev_rate = _prev_ded / _prev_pay * 100 if _prev_pay > 0 else 0.0

        if _rate >= 85:
            _grade, _gc = "건강", "#22c55e"
            _gmsg = "결제된 매출이 대부분 실제 차감으로 이어지고 있어 안정적인 구조입니다."
        elif _rate >= 70:
            _grade, _gc = "주의", "#f39c12"
            _gmsg = "결제와 차감 사이에 간격이 있어 관리가 필요합니다. 미차감 고객을 점검해야 합니다."
        else:
            _grade, _gc = "위험", "#ef4444"
            _gmsg = "결제 대비 실제 차감이 낮습니다. 미차감이 쌓이면 나중에 공짜로 일하는 구조가 됩니다."

        dc1, dc2, dc3, dc4, dc5 = st.columns(5)
        dc1.metric("총매출 (결제)", f"₩{_pay:,}",
                   delta=f"전월 ₩{_prev_pay:,}", delta_color="off")
        dc2.metric("차감매출 (실매출)", f"₩{_ded:,}",
                   delta=mom_pct(_ded, _prev_ded))
        dc3.metric("차감률", f"{_rate:.1f}%",
                   delta=(f"{_rate - _prev_rate:+.1f}%p 전월比"
                          if _prev_pay > 0 else None),
                   delta_color="normal" if _rate >= 85 else "inverse")
        dc4.metric("미차감 금액", f"₩{_und:,}",
                   delta=f"미차감률 {_und_rate:.1f}%",
                   delta_color="inverse" if _und > 0 else "off")
        dc5.metric("상태", _grade, delta_color="off")
        st.markdown("---")
        st.markdown("#### 신규 / 재방 / 손님 매출 분리")
        _n1, _n2, _n3 = st.columns(3)
        _n1.metric("신규 매출", f"₩{c.get('신규_매출', 0):,}", f"{c.get('신규_건수', 0)}건 / {c.get('신규_명수', 0)}명")
        _n2.metric("재방 매출", f"₩{c.get('재방_매출', 0):,}", f"{c.get('재방_건수', 0)}건 / {c.get('재방_명수', 0)}명")
        _n3.metric("손님(워크인) 매출", f"₩{c.get('손님_매출', 0):,}", f"{c.get('손님_건수', 0)}건")
        _신규건수 = c.get('신규_건수', 0)
        _재방건수 = c.get('재방_건수', 0)
        _총건수 = _신규건수 + _재방건수
        if _총건수 > 0:
            _신규pct = _신규건수 / _총건수 * 100
            _재방pct = _재방건수 / _총건수 * 100
            st.markdown(f"**신규 : 재방 비율** — 신규 {_신규pct:.0f}% : 재방 {_재방pct:.0f}% {'✅ 재방 목표(40%) 달성' if _재방pct >= 40 else '⚠️ 재방 목표 40% 미달'}")
        _신규명수 = c.get('신규_명수', 0)
        _재방명수 = c.get('재방_명수', 0)
        _cv1, _cv2 = st.columns(2)
        if _신규명수 > 0 and ticket_bought > 0:
            _conv = ticket_bought / _신규명수 * 100
            _cv1.info(f"🎯 신규 전환율: **{ticket_bought}명/{_신규명수}명 = {_conv:.1f}%** {'✅' if _conv >= 50 else '⚠️ 목표 50% 미달'}")
        if _재방명수 > 0 and reticket_bought > 0:
            _reconv = reticket_bought / _재방명수 * 100
            _cv2.info(f"🔄 재티켓률: **{reticket_bought}명/{_재방명수}명 = {_reconv:.1f}%**")
        st.markdown(f"""<div style="border-left:5px solid {_gc};
            background:#fff;border-radius:10px;padding:1rem 1.4rem;
            box-shadow:0 1px 8px rgba(0,0,0,.06);margin:.5rem 0">
            <strong style="color:{_gc};font-size:1rem">{_grade}</strong>
            &nbsp;|&nbsp; 차감률 {_rate:.1f}%<br>
            <span style="font-size:.93rem;line-height:1.75">{_gmsg}</span>
        </div>""", unsafe_allow_html=True)

        if _rate < 85:
            _warn_msgs = ["미차감은 자산이 아니라 아직 갚아야 할 약속입니다."]
            if _pay > 0 and _rate < 80:
                _warn_msgs.insert(0,
                    "총매출은 좋아 보이지만 차감률이 낮으면 미래 부담이 쌓이는 구조입니다.")
            if _rate < 70:
                _warn_msgs.append("지금은 결제보다 차감 관리가 우선입니다. "
                                   "신규 결제보다 기존 미차감 소진을 먼저 챙겨야 합니다.")
            for _wm in _warn_msgs:
                st.markdown(f"""<div style="background:#fff8f0;border-radius:8px;
                    padding:.7rem 1rem;margin:.3rem 0;font-size:.9rem;
                    border:1px solid #ffe0b2;color:#7c4700">
                    ⚠️ {_wm}
                </div>""", unsafe_allow_html=True)

        # 이중 가동률
        st.markdown("---")
        st.markdown("### 🛏️ 이중 가동률 엔진")
        gc1, gc2 = st.columns(2)
        du = c.get('dir_util', 0)
        fu = c.get('fl_util', 0)
        du_c = "#22c55e" if du >= TARGET_UTIL else ("#f39c12" if du >= 50 else "#ef4444")
        fu_c = "#22c55e" if fu >= TARGET_UTIL else ("#f39c12" if fu >= 50 else "#ef4444")

        with gc1:
            st.markdown(f"""<div class="card" style="border-left-color:{du_c};padding:1.2rem">
              <div class="card-lbl">👑 원장님 가동률 (건수 기반)</div>
              <div class="card-val" style="color:{du_c}">{du:.1f}%</div>
              {gauge(du, du_c)}
              <div class="card-sub">
                시술 {c.get('원장_건수',0)}건 / 최대 {working_days * DIRECTOR_MAX_PER_DAY}건
              </div>
            </div>""", unsafe_allow_html=True)

        with gc2:
            st.markdown(f"""<div class="card" style="border-left-color:{fu_c};padding:1.2rem">
              <div class="card-lbl">🤝 프리랜서 가동률 (분 기반)</div>
              <div class="card-val" style="color:{fu_c}">{fu:.1f}%</div>
              {gauge(fu, fu_c)}
              <div class="card-sub">
                사용 {c.get('fl_used_min',0):,}분 / 가용 {int(daily_hours*60*working_days*FREELANCER_BEDS):,}분
              </div>
            </div>""", unsafe_allow_html=True)

        # 15일 점검
        st.markdown("---")
        st.markdown(f"### 🎯 {sel_period} 15일 중간 점검 & 월말 예측")
        fc = midpoint_forecast(curr_df, sel_year, sel_month, target_revenue, daily_hours, working_days)
        if fc:
            m1, m2, m3 = st.columns(3)
            m1.metric("경과 일수", f"{fc['elapsed']}일", delta=f"잔여 {fc['remain']}일")
            m2.metric("현재 차감 매출", f"₩{fc['curr_rev']:,}", delta=f"일평균 ₩{fc['daily_rev']:,}")
            m3.metric("월말 예측 매출", f"₩{fc['proj_rev']:,}",
                      delta=f"목표의 {fc['rev_pct']:.1f}%",
                      delta_color="normal" if fc['on_track'] else "inverse")

            prog = min(100, fc['curr_rev'] / target_revenue * 100) if target_revenue else 0
            p_c  = "#22c55e" if prog >= 50 else ("#f39c12" if prog >= 30 else "#ef4444")
            st.markdown(f"""<div class="card" style="border-left-color:{p_c};padding:1.2rem">
              <div class="card-lbl">월 목표 달성률 (현재 기준)</div>
              <div class="card-val" style="color:{p_c}">{prog:.1f}%</div>
              {gauge(prog, p_c)}
              <div class="card-sub">현재 ₩{fc['curr_rev']:,} / 목표 ₩{target_revenue:,}</div>
            </div>""", unsafe_allow_html=True)

            if fc['on_track']:
                st.markdown(f"""<div class="bx-g">
                  <strong>✅ 현재 추이라면 월말 목표 달성 가능</strong><br>
                  일평균 ₩{fc['daily_rev']:,} 유지 시 예측 ₩{fc['proj_rev']:,} 달성 예상.
                </div>""", unsafe_allow_html=True)
            else:
                needed = int((target_revenue - fc['curr_rev']) / fc['remain']) if fc['remain'] else 0
                st.markdown(f"""<div class="bx-r">
                  <strong>⚠️ 현재 추이로는 목표 달성 어렵습니다</strong><br>
                  잔여 {fc['remain']}일간 일평균 <strong>₩{needed:,}</strong> 이상 필요.
                </div>""", unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # 탭 2 — 수익 구조 분석
    # ══════════════════════════════════════════════════════════════════════════
    with tab_profit:
        st.markdown(f"### {sel_period} 수익 구조 분석")
        차감 = c.get('차감_매출', 0)
        fl_w = c.get('fl_wages', 0)
        총비용 = monthly_rent + monthly_material + fl_w
        net   = c.get('net_profit', 0)

        p1, p2, p3, p4 = st.columns(4)
        p1.metric("차감 매출", f"₩{차감:,}")
        p2.metric("총 비용", f"₩{총비용:,}",
                  delta=f"-{총비용/차감*100:.1f}%" if 차감 else None, delta_color="inverse")
        p3.metric("월 순이익", f"₩{net:,}",
                  delta=f"이익률 {net/차감*100:.1f}%" if 차감 else None,
                  delta_color="normal" if net >= 0 else "inverse")
        p4.metric("손익분기 객단가",
                  f"₩{int(총비용/c.get('시술_건수',1)):,}" if c.get('시술_건수') else "N/A")

        # ── 매출 투명성: 세 가지 매출 나란히 비교 ─────────────────────────────
        st.markdown("---")
        st.markdown("#### 💡 매출 계산 기준 비교")

        _rb = calc_revenue_breakdown(curr_df)
        _총영업 = _rb['총영업매출']
        _실매출  = _rb['실매출']
        _차감매출 = _rb['차감매출']

        rv1, rv2, rv3 = st.columns(3)
        rv1.metric(
            "총영업매출 (핸드SOS 기준)",
            f"₩{_총영업:,}",
            help="결제액 컬럼 전체 합 — 정액권·충전·환불 포함. 핸드SOS 총영업합계와 동일."
        )
        rv2.metric(
            "실매출 (시술+점판)",
            f"₩{_실매출:,}",
            delta=f"총영업매출의 {_실매출/_총영업*100:.1f}%" if _총영업 else None,
            help="결제액 중 구분='시술' 또는 '점판' 해당 행만 합산. 선결제(정액권/충전) 제외."
        )
        rv3.metric(
            "차감매출 (판단 기준)",
            f"₩{_차감매출:,}",
            delta=f"총영업매출의 {_차감매출/_총영업*100:.1f}%" if _총영업 else None,
            help="판매가 컬럼, 구분='시술' 필터 적용. 현재 순이익/가동률 계산의 기준 매출."
        )

        # 항목별 분해 테이블
        if _rb['항목별']:
            st.markdown("##### 구분별 결제액 분해")
            _rows = []
            for _구분, _금액 in sorted(_rb['항목별'].items(), key=lambda x: -x[1]):
                _비중 = f"{_금액/_총영업*100:.1f}%" if _총영업 else "-"
                _포함여부 = (
                    "✅ 실매출·차감매출" if '시술' in _구분 else
                    "✅ 실매출" if '점판' in _구분 else
                    "⬜ 총영업매출만"
                )
                _rows.append({'구분': _구분, '결제액 합계': f'₩{_금액:,}',
                               '총영업매출 비중': _비중, '포함 범위': _포함여부})
            _rows.append({'구분': '합계', '결제액 합계': f'₩{_총영업:,}',
                           '총영업매출 비중': '100.0%', '포함 범위': ''})
            st.dataframe(pd.DataFrame(_rows), use_container_width=True, hide_index=True)

        # 사용 컬럼 설명 박스
        with st.expander("📌 계산 컬럼 상세 설명"):
            for _label, _desc in _rb['사용컬럼'].items():
                st.markdown(f"- **{_label}**: {_desc}")
            st.markdown("""
---
| 매출 구분 | 포함 항목 | 제외 항목 |
|---|---|---|
| 총영업매출 | 시술, 점판, 정액권 판매, 충전, 회원권 사용, 환불 등 **전체** | — |
| 실매출 | 시술, 점판 | 정액권 판매, 충전, 환불 |
| 차감매출 | 시술 (판매가 기준) | 점판, 정액권, 충전, 환불 |

> **차감매출** = 정액권으로 결제한 시술도 `판매가` 컬럼에 서비스 단가가 기록되므로 포함됨.
> **총영업매출** vs **차감매출** 차이가 클수록 정액권/충전 비중이 높은 구조.
""")

        st.markdown("---")
        pr1, pr2 = st.columns(2)
        with pr1:
            labels = ['순이익', '임대료', '재료·비품비', '프리랜서 인건비']
            vals   = [max(0, net), monthly_rent, monthly_material, fl_w]
            fig_pie = go.Figure(go.Pie(
                labels=labels, values=vals,
                marker_colors=['#22c55e','#ef4444','#f39c12','#a29bfe'],
                hole=.4, textinfo='label+percent', textposition='outside'
            ))
            fig_pie.update_layout(title='매출 대비 비용 구성', height=360,
                                  paper_bgcolor='rgba(0,0,0,0)',
                                  legend=dict(orientation='h', y=-.15))
            st.plotly_chart(fig_pie, use_container_width=True)

        with pr2:
            fig_wf = go.Figure(go.Waterfall(
                measure=['absolute','relative','relative','relative','total'],
                x=['차감 매출','임대료','재료·비품비','프리랜서\n인건비','순이익'],
                y=[차감, -monthly_rent, -monthly_material, -fl_w, net],
                connector={'line':{'color':'#ddd'}},
                decreasing={'marker':{'color':'#ef4444'}},
                increasing={'marker':{'color':'#22c55e'}},
                totals={'marker':{'color':'#22c55e' if net >= 0 else '#ef4444'}},
                text=[f'₩{abs(v):,.0f}' for v in [차감, -monthly_rent, -monthly_material, -fl_w, net]],
                textposition='outside',
            ))
            fig_wf.update_layout(title='수익 폭포 차트', height=360, showlegend=False,
                                  paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_wf, use_container_width=True)

        # 비용 상세 표
        st.markdown("---")
        st.markdown("#### 📋 비용 상세")
        cost_data = {
            '항목': ['임대료', '재료·비품비', '프리랜서 인건비', '총 비용', '차감 매출', '순이익'],
            '금액': [monthly_rent, monthly_material, fl_w, 총비용, 차감, net],
        }
        cost_df = pd.DataFrame(cost_data)
        cost_df['금액'] = cost_df['금액'].apply(lambda x: f'₩{x:,}')
        if 차감:
            cost_df['비중'] = [
                f'{monthly_rent/차감*100:.1f}%',
                f'{monthly_material/차감*100:.1f}%',
                f'{fl_w/차감*100:.1f}%',
                f'{총비용/차감*100:.1f}%',
                '100.0%',
                f'{net/차감*100:.1f}%',
            ]
        st.dataframe(cost_df, use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════════════════════════════
    # 탭 3 — 월별 트렌드
    # ══════════════════════════════════════════════════════════════════════════
    with tab_trend:
        st.markdown("### 📈 월별 트렌드")
        ms = monthly_summary(master_df, daily_hours, working_days)
        if ms.empty:
            st.info("월별 데이터를 분석하려면 2개월 이상의 데이터가 필요합니다.")
        else:
            ms['순이익'] = ms['차감_매출'] - monthly_rent - monthly_material - ms['fl_인건비']
            fig_tr = make_subplots(
                rows=2, cols=2,
                subplot_titles=('월별 차감 매출', '월별 순이익', '가동률 추이', '시술 건수'),
                vertical_spacing=.15, horizontal_spacing=.1,
            )
            kw = dict(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            fig_tr.add_trace(go.Scatter(x=ms['연월'], y=ms['차감_매출'], name='차감 매출',
                                        mode='lines+markers', line=dict(color='#667eea', width=2.5),
                                        marker=dict(size=7)), row=1, col=1)
            fig_tr.add_hline(y=target_revenue, line_dash='dash', line_color='red',
                              annotation_text='목표', row=1, col=1)
            bar_c = ['#22c55e' if v >= 0 else '#ef4444' for v in ms['순이익']]
            fig_tr.add_trace(go.Bar(x=ms['연월'], y=ms['순이익'], name='순이익',
                                    marker_color=bar_c, opacity=.85), row=1, col=2)
            fig_tr.add_trace(go.Scatter(x=ms['연월'], y=ms['원장_가동률'], name='원장',
                                        mode='lines+markers', line=dict(color='#f7971e', width=2)),
                             row=2, col=1)
            fig_tr.add_trace(go.Scatter(x=ms['연월'], y=ms['프리랜서_가동률'], name='프리랜서',
                                        mode='lines+markers', line=dict(color='#667eea', width=2)),
                             row=2, col=1)
            fig_tr.add_hline(y=TARGET_UTIL, line_dash='dash', line_color='red',
                              annotation_text=f'목표 {TARGET_UTIL:.0f}%', row=2, col=1)
            fig_tr.add_trace(go.Bar(x=ms['연월'], y=ms['원장_건수'], name='원장 건수',
                                    marker_color='#f7971e', opacity=.85), row=2, col=2)
            fig_tr.add_trace(go.Bar(x=ms['연월'], y=ms['fl_건수'], name='프리랜서 건수',
                                    marker_color='#667eea', opacity=.85), row=2, col=2)
            fig_tr.update_layout(
                height=600, barmode='stack',
                font=dict(family='Malgun Gothic, Apple SD Gothic Neo, sans-serif'),
                legend=dict(orientation='h', yanchor='bottom', y=1.01, x=0),
                **kw,
            )
            fig_tr.update_xaxes(gridcolor='#f0f0f0', tickangle=30)
            fig_tr.update_yaxes(gridcolor='#f0f0f0')
            st.plotly_chart(fig_tr, use_container_width=True)

            # 신규 / 재방 월별 트렌드
            _all_conv = load_conversion()
            ms['전환율']  = ms['연월'].map(lambda m: _all_conv.get(str(m), {}).get('신규') if isinstance(_all_conv.get(str(m)), dict) else _all_conv.get(str(m)))
            ms['재티켓률'] = ms['연월'].map(lambda m: _all_conv.get(str(m), {}).get('재티켓') if isinstance(_all_conv.get(str(m)), dict) else None)
            if '신규_건수' in ms.columns and ms['신규_건수'].sum() > 0:
                st.markdown("#### 👥 신규 / 재방 월별 트렌드")
                fig_nr = make_subplots(rows=1, cols=3,
                    subplot_titles=('신규 vs 재방 건수', '신규 : 재방 비율(%)', '신규 전환율(%)'),
                    horizontal_spacing=.1)
                fig_nr.add_trace(go.Bar(x=ms['연월'], y=ms['신규_건수'], name='신규',
                                        marker_color='#667eea', opacity=.85), row=1, col=1)
                fig_nr.add_trace(go.Bar(x=ms['연월'], y=ms['재방_건수'], name='재방',
                                        marker_color='#f7971e', opacity=.85), row=1, col=1)
                fig_nr.add_trace(go.Scatter(x=ms['연월'], y=ms['재방_비율'], name='재방 비율',
                                            mode='lines+markers', line=dict(color='#f7971e', width=2.5),
                                            marker=dict(size=7)), row=1, col=2)
                fig_nr.add_hline(y=40, line_dash='dash', line_color='red',
                                  annotation_text='목표 40%', row=1, col=2)
                _conv_ms = ms.dropna(subset=['전환율'])
                if not _conv_ms.empty:
                    fig_nr.add_trace(go.Scatter(x=_conv_ms['연월'], y=_conv_ms['전환율'], name='신규전환율',
                                                mode='lines+markers', line=dict(color='#00b894', width=2.5),
                                                marker=dict(size=7)), row=1, col=3)
                _reconv_ms = ms.dropna(subset=['재티켓률'])
                if not _reconv_ms.empty:
                    fig_nr.add_trace(go.Scatter(x=_reconv_ms['연월'], y=_reconv_ms['재티켓률'], name='재티켓률',
                                                mode='lines+markers', line=dict(color='#fd79a8', width=2.5),
                                                marker=dict(size=7)), row=1, col=3)
                fig_nr.add_hline(y=50, line_dash='dash', line_color='red',
                                  annotation_text='목표 50%', row=1, col=3)
                fig_nr.update_layout(height=320, barmode='stack',
                    font=dict(family='Malgun Gothic, Apple SD Gothic Neo, sans-serif'),
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, x=0),
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                fig_nr.update_xaxes(gridcolor='#f0f0f0', tickangle=30)
                fig_nr.update_yaxes(gridcolor='#f0f0f0')
                st.plotly_chart(fig_nr, use_container_width=True)

            # 월별 요약 테이블
            st.markdown("#### 📋 월별 요약 테이블")
            ms_disp = ms.copy()
            ms_disp['차감_매출'] = ms_disp['차감_매출'].apply(lambda x: f'₩{x:,}')
            ms_disp['순이익']    = ms_disp['순이익'].apply(lambda x: f'₩{x:,}')
            ms_disp['fl_인건비'] = ms_disp['fl_인건비'].apply(lambda x: f'₩{x:,}')
            ms_disp['원장_가동률']     = ms_disp['원장_가동률'].apply(lambda x: f'{x:.1f}%')
            ms_disp['프리랜서_가동률'] = ms_disp['프리랜서_가동률'].apply(lambda x: f'{x:.1f}%')
            st.dataframe(ms_disp, use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════════════════════════════
    # 탭 4 — CRM / 작살낚시
    # ══════════════════════════════════════════════════════════════════════════
    with tab_crm:
        st.markdown("### 🎣 CRM / 작살낚시")

        # ══ 🔥 단골 전환 대상 (최우선) ══════════════════════════════════════
        st.markdown("#### 🔥 단골 전환 대상 — 2~3회 고객 (최우선 관리)")
        st.caption("이미 서비스를 경험하고 다시 온 고객들입니다. 4회를 넘기면 도민 고정 고객으로 전환됩니다. "
                   "장기 미방문 관리보다 이 구간이 먼저입니다.")

        if '고객명' in master_df.columns and '날짜' in master_df.columns:
            _crm_agg: dict = {'최근방문': ('날짜', 'max')}
            if '결제액' in master_df.columns:
                _crm_agg['총결제'] = ('결제액', 'sum')
            else:
                _crm_agg['총결제'] = ('날짜', 'count')
            _g = master_df.groupby('고객명').agg(**_crm_agg).reset_index()
            # 진짜 방문 횟수 (60분 윈도우 기준)
            _vc_crm = true_visit_count(master_df)
            _g['방문수'] = _g['고객명'].map(_vc_crm).fillna(0).astype(int)
            _early = _g[(_g['방문수'] >= 2) & (_g['방문수'] <= 3)].copy()
            _early['최근방문일'] = _early['최근방문'].dt.strftime('%Y-%m-%d')
            _early['미방문일수'] = (pd.Timestamp.now() - _early['최근방문']).dt.days.astype(int)
            _early = _early.sort_values('최근방문', ascending=False)

            if _early.empty:
                st.success("✅ 현재 2~3회 고객이 없습니다. 단골 전환이 잘 이루어지고 있거나 데이터가 부족합니다.")
            else:
                st.markdown(f"**총 {len(_early)}명 — 지금 바로 연락해야 할 대상**")
                for _, row in _early.iterrows():
                    _days   = int(row['미방문일수'])
                    _visits = int(row['방문수'])
                    _pay    = int(row.get('총결제', 0))
                    _last   = str(row['최근방문일'])
                    _urg    = "🔴" if _days > 60 else ("🟡" if _days > 30 else "🟢")
                    with st.expander(
                        f"{_urg} {row['고객명']} · {_visits}회 방문 · "
                        f"최근 {_last} ({_days}일 전) · ₩{_pay:,}"
                    ):
                        st.markdown(f"""<div style="background:#fff8f0;border-left:4px solid #f39c12;
                            border-radius:8px;padding:.9rem 1.2rem;font-size:.92rem;line-height:1.7">
                            <strong>📌 액션</strong>: 4회 패키지 또는 다음 예약 제안<br>
                            <strong>💬 메시지 예시</strong>:<br>
                            "{row['고객명']}님, 지난번에 오셔서 좋으셨으면 했는데
                            다음 방문 예약 잡아드릴까요? 이번에 4회 패키지로 오시면 편하게 정기 관리 가능합니다 😊"
                        </div>""", unsafe_allow_html=True)
                        st.caption(f"방문 {_visits}회 | 최근 방문: {_last} | 총 결제: ₩{_pay:,}")
        else:
            st.info("고객명 또는 날짜 컬럼이 없어 단골 전환 대상을 확인할 수 없습니다.")

        st.markdown("---")

        # ══ 🎯 장기 미방문 VVIP 재활성화 (후순위) ═══════════════════════
        st.markdown("#### 🎯 장기 미방문 VVIP 재활성화 (후순위 관리)")
        st.caption("복구 대상입니다. 단골 전환 관리 후 여력이 생길 때 접근하세요. "
                   "누적 결제액 상위 고객 중 장기 미방문자를 자동 선별합니다.")

        fc1, fc2 = st.columns(2)
        months_thr = fc1.slider("미방문 기준 (개월)", 1, 12, 3, key="fish_months")
        top_n_fish = fc2.slider("대상 고객 수", 3, 20, 10, key="fish_n")

        dormant = get_dormant(master_df, months=months_thr, top_n=top_n_fish)

        if dormant.empty:
            st.success(f"✅ {months_thr}개월 이상 미방문 고객이 없습니다!")
        else:
            st.markdown(f"**총 {len(dormant)}명 대상**")
            for _, row in dormant.iterrows():
                name     = str(row['고객명'])
                days     = int(row.get('미방문_일수', 0))
                pay      = int(row.get('총_결제액', 0))
                visits   = int(row.get('총_방문', 0))
                last_v   = str(row.get('최근_방문', '-'))
                menu_    = str(row.get('주요_메뉴', '-'))
                urg      = "🔴" if days > 180 else ("🟡" if days > 90 else "🟢")

                msg_row = row.copy()
                msg_row['고객명'] = name
                msg = make_message(msg_row)

                with st.expander(
                    f"{urg} {name} · {days}일 미방문 · "
                    f"₩{pay:,} · {visits}회 방문 · 최근 {last_v}"
                ):
                    st.markdown(f"""<div class="msg">
                      <div class="msg-nm">📩 AI 맞춤 안부 메시지 — {name}
                        &nbsp;|&nbsp; 주요 메뉴: {menu_}</div>
                      <div class="msg-bd">{msg}</div>
                    </div>""", unsafe_allow_html=True)
                    st.caption("위 메시지를 복사하여 카카오톡 / 문자로 발송하세요.")

            st.markdown("##### 📋 대상 고객 요약")
            disp_d = dormant.copy()
            if '총_결제액' in disp_d.columns:
                disp_d['총_결제액'] = disp_d['총_결제액'].apply(lambda x: f'₩{x:,.0f}')
            st.dataframe(disp_d, use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════════════════════════════
    # 탭 6 — 📊 가동률 분석
    # ══════════════════════════════════════════════════════════════════════════
    with tab_util:
        ud = util_data
        st.markdown(f"### 📊 가동률 분석 — {sel_period}")
        st.caption("베드 구조 설정값 기준으로 실제 가동률을 계산합니다.")

        # ── 평균 시술시간 자동 계산 결과 표시 ────────────────────────────────
        dur_color = "#22c55e" if "기본값" not in dur_source else "#f39c12"
        st.markdown(f"""<div style="background:#f8f9ff;border:1px solid #e0e4ff;border-radius:10px;
            padding:.75rem 1.1rem;margin-bottom:.8rem;font-size:.88rem;color:#2d3561;">
            ⏱️ <strong>평균 시술시간: {ud['avg_duration']}분</strong>
            &nbsp;·&nbsp;
            <span style="color:{dur_color}">📌 계산 기준: {dur_source}</span>
        </div>""", unsafe_allow_html=True)

        # ── 구조 요약 ─────────────────────────────────────────────────────────
        st.markdown("#### 🏗️ 운영 구조 요약")
        uc1, uc2, uc3 = st.columns(3)
        uc1.markdown(card("원장 월 최대 시술", f"{ud['dir_max']}건",
                          f"하루 {dir_max_per_day}건 × {working_days}일", "#f7971e"), unsafe_allow_html=True)
        uc2.markdown(card("프리랜서 월 최대 시술", f"{ud['fl_max']}건",
                          f"베드 {ud['fl_beds']}개 × 일 {daily_hours:.0f}h ÷ {ud['avg_duration']}분(자동) × {working_days}일",
                          "#667eea"), unsafe_allow_html=True)
        uc3.markdown(card("총 최대 시술", f"{ud['total_max']}건",
                          f"잠재 매출 ₩{ud['max_revenue']:,}", "#a29bfe"), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── 실제 vs 최대 ──────────────────────────────────────────────────────
        st.markdown("#### 📈 실제 vs 최대 비교")
        ua1, ua2, ua3 = st.columns(3)
        ua1.markdown(card("실제 시술 건수", f"{ud['actual']}건",
                          f"원장 {ud['dir_actual']}건 / 프리 {ud['fl_actual']}건", "#00b894"), unsafe_allow_html=True)
        ua2.markdown(card("실제 매출", f"₩{ud['actual_revenue']:,}",
                          f"잠재 매출 대비 {ud['rev_potential']:.1f}%", "#667eea"), unsafe_allow_html=True)
        ua3.markdown(card("잠재 매출 공백",
                          f"₩{max(0, ud['max_revenue'] - ud['actual_revenue']):,}",
                          "가동률 100% 달성 시 추가 가능 매출", "#ef4444"), unsafe_allow_html=True)

        st.markdown("---")

        # ── 가동률 게이지 ─────────────────────────────────────────────────────
        st.markdown("#### 🎯 가동률 현황")
        ug1, ug2, ug3 = st.columns(3)
        with ug1:
            st.markdown(f"""<div class="card" style="border-left-color:{ud['dir_color']};padding:1.2rem">
              <div class="card-lbl">👑 원장 가동률</div>
              <div class="card-val" style="color:{ud['dir_color']}">{ud['dir_util']:.1f}%</div>
              {gauge(ud['dir_util'], ud['dir_color'])}
              <div class="card-sub">
                실제 {ud['dir_actual']}건 / 최대 {ud['dir_max']}건 — <strong>{ud['dir_grade']}</strong>
              </div>
            </div>""", unsafe_allow_html=True)
        with ug2:
            st.markdown(f"""<div class="card" style="border-left-color:{ud['fl_color']};padding:1.2rem">
              <div class="card-lbl">🤝 프리랜서 가동률</div>
              <div class="card-val" style="color:{ud['fl_color']}">{ud['fl_util']:.1f}%</div>
              {gauge(ud['fl_util'], ud['fl_color'])}
              <div class="card-sub">
                실제 {ud['fl_actual']}건 / 최대 {ud['fl_max']}건 — <strong>{ud['fl_grade']}</strong>
              </div>
            </div>""", unsafe_allow_html=True)
        with ug3:
            st.markdown(f"""<div class="card" style="border-left-color:{ud['total_color']};padding:1.2rem">
              <div class="card-lbl">🏠 전체 통합 가동률</div>
              <div class="card-val" style="color:{ud['total_color']}">{ud['total_util']:.1f}%</div>
              {gauge(ud['total_util'], ud['total_color'])}
              <div class="card-sub">
                실제 {ud['actual']}건 / 최대 {ud['total_max']}건 — <strong>{ud['total_grade']}</strong>
              </div>
            </div>""", unsafe_allow_html=True)

        st.markdown("---")

        # ── 가동률 차트 ───────────────────────────────────────────────────────
        st.markdown("#### 📊 원장 / 프리랜서 가동률 비교")
        fig_util = go.Figure()
        categories = ['원장', '프리랜서', '통합']
        values     = [ud['dir_util'], ud['fl_util'], ud['total_util']]
        colors     = [ud['dir_color'], ud['fl_color'], ud['total_color']]
        fig_util.add_trace(go.Bar(
            x=categories, y=values,
            marker_color=colors, text=[f"{v:.1f}%" for v in values],
            textposition='outside', width=0.4,
        ))
        fig_util.add_hline(y=70, line_dash='dash', line_color='#22c55e',
                           annotation_text='안정 기준 70%')
        fig_util.add_hline(y=50, line_dash='dot', line_color='#f39c12',
                           annotation_text='주의 기준 50%')
        fig_util.update_layout(
            height=350, showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            yaxis=dict(range=[0, 115], gridcolor='#f0f0f0'),
            xaxis=dict(gridcolor='#f0f0f0'),
            font=dict(family='Malgun Gothic, Apple SD Gothic Neo, sans-serif'),
        )
        st.plotly_chart(fig_util, use_container_width=True)

        # ── 운영 해석 ─────────────────────────────────────────────────────────
        st.markdown("#### 💬 가동률 해석")
        tu = ud['total_util']
        du = ud['dir_util']
        fu = ud['fl_util']
        rev = ud['actual_revenue']
        max_rev = ud['max_revenue']

        if tu < 50:
            if du < 50 and fu < 50:
                interp_msg = (f"원장({du:.0f}%)·프리랜서({fu:.0f}%) 모두 가동률이 낮다. "
                              f"고객 자체가 부족한 상태 — 신규 유입보다 기존 고객 재방문 구조를 먼저 잡아야 한다.")
                interp_cls = "bx-r"
            elif du < fu:
                interp_msg = (f"원장 가동률({du:.0f}%)이 프리랜서({fu:.0f}%)보다 낮다. "
                              f"원장 담당 예약이 비어 있는 상태 — 원장 시간대 고정 고객을 배치하는 구조가 필요하다.")
                interp_cls = "bx-r"
            else:
                interp_msg = (f"프리랜서 가동률({fu:.0f}%)이 원장({du:.0f}%)보다 낮다. "
                              f"프리랜서 베드가 비고 있다 — 인건비 대비 효율이 떨어지는 구조다.")
                interp_cls = "bx-o"
        elif tu < 70:
            interp_msg = (f"전체 가동률 {tu:.0f}% — 주의 구간이다. "
                          f"잠재 매출 ₩{max_rev:,} 대비 현재 {ud['rev_potential']:.0f}%만 실현하고 있다. "
                          f"빈 슬롯을 채울 재방문 고객이 부족한 것이 핵심 원인이다.")
            interp_cls = "bx-o"
        else:
            if rev < max_rev * 0.8:
                interp_msg = (f"가동률 {tu:.0f}%로 안정권이지만, 실제 매출이 잠재 대비 {ud['rev_potential']:.0f}%다. "
                              f"시술 건수는 채워지고 있는데 객단가가 낮은 상태 — 고가 코스 비중을 높여야 한다.")
                interp_cls = "bx-o"
            else:
                interp_msg = (f"가동률 {tu:.0f}%, 매출 실현율 {ud['rev_potential']:.0f}% — 현재 구조는 안정적이다. "
                              f"다음 단계는 베드를 늘리거나 객단가를 높여 잠재 매출 자체를 키우는 방향이다.")
                interp_cls = "bx-g"

        st.markdown(f"""<div class="{interp_cls}" style="margin:.5rem 0">
            {interp_msg}
        </div>""", unsafe_allow_html=True)

        # ── 핵심 개념 안내 ────────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### 📌 가동률 판단 기준")
        st.markdown("""
| 구간 | 판단 | 의미 |
|------|------|------|
| 70% 이상 | ✅ 안정 | 공간·시간·고객이 균형 잡힌 상태 |
| 50~70% | ⚠️ 주의 | 빈 슬롯이 있음 — 재방문 구조 점검 필요 |
| 50% 미만 | 🚨 위험 | 고객 부족 또는 운영 구조 불균형 |
""")
        st.caption("가동률은 공간 + 시간 + 고객 수의 결과값입니다. "
                   "원장 가동률이 핵심 수익 구조를 결정하고, 프리랜서는 보조 생산 구조입니다.")

    # ══════════════════════════════════════════════════════════════════════════
    # 탭 5 — 담당자 / 프로그램 분석
    # ══════════════════════════════════════════════════════════════════════════
    with tab_staff:
        st.markdown(f"### 👤 담당자 분석 ({sel_period})")
        st.markdown("#### 👩‍⚕️ 담당별 실적")

        if '담당' not in enriched_df.columns:
            st.warning(f"'담당' 컬럼 없음. 현재 컬럼: {list(enriched_df.columns)}")
        else:
            sisl_df = enriched_df[mask_sisl(enriched_df)].copy()
            if sisl_df.empty:
                st.info("시술 데이터가 없습니다.")
            else:
                sisl_df['담당_norm'] = sisl_df['담당'].astype(str).str.strip()
                agg_dict = {'시술_건수': ('날짜', 'count'), '사용_분': ('시술_시간', 'sum'), '인건비': ('인건비', 'sum')}
                if '판매가' in sisl_df.columns:
                    agg_dict['차감_매출'] = ('판매가', 'sum')
                grp = sisl_df.groupby('담당_norm').agg(**agg_dict).reset_index()
                grp.rename(columns={'담당_norm': '담당'}, inplace=True)
                if '차감_매출' not in grp.columns:
                    grp['차감_매출'] = 0
                grp['차감_매출']   = grp['차감_매출'].astype(int)
                grp['평균_객단가'] = (grp['차감_매출'] / grp['시술_건수']).fillna(0).astype(int)
                grp['매출_비중']   = (grp['차감_매출'] / grp['차감_매출'].sum() * 100).round(1)

                def row_util(r):
                    nm = str(r['담당']).strip()
                    if nm == DIRECTOR_KEYWORD:
                        return util_director(int(r['시술_건수']), working_days)
                    return util_freelancer(int(r['사용_분']), daily_hours, working_days)
                grp['가동률'] = grp.apply(row_util, axis=1)

                top  = grp[grp['담당'] == DIRECTOR_KEYWORD]
                rest = grp[grp['담당'] != DIRECTOR_KEYWORD].sort_values('차감_매출', ascending=False)
                grp_sorted = pd.concat([top, rest], ignore_index=True)

                for _, row in grp_sorted.iterrows():
                    nm = str(row['담당']).strip()
                    is_dir = nm == DIRECTOR_KEYWORD
                    is_fl  = (nm != DIRECTOR_KEYWORD) and (nm != '')
                    box    = "bx-o" if is_dir else ("bx-b" if is_fl else "bx-gr")
                    icon   = "👑" if is_dir else ("🤝" if is_fl else "👤")
                    labor_str = ("₩0 (원장님)" if is_dir
                                 else f"₩{int(row['인건비']):,} (단가표 적용)" if is_fl
                                 else "₩0 (기타)")
                    ut_c   = "#22c55e" if row['가동률'] >= TARGET_UTIL else "#ef4444"
                    st.markdown(f"""<div class="{box}">
                      <strong>{icon} {nm}</strong>
                      &nbsp;·&nbsp; 시술 {int(row['시술_건수'])}건
                      &nbsp;·&nbsp; 차감 ₩{int(row['차감_매출']):,}
                      &nbsp;·&nbsp; 객단가 ₩{int(row['평균_객단가']):,}
                      &nbsp;·&nbsp; 인건비 {labor_str}
                      &nbsp;·&nbsp; 비중 {row['매출_비중']:.1f}%<br>
                      가동률: <strong style="color:{ut_c}">{row['가동률']:.1f}%</strong>
                      &nbsp;·&nbsp; 사용 {int(row['사용_분'])}분
                    </div>""", unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)
                sc1, sc2 = st.columns(2)
                with sc1:
                    fig_bar = px.bar(grp_sorted, x='담당', y='차감_매출', color='담당',
                                     text='차감_매출', title='담당별 차감 매출',
                                     color_discrete_sequence=['#f7971e','#667eea','#a29bfe','#00b894','#fd79a8'])
                    fig_bar.update_traces(texttemplate='₩%{text:,.0f}', textposition='outside')
                    fig_bar.update_layout(height=320, showlegend=False,
                                          paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig_bar, use_container_width=True)

                with sc2:
                    fig_ut = px.bar(grp_sorted, x='담당', y='가동률', color='담당',
                                    text='가동률', title='담당별 가동률 (%)',
                                    color_discrete_sequence=['#f7971e','#667eea','#a29bfe','#00b894','#fd79a8'])
                    fig_ut.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                    fig_ut.add_hline(y=TARGET_UTIL, line_dash='dash', line_color='red',
                                     annotation_text=f'목표 {TARGET_UTIL:.0f}%')
                    fig_ut.update_layout(height=320, showlegend=False,
                                         paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                    fig_ut.update_yaxes(range=[0, 110])
                    st.plotly_chart(fig_ut, use_container_width=True)

                # 메뉴 카테고리 분포
                if '메뉴_카테고리' in sisl_df.columns and '판매가' in sisl_df.columns:
                    st.markdown("##### 🏷️ 메뉴 카테고리 분포")
                    cat_cnt = sisl_df.groupby('메뉴_카테고리')['판매가'].sum().reset_index()
                    cat_cnt.columns = ['카테고리', '매출']
                    fig_cat = px.pie(cat_cnt, values='매출', names='카테고리',
                                     title='카테고리별 매출 비중', hole=.38,
                                     color_discrete_sequence=['#667eea','#f7971e','#00b894','#fd79a8','#a29bfe'])
                    fig_cat.update_layout(height=320, paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig_cat, use_container_width=True)

                # 상세메뉴별 매출
                menu_col2 = next((c for c in ('상세메뉴', '메뉴') if c in sisl_df.columns), None)
                if menu_col2 and '판매가' in sisl_df.columns:
                    st.markdown("##### 📋 상세메뉴별 매출")
                    menu_rev = (sisl_df.groupby(menu_col2)['판매가']
                                .agg(['sum', 'count'])
                                .reset_index())
                    menu_rev.columns = ['메뉴', '매출합계', '건수']
                    menu_rev = menu_rev.sort_values('매출합계', ascending=False)
                    menu_rev['비중'] = (menu_rev['매출합계'] / menu_rev['매출합계'].sum() * 100).round(1)
                    menu_rev['매출합계'] = menu_rev['매출합계'].apply(lambda x: f"₩{x:,}")
                    menu_rev['비중'] = menu_rev['비중'].apply(lambda x: f"{x}%")
                    st.dataframe(menu_rev, use_container_width=True, hide_index=True)


    # ══════════════════════════════════════════════════════════════════════════
    # 탭 6 — 🧠 액션 플랜
    # ══════════════════════════════════════════════════════════════════════════
    with tab_action:
        st.markdown(f"### 🧠 운영 분석 보고서 — {sel_period}")
        st.caption("매출 공식 · 고객 구조 · 가동률 기준으로 운영 상태를 진단합니다. "
                   "출력 순서: 숫자 → 구조 해석 → 문제 정의 → 액션 플랜")

        ap = build_action_stats(master_df, c, target_revenue, util_data)
        ap['sel_month'] = sel_month

        # ══ [0] 핵심 수치 (숫자) ═════════════════════════════════════════════
        st.markdown("---")
        st.markdown("#### 📊 핵심 수치")
        n1, n2, n3, n4, n5 = st.columns(5)
        n1.metric("매장 단계", ap['store_stage'],
                  delta=ap['store_stage_desc'].split(' — ')[0], delta_color="off")
        n2.metric("결제 매출", f"₩{ap['payment']:,}")
        ded = ap['deduction_risk']
        n3.metric("차감 매출 (진짜 매출)", f"₩{ap['revenue']:,}",
                  delta=f"비율 {ded['ratio']*100:.0f}% {ded['label']}",
                  delta_color="inverse" if ded['risk'] else "normal")
        n4.metric("평균 객단가", f"₩{ap['avg_unit']:,}",
                  delta=f"목표比 {ap['avg_unit']/max(TARGET_UNIT_PRICE,1)*100:.0f}%",
                  delta_color="normal" if ap['avg_unit'] >= TARGET_UNIT_PRICE else "inverse")
        n5.metric("원장 가동률", f"{ap['dir_util']:.0f}%",
                  delta="안정" if ap['dir_util'] >= 70 else ("주의" if ap['dir_util'] >= 50 else "위험"),
                  delta_color="normal" if ap['dir_util'] >= 70 else "inverse")

        # 미차감 위험 경고
        if ded['risk']:
            st.markdown(f"""<div class="bx-r" style="margin:.5rem 0;padding:.9rem 1.2rem">
                ⚠️ <strong>미차감 누적 위험</strong>: {ded['msg']}
            </div>""", unsafe_allow_html=True)

        # 매출 공식 분해
        rd = ap.get('revenue_decompose')
        if rd:
            primary_color = "#4f86f7" if rd['primary'] == "관리횟수" else "#a06ab4"
            st.markdown(f"""<div class="bx-o" style="margin:.4rem 0;padding:.9rem 1.3rem">
                📐 <strong>매출 공식 분해</strong> — 차감매출 = 관리횟수 × 객단가<br>
                <span style="font-size:.9rem;line-height:1.9">
                현재: {rd['current_cnt']}건 × ₩{rd['current_unit']:,} = ₩{ap['revenue']:,}
                &nbsp;|&nbsp; 목표 부족 <strong>₩{rd['gap']:,}</strong><br>
                방법 A) 관리횟수 → {rd['needed_cnt']}건 필요
                  <span style="color:#666">(+{rd['cnt_gap']}건, +{rd['cnt_pct']:.0f}%)</span><br>
                방법 B) 객단가 → ₩{rd['needed_unit']:,} 필요
                  <span style="color:#666">(+₩{rd['unit_gap']:,}, +{rd['unit_pct']:.0f}%)</span><br>
                <strong style="color:{primary_color}">👉 우선 레버: {rd['primary']}</strong>
                </span>
            </div>""", unsafe_allow_html=True)

        # 인건비 구조 경고
        if ap['labor_over'] and ap['fl_wages'] > 0:
            st.markdown(f"""<div class="bx-o" style="margin:.4rem 0;padding:.8rem 1.2rem">
                💸 <strong>인건비 비율 초과</strong>: 프리랜서 인건비가 매출의
                {ap['labor_ratio']*100:.0f}% — 기준 {int(COST_BENCH['인건비']*100)}% 초과.
                원장 시술 비중을 높여야 한다.
            </div>""", unsafe_allow_html=True)

        # 고객 구조 미니 KPI
        st.markdown("---")
        ak1, ak2, ak3, ak4, ak5 = st.columns(5)
        ak1.metric("전체 고객", f"{ap['total_cust']}명")
        tour_n = ap['one_time_n'] + ap['early_n']
        dom_n  = ap['repeat_n']  + ap['vip_n']
        ak2.metric("관광객/초기 (1~3회)", f"{tour_n}명",
                   delta="제주 정상구조", delta_color="off")
        dom_pct = dom_n / max(ap['total_cust'], 1) * 100
        ak3.metric("도민 고정 추정 (4회+)", f"{dom_n}명",
                   delta=f"{dom_pct:.0f}% (목표 {JEJU_DOM_TARGET_PCT}%)",
                   delta_color="normal" if dom_pct >= 40 else "inverse")
        ak4.metric("VIP (10회+)", f"{ap['vip_n']}명")
        ak5.metric("90일+ 휴면", f"{ap['dormant_90']}명",
                   delta="주의" if ap['dormant_90'] >= 5 else "양호",
                   delta_color="inverse" if ap['dormant_90'] >= 5 else "off")

        # ══ [1] 현재 상태 총진단 (구조 해석) ══════════════════════════════════
        st.markdown("---")
        st.markdown("#### 📌 [1] 현재 상태 총진단")
        diag = generate_action_diagnosis(ap)
        st.markdown(f"""<div style="background:linear-gradient(135deg,#2d3561,#a06ab4);
            color:#fff;border-radius:14px;padding:1.3rem 1.6rem;
            font-size:.98rem;font-weight:500;line-height:1.85;">
            {diag}
        </div>""", unsafe_allow_html=True)
        st.markdown(f"""<div class="bx-b" style="margin:.5rem 0;font-size:.9rem">
            🏪 <strong>{ap['store_stage']}</strong> — {ap['store_stage_desc']}
        </div>""", unsafe_allow_html=True)

        # ══ [2] 고객 구조 분석 (구조 해석) ════════════════════════════════════
        st.markdown("---")
        st.markdown("#### 👥 [2] 고객 구조 분석 — 도민 vs 관광객 추정")
        cs = generate_customer_structure(ap)
        cs_c1, cs_c2 = st.columns([1, 2])
        with cs_c1:
            for item in cs['items']:
                st.markdown(f"""<div style="margin:.35rem 0;background:#f8f9ff;
                    border-radius:8px;padding:.75rem 1rem;border-left:4px solid #4f86f7">
                    <div style="font-size:.85rem;color:#666">{item['label']}</div>
                    <span style="font-size:1.3rem;font-weight:700">{item['n']}명</span>
                    <span style="color:#888;margin-left:.4rem;font-size:.9rem">({item['pct']:.0f}%)</span>
                </div>""", unsafe_allow_html=True)
        with cs_c2:
            st.markdown(f"""<div style="border-left:5px solid {cs['color']};
                background:#fff;border-radius:10px;padding:1rem 1.3rem;
                box-shadow:0 1px 8px rgba(0,0,0,.06);height:100%">
                <span style="color:{cs['color']};font-weight:700;font-size:.95rem">{cs['grade']}</span><br>
                <span style="font-size:.93rem;line-height:1.75">{cs['summary']}</span><br><br>
                <span style="font-size:.8rem;color:#888">
                ※ 4회 이상 방문 = 도민 고정 고객 추정 / 1~3회 = 관광객 또는 초기 도민 혼재<br>
                ※ 운영 목표: 도민 {JEJU_DOM_TARGET_PCT}% : 관광객 {JEJU_TOUR_TARGET_PCT}%
                </span>
            </div>""", unsafe_allow_html=True)

        # ══ [3] 주요 문제/기회 3개 (문제 정의) ════════════════════════════════
        st.markdown("---")
        st.markdown("#### 🔥 [3] 지금 가장 큰 문제 / 기회")
        st.caption("기존 고객 이탈 · 신규 전환율 · 관리횟수(가동률) 세 가지 기준으로 판단합니다.")
        issues = generate_top_issues(ap)
        for i, iss in enumerate(issues, 1):
            color = ["#ef4444", "#f39c12", "#f7971e"][i - 1]
            st.markdown(f"""<div style="background:#fff;border-left:5px solid {color};
                border-radius:10px;padding:.9rem 1.2rem;margin:.4rem 0;
                box-shadow:0 1px 8px rgba(0,0,0,.07);">
                <span style="color:{color};font-weight:700">#{i}</span>&nbsp; {iss}
            </div>""", unsafe_allow_html=True)

        # ══ [4] 운영 구조 해석 (구조 해석) ════════════════════════════════════
        st.markdown("---")
        st.markdown("#### 💬 [4] 운영 구조 해석 — 이 숫자가 말하는 것")
        interp = generate_operational_interpretation(ap)
        for line in interp:
            st.markdown(f"""<div style="background:#f8f9ff;border-radius:10px;
                padding:.9rem 1.1rem;margin:.35rem 0;
                border:1px solid #e0e4ff;font-size:.92rem;color:#2d3561;line-height:1.75;">
                💡 {line}
            </div>""", unsafe_allow_html=True)

        # ══ [5] 이번 주 액션 플랜 (액션) ══════════════════════════════════════
        st.markdown("---")
        st.markdown("#### ⚡ [5] 이번 주 액션 플랜")
        weekly = generate_weekly_action_plan(ap)
        for i, plan in enumerate(weekly, 1):
            st.markdown(f"""<div class="bx-b" style="margin:.5rem 0">
                <strong>✅ {i}. {plan['무엇']}</strong><br>
                <span style="font-size:.88rem;opacity:.9">📌 왜: {plan['왜']}</span><br>
                <span style="font-size:.87rem;opacity:.88">🎯 대상: {plan['대상']}</span><br>
                <span style="font-size:.86rem;opacity:.85">📈 기대효과: {plan['기대효과']}</span>
            </div>""", unsafe_allow_html=True)

        # ══ [6] 이번 달 구조 개선 (액션) ══════════════════════════════════════
        st.markdown("---")
        st.markdown("#### 📅 [6] 이번 달 구조 개선 과제")
        monthly = generate_monthly_action_plan(ap)
        for i, plan in enumerate(monthly, 1):
            st.markdown(f"""<div class="bx-g" style="margin:.5rem 0">
                <strong>🔧 {i}. {plan['제목']}</strong><br>
                <span style="font-size:.88rem;opacity:.9">📌 이유: {plan['이유']}</span><br>
                <span style="font-size:.86rem;opacity:.85">→ {plan['방향']}</span>
            </div>""", unsafe_allow_html=True)

        # ══ [7] 하지 말아야 할 것 ════════════════════════════════════════════
        st.markdown("---")
        st.markdown("#### 🚫 [7] 지금 하지 말아야 할 것")
        donts = generate_donts(ap)
        for dont in donts:
            st.markdown(f"""<div class="bx-r" style="margin:.4rem 0;padding:.85rem 1.2rem">
                <strong>❌</strong> {dont}
            </div>""", unsafe_allow_html=True)

        # ══ [8] 운영 결론 ═════════════════════════════════════════════════════
        st.markdown("---")
        st.markdown("#### 🎯 [8] 운영 결론")
        summary = generate_final_summary(ap)
        st.markdown(f"""<div style="background:linear-gradient(135deg,#1a1a2e,#2d3561);
            color:#fff;border-radius:16px;padding:1.4rem 1.8rem;
            font-size:1.05rem;font-weight:600;line-height:1.85;margin-top:.5rem;">
            {summary}
        </div>""", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)


if __name__ == '__main__':
    main()
