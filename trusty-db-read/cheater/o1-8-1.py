import pandas as pd
import numpy as np

from datetime import datetime
from sklearn.metrics import precision_score, recall_score, classification_report

def parse_date(date_str):
    """
    날짜 문자열을 datetime 형태로 변환.
    - 예시 형식: '9/6/2022', '2023-05-04'
    - 공백 또는 빈 값('')인 경우 None 반환
    """
    if pd.isna(date_str) or date_str == '':
        return None
    
    for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%m/%d/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S']:
        try:
            return datetime.strptime(date_str, fmt)
        except:
            pass
    return None

def load_and_label_data(kkun_file_path, non_kkun_file_path):
    """
    의도적 조기상환 꾼(1) 데이터와, 꾼이 아닌(0) 데이터 불러와서 라벨링.
    """
    df_kkun = pd.read_csv(kkun_file_path, sep=',', engine='python')
    df_kkun['is_kkun'] = 1
    
    df_non_kkun = pd.read_csv(non_kkun_file_path, sep=',', engine='python')
    df_non_kkun['is_kkun'] = 0
    
    df = pd.concat([df_kkun, df_non_kkun], ignore_index=True)
    return df

def feature_engineering(df):
    """
    userId별로 아래 예시 Feature들을 생성:
     - early_repay_count
     - avg_early_loan_amt
     - max_lost_loan_amt
     - total_loan_count
     - lost_count
    """
    # 날짜 파싱
    df['openedAt_dt'] = df['openedAt'].apply(parse_date)
    df['dueAt_dt'] = df['dueAt'].apply(parse_date)
    df['lastRepaymentAt_dt'] = df['lastRepaymentAt'].apply(parse_date)
    
    # '조기상환' 플래그
    def is_early_repay(row):
        if (row['openedAt_dt'] is not None) and \
           (row['dueAt_dt'] is not None) and \
           (row['lastRepaymentAt_dt'] is not None) and \
           (row['status'] == 'performing'):
            total_term = (row['dueAt_dt'] - row['openedAt_dt']).days
            actual_term = (row['lastRepaymentAt_dt'] - row['openedAt_dt']).days
            if total_term > 0 and actual_term >= 0:
                return 1 if actual_term < 0.5 * total_term else 0
        return 0

    df['early_repay_flag'] = df.apply(is_early_repay, axis=1)
    
    # 그룹바이
    grouping = df.groupby('userId')
    
    early_repay_count = grouping['early_repay_flag'].sum()
    early_repay_avg_loan = grouping.apply(
        lambda g: g[g['early_repay_flag'] == 1]['loanAmount'].mean(skipna=True)
    ).fillna(0)

    def get_max_lost_loan_amount(group):
        lost_loans = group[group['status'] == 'lost']['loanAmount']
        return lost_loans.max() if len(lost_loans) > 0 else 0
    
    max_lost_loan_amt = grouping.apply(get_max_lost_loan_amount)
    total_loan_count = grouping.size()
    lost_count = grouping.apply(lambda g: np.sum(g['status'] == 'lost'))
    
    is_kkun_label = grouping['is_kkun'].unique().apply(lambda x: x[0])
    
    feature_df = pd.DataFrame({
        'userId': early_repay_count.index,
        'early_repay_count': early_repay_count.values,
        'avg_early_loan_amt': early_repay_avg_loan.values,
        'max_lost_loan_amt': max_lost_loan_amt.values,
        'total_loan_count': total_loan_count.values,
        'lost_count': lost_count.values,
        'is_kkun': is_kkun_label.values
    })
    
    return feature_df

def apply_rules_variation(feature_df, 
                          min_early_count=3, 
                          min_early_ratio=0.3, 
                          min_lost_count=2,
                          lost_amt_factor=2.5,
                          threshold=3,
                          must_cond_lost=None):
    """
    '점수 + 필수조건' 방식의 룰을 적용.
    
    파라미터 설명:
      - min_early_count : 조기상환 횟수 기준
      - min_early_ratio : 조기상환 비율 기준
      - min_lost_count  : 부실 건수 기준
      - lost_amt_factor : 부실금액이 (조기상환평균 * lost_amt_factor) 이상일 때 +1
      - threshold       : 총점 임계값
      - must_cond_lost  : '가장 큰 대출이 부실' 등을 필수로 할지 여부 (bool or None)
         * True이면 'max_lost_loan_amt == user 전체 대출 중 최대 loanAmount'를 필수로 둠
         * False 또는 None이면 필수조건 없이 점수화만
    
    점수 계산 로직:
      (1) early_repay_count >= min_early_count --> +1
      (2) (early_repay_count / total_loan_count) >= min_early_ratio --> +1
      (3) lost_count >= min_lost_count --> +1
      (4) max_lost_loan_amt >= avg_early_loan_amt * lost_amt_factor --> +1
    """
    # 조기상환 비율
    ratio = feature_df['early_repay_count'] / np.where(feature_df['total_loan_count']>0,
                                                       feature_df['total_loan_count'], 1)
    score = np.zeros(len(feature_df))
    
    # (1)
    cond_a = (feature_df['early_repay_count'] >= min_early_count)
    score += cond_a.astype(int)

    # (2)
    cond_b = (ratio >= min_early_ratio)
    score += cond_b.astype(int)

    # (3)
    cond_c = (feature_df['lost_count'] >= min_lost_count)
    score += cond_c.astype(int)

    # (4)
    cond_d = (feature_df['max_lost_loan_amt'] >= (feature_df['avg_early_loan_amt'] * lost_amt_factor))
    score += cond_d.astype(int)

    # 필수조건: "가장 큰 대출이 부실" 적용 여부
    must_condition = np.ones(len(feature_df), dtype=bool)  # 기본 True
    if must_cond_lost:
        # user별로 '전체 대출 중 최대 loanAmount'를 찾아서 필수인지 체크
        # (실제로는 df 접근 필요. 여기서는 feature_df만으로는 알 수 없음 -> 가정 or 별도 인자)
        # 편의를 위해 'max_lost_loan_amt > 0이면 일단 필수조건 통과' 등 간단히 처리 or
        # 예시 구현: 'lost_count >=1 이면서 max_lost_loan_amt == user 전체 대출 중 최대값' 
        #           => (실무에서는 df와 join하여 구체적으로 계산)
        # 여기서는 max_lost_loan_amt가 0이 아니면 '일단 True' 라고 가정 (데모 목적)
        # ----------------------------------------------------
        # 간단 예시: "lost_count >=1" and "max_lost_loan_amt > 0" => 필수
        must_condition = (feature_df['lost_count'] >= 1) & (feature_df['max_lost_loan_amt'] > 0)
        # 실제로는 'max_lost_loan_amt == user 최대 loan' 등을 구해야 함

    predict_kkun = np.where((must_condition) & (score >= threshold), 1, 0)
    return predict_kkun

def main():
    # 1. CSV 경로 (예시)
    kkun_file_path = '꾼.csv'
    non_kkun_file_path = '꾼아님.csv'
    
    # 2. 데이터 로드 및 피처 엔지니어링
    df = load_and_label_data(kkun_file_path, non_kkun_file_path)
    feature_df = feature_engineering(df)
    
    # 3. 여러 가지 룰 파라미터 정의 (10개)
    #    - 점수 조건( early_count, early_ratio, lost_count, lost_amt_factor, threshold, must_cond_lost 등) 다양화
    rules_params_list = [
        # rule1: (기존 + must_cond X)
        {'name': 'rule1',
         'min_early_count':3, 'min_early_ratio':0.3, 'min_lost_count':2,
         'lost_amt_factor':2.5, 'threshold':3, 'must_cond_lost':False},
        # rule2: (필수조건 = 'max_lost>0', threshold=3)
        {'name': 'rule2',
         'min_early_count':3, 'min_early_ratio':0.3, 'min_lost_count':1,
         'lost_amt_factor':2.5, 'threshold':3, 'must_cond_lost':True},
        # rule3: (early_count=4, ratio=0.4, lost_count=1, factor=2.0, threshold=3)
        {'name': 'rule3',
         'min_early_count':4, 'min_early_ratio':0.4, 'min_lost_count':1,
         'lost_amt_factor':2.0, 'threshold':3, 'must_cond_lost':False},
        # rule4: (가중치 살짝 완화, threshold=2)
        {'name': 'rule4',
         'min_early_count':2, 'min_early_ratio':0.2, 'min_lost_count':1,
         'lost_amt_factor':2.0, 'threshold':2, 'must_cond_lost':False},
        # rule5: (정밀도 우선, must_cond_lost + lost_count>=2, threshold=4)
        {'name': 'rule5',
         'min_early_count':3, 'min_early_ratio':0.3, 'min_lost_count':2,
         'lost_amt_factor':2.5, 'threshold':4, 'must_cond_lost':True},
        # rule6: (조기상환 횟수=5, 비율=0.5, lost_count=1, factor=2, thr=3)
        {'name': 'rule6',
         'min_early_count':5, 'min_early_ratio':0.5, 'min_lost_count':1,
         'lost_amt_factor':2.0, 'threshold':3, 'must_cond_lost':False},
        # rule7: (조기상환 횟수=2, 비율=0.2, lost_count=2, factor=1.8, thr=3)
        {'name': 'rule7',
         'min_early_count':2, 'min_early_ratio':0.2, 'min_lost_count':2,
         'lost_amt_factor':1.8, 'threshold':3, 'must_cond_lost':False},
        # rule8: (필수: lost_count>=1, must_cond_lost, factor=3.0, thr=3)
        {'name': 'rule8',
         'min_early_count':3, 'min_early_ratio':0.3, 'min_lost_count':1,
         'lost_amt_factor':3.0, 'threshold':3, 'must_cond_lost':True},
        # rule9: (threshold=4, min_early_count=4, lost_count=2)
        {'name': 'rule9',
         'min_early_count':4, 'min_early_ratio':0.3, 'min_lost_count':2,
         'lost_amt_factor':2.0, 'threshold':4, 'must_cond_lost':False},
        # rule10: (가장 빡빡, must_cond_lost, threshold=4, min_early_count=4, lost_count=2, factor=2.5)
        {'name': 'rule10',
         'min_early_count':4, 'min_early_ratio':0.4, 'min_lost_count':2,
         'lost_amt_factor':2.5, 'threshold':4, 'must_cond_lost':True},
    ]
    
    # 4. 10개 룰 각각 적용 → 성능 측정
    results = []
    for rule_cfg in rules_params_list:
        name = rule_cfg['name']
        
        y_pred = apply_rules_variation(
            feature_df,
            min_early_count=rule_cfg['min_early_count'],
            min_early_ratio=rule_cfg['min_early_ratio'],
            min_lost_count=rule_cfg['min_lost_count'],
            lost_amt_factor=rule_cfg['lost_amt_factor'],
            threshold=rule_cfg['threshold'],
            must_cond_lost=rule_cfg['must_cond_lost']
        )
        
        y_true = feature_df['is_kkun'].values
        prec = precision_score(y_true, y_pred, pos_label=1)
        rec  = recall_score(y_true, y_pred, pos_label=1)
        
        results.append({
            'rule_name': name,
            'precision': prec,
            'recall': rec
        })
    
    # 5. 결과 출력
    print("=== 여러 개 Rule 결과 ===")
    for res in results:
        rname = res['rule_name']
        p = res['precision']
        r = res['recall']
        print(f"{rname}: precision={p:.4f}, recall={r:.4f}")

    # 추가로 'best'를 골라볼 수도 있음
    # 예: 정밀도와 재현율이 모두 0.8 이상인 rule이 있는지 체크
    valid_candidates = [res for res in results if res['precision']>=0.8 and res['recall']>=0.8]
    if len(valid_candidates) == 0:
        print(">>> 재현율과 정밀도 모두 0.8 이상인 룰이 없습니다.")
    else:
        print(">>> 아래 룰은 정밀도/재현율 모두 0.8 이상 달성:")
        for c in valid_candidates:
            print(f"    - {c['rule_name']}")

main()