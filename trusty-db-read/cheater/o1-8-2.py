import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.metrics import precision_score, recall_score, classification_report

def parse_date(date_str):
    if pd.isna(date_str) or date_str == '':
        return None
    for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%m/%d/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S']:
        try:
            return datetime.strptime(date_str, fmt)
        except:
            pass
    return None

def load_and_label_data(kkun_file_path, non_kkun_file_path):
    df_kkun = pd.read_csv(kkun_file_path, sep=',', engine='python')
    df_kkun['is_kkun'] = 1
    df_non_kkun = pd.read_csv(non_kkun_file_path, sep=',', engine='python')
    df_non_kkun['is_kkun'] = 0
    df = pd.concat([df_kkun, df_non_kkun], ignore_index=True)
    return df

def feature_engineering(df):
    df['openedAt_dt'] = df['openedAt'].apply(parse_date)
    df['dueAt_dt'] = df['dueAt'].apply(parse_date)
    df['lastRepaymentAt_dt'] = df['lastRepaymentAt'].apply(parse_date)
    
    def is_early_repay(row):
        if (row['openedAt_dt'] is not None) and \
           (row['dueAt_dt'] is not None) and \
           (row['lastRepaymentAt_dt'] is not None) and \
           (row['status'] == 'performing'):
            total_term = (row['dueAt_dt'] - row['openedAt_dt']).days
            actual_term = (row['lastRepaymentAt_dt'] - row['openedAt_dt']).days
            if total_term>0 and actual_term>=0:
                return 1 if actual_term < 0.5*total_term else 0
        return 0
    df['early_repay_flag'] = df.apply(is_early_repay, axis=1)
    
    grouping = df.groupby('userId')
    early_repay_count = grouping['early_repay_flag'].sum()
    early_repay_avg_loan = grouping.apply(
        lambda g: g[g['early_repay_flag']==1]['loanAmount'].mean(skipna=True)
    ).fillna(0)
    
    def get_max_lost_loan_amount(grp):
        lost = grp[grp['status']=='lost']['loanAmount']
        return lost.max() if len(lost)>0 else 0
    max_lost_loan_amt = grouping.apply(get_max_lost_loan_amount)
    
    total_loan_count = grouping.size()
    lost_count = grouping.apply(lambda g: np.sum(g['status']=='lost'))
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
                          min_early_count=5,
                          min_early_ratio=0.5,
                          min_lost_count=1,
                          lost_amt_factor=2.0,
                          threshold=3,
                          must_cond_lost=False):
    ratio = feature_df['early_repay_count'] / np.where(feature_df['total_loan_count']>0,
                                                       feature_df['total_loan_count'],1)
    score = np.zeros(len(feature_df))

    # 점수 4가지
    cond_a = (feature_df['early_repay_count'] >= min_early_count)    # +1
    cond_b = (ratio >= min_early_ratio)                              # +1
    cond_c = (feature_df['lost_count'] >= min_lost_count)            # +1
    cond_d = (feature_df['max_lost_loan_amt'] >= 
              feature_df['avg_early_loan_amt']*lost_amt_factor)      # +1

    score += cond_a.astype(int)
    score += cond_b.astype(int)
    score += cond_c.astype(int)
    score += cond_d.astype(int)

    # 필수조건
    must_condition = np.ones(len(feature_df), dtype=bool)
    if must_cond_lost:
        # "가장 큰 대출 부실" 등 구현 필요 - 여기서는 간단히 lost_count>=1이면 필수통과
        must_condition = (feature_df['lost_count']>=1) & (feature_df['max_lost_loan_amt']>0)

    predict_kkun = np.where((must_condition)&(score>=threshold),1,0)
    return predict_kkun

def main():
    kkun_file_path = '꾼.csv'
    non_kkun_file_path = '꾼아님.csv'
    
    df = load_and_label_data(kkun_file_path, non_kkun_file_path)
    feature_df = feature_engineering(df)

    # --- 새로운 10개 룰: Rule6 인사이트 기반 ---
    new_rules_params_list = [
        # A) (Rule6와 동일)
        {'name': 'ruleA',
         'min_early_count':5, 'min_early_ratio':0.5,
         'min_lost_count':1, 'lost_amt_factor':2.0,
         'threshold':3, 'must_cond_lost':False},
        # B) 조기상환 횟수 6, 비율 0.5
        {'name': 'ruleB',
         'min_early_count':6, 'min_early_ratio':0.5,
         'min_lost_count':1, 'lost_amt_factor':2.0,
         'threshold':3, 'must_cond_lost':False},
        # C) 비율 0.6
        {'name': 'ruleC',
         'min_early_count':5, 'min_early_ratio':0.6,
         'min_lost_count':1, 'lost_amt_factor':2.0,
         'threshold':3, 'must_cond_lost':False},
        # D) 배수 2.5
        {'name': 'ruleD',
         'min_early_count':5, 'min_early_ratio':0.5,
         'min_lost_count':1, 'lost_amt_factor':2.5,
         'threshold':3, 'must_cond_lost':False},
        # E) 부실횟수=2
        {'name': 'ruleE',
         'min_early_count':5, 'min_early_ratio':0.5,
         'min_lost_count':2, 'lost_amt_factor':2.0,
         'threshold':3, 'must_cond_lost':False},
        # F) threshold=4
        {'name': 'ruleF',
         'min_early_count':5, 'min_early_ratio':0.5,
         'min_lost_count':1, 'lost_amt_factor':2.0,
         'threshold':4, 'must_cond_lost':False},
        # G) must_cond_lost=True, threshold=3
        {'name': 'ruleG',
         'min_early_count':5, 'min_early_ratio':0.5,
         'min_lost_count':1, 'lost_amt_factor':2.0,
         'threshold':3, 'must_cond_lost':True},
        # H) must_cond_lost=True, threshold=4
        {'name': 'ruleH',
         'min_early_count':5, 'min_early_ratio':0.5,
         'min_lost_count':1, 'lost_amt_factor':2.0,
         'threshold':4, 'must_cond_lost':True},
        # I) 부실횟수=2, 배수=2.5
        {'name': 'ruleI',
         'min_early_count':5, 'min_early_ratio':0.5,
         'min_lost_count':2, 'lost_amt_factor':2.5,
         'threshold':3, 'must_cond_lost':False},
        # J) 조기상환 횟수=6, 배수=2.5, threshold=4
        {'name': 'ruleJ',
         'min_early_count':6, 'min_early_ratio':0.5,
         'min_lost_count':1, 'lost_amt_factor':2.5,
         'threshold':4, 'must_cond_lost':False},
    ]

    results = []
    for rule_cfg in new_rules_params_list:
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

    print("=== [New 10 Rules] 결과 ===")
    for res in results:
        print(f"{res['rule_name']}: precision={res['precision']:.4f}, recall={res['recall']:.4f}")

    # 혹시 0.8/0.8 이상 찾기
    good_ones = [r for r in results if r['precision']>=0.8 and r['recall']>=0.8]
    if good_ones:
        print(">>> 아래 룰은 precision/recall 모두 >=0.8")
        for r in good_ones:
            print(f"    {r['rule_name']}")
    else:
        print(">>> (주의) precision/recall 동시 0.8 이상인 룰은 없습니다.")

if __name__ == "__main__":
    main()
