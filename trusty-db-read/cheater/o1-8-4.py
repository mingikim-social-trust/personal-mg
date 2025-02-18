import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.metrics import precision_score, recall_score

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
    return pd.concat([df_kkun, df_non_kkun], ignore_index=True)

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

def apply_rules_weighted(feature_df,
                         # 가중치 설정
                         weight_early_count5=1,   # 조기상환 5회 이상
                         weight_early_count6=1,   # 조기상환 6회 이상이면 추가점수?
                         weight_ratio05=1,        # 비율 >=0.5
                         weight_ratio06=1,        # 비율 >=0.6 추가점수?
                         weight_lost1=1,          # 부실1회이상
                         weight_lost2=1,          # 부실2회이상
                         weight_factor20=1,       # 배수>=2.0
                         weight_factor25=1,       # 배수>=2.5 추가점수?
                         threshold=3,
                         must_cond_lost=False):
    """가중치 기반으로 점수를 부여:
       - 조기상환 횟수>=5 -> +weight_early_count5
         추가로 횟수>=6 -> +weight_early_count6 (누적)
       - 조기상환 비율>=0.5 -> +weight_ratio05
         추가로 비율>=0.6 -> +weight_ratio06
       - lost_count>=1 -> +weight_lost1
         추가로 lost_count>=2 -> +weight_lost2
       - 배수>=2.0 -> +weight_factor20
         추가로 배수>=2.5 -> +weight_factor25
    """
    ratio = feature_df['early_repay_count'] / np.where(feature_df['total_loan_count']>0,
                                                       feature_df['total_loan_count'], 1)
    
    score = np.zeros(len(feature_df))

    # 조기상환 횟수
    cond_cnt5 = (feature_df['early_repay_count'] >=5)
    cond_cnt6 = (feature_df['early_repay_count'] >=6)
    score += cond_cnt5.astype(int)*weight_early_count5
    score += cond_cnt6.astype(int)*weight_early_count6

    # 조기상환 비율
    cond_ratio05 = (ratio >=0.5)
    cond_ratio06 = (ratio >=0.6)
    score += cond_ratio05.astype(int)*weight_ratio05
    score += cond_ratio06.astype(int)*weight_ratio06

    # 부실 건수
    cond_lost1 = (feature_df['lost_count'] >=1)
    cond_lost2 = (feature_df['lost_count'] >=2)
    score += cond_lost1.astype(int)*weight_lost1
    score += cond_lost2.astype(int)*weight_lost2

    # 부실금액 배수
    lost_factor = np.where(feature_df['avg_early_loan_amt']>0,
                           feature_df['max_lost_loan_amt']/feature_df['avg_early_loan_amt'],
                           0)
    cond_factor20 = (lost_factor >=2.0)
    cond_factor25 = (lost_factor >=2.5)
    score += cond_factor20.astype(int)*weight_factor20
    score += cond_factor25.astype(int)*weight_factor25

    # 필수조건 (예: "가장 큰 대출이 부실" 등)
    must_condition = np.ones(len(feature_df), dtype=bool)
    if must_cond_lost:
        must_condition = (feature_df['lost_count']>=1) & (feature_df['max_lost_loan_amt']>0)

    predict_kkun = np.where((must_condition)&(score>=threshold),1,0)
    return predict_kkun

def main():
    # 데이터 로드
    df = load_and_label_data('꾼.csv', '꾼아님.csv')
    feature_df = feature_engineering(df)

    # 새로 제안할 10개 룰
    new_rules_params_list = [
        # 1) ruleW1: 횟수>=5->+1,>=6->+2, 비율>=0.5->+1,>=0.6->+1, lost>=1->+1,>=2->+1, factor>=2->+1,>=2.5->+1, thr=4
        {'name':'ruleW1','w_cnt5':1,'w_cnt6':2,'w_r05':1,'w_r06':1,
         'w_l1':1,'w_l2':1,'w_f20':1,'w_f25':1,'threshold':4,'must_cond_lost':False},
        
        # 2) ruleW2: 위보다 살짝 느슨, threshold=3
        {'name':'ruleW2','w_cnt5':1,'w_cnt6':1,'w_r05':1,'w_r06':1,
         'w_l1':1,'w_l2':2,'w_f20':1,'w_f25':2,'threshold':3,'must_cond_lost':False},
        
        # 3) ruleW3: must_cond_lost=True, threshold=3
        {'name':'ruleW3','w_cnt5':1,'w_cnt6':1,'w_r05':1,'w_r06':1,
         'w_l1':1,'w_l2':1,'w_f20':1,'w_f25':2,'threshold':3,'must_cond_lost':True},
        
        # 4) ruleW4: threshold=5 (점수 많이 필요), 조기상환 비중 점수 강화
        {'name':'ruleW4','w_cnt5':1,'w_cnt6':2,'w_r05':2,'w_r06':2,
         'w_l1':1,'w_l2':1,'w_f20':1,'w_f25':1,'threshold':5,'must_cond_lost':False},
        
        # 5) ruleW5: 부실점수 강화(lost1->2, lost2->3), threshold=5
        {'name':'ruleW5','w_cnt5':1,'w_cnt6':1,'w_r05':1,'w_r06':2,
         'w_l1':2,'w_l2':3,'w_f20':1,'w_f25':1,'threshold':5,'must_cond_lost':False},
        
        # 6) ruleW6: factor 점수 강화(f20->2,f25->3), threshold=4
        {'name':'ruleW6','w_cnt5':1,'w_cnt6':1,'w_r05':1,'w_r06':1,
         'w_l1':1,'w_l2':1,'w_f20':2,'w_f25':3,'threshold':4,'must_cond_lost':False},
        
        # 7) ruleW7: must_cond_lost=True + 부실 강화 + threshold=4
        {'name':'ruleW7','w_cnt5':1,'w_cnt6':2,'w_r05':1,'w_r06':1,
         'w_l1':1,'w_l2':2,'w_f20':1,'w_f25':2,'threshold':4,'must_cond_lost':True},
        
        # 8) ruleW8: 더 강한 조기상환 점수 + threshold=3
        {'name':'ruleW8','w_cnt5':2,'w_cnt6':3,'w_r05':2,'w_r06':3,
         'w_l1':1,'w_l2':1,'w_f20':1,'w_f25':2,'threshold':3,'must_cond_lost':False},
        
        # 9) ruleW9: 조기상환 약간 완화 + 부실조건 높임
        {'name':'ruleW9','w_cnt5':1,'w_cnt6':1,'w_r05':1,'w_r06':1,
         'w_l1':1,'w_l2':2,'w_f20':2,'w_f25':2,'threshold':4,'must_cond_lost':False},
        
        # 10) ruleW10: must_cond_lost, 조기상환 극강(5->2점,6->3점), threshold=4
        {'name':'ruleW10','w_cnt5':2,'w_cnt6':3,'w_r05':1,'w_r06':2,
         'w_l1':1,'w_l2':1,'w_f20':2,'w_f25':3,'threshold':4,'must_cond_lost':True},
    ]

    results = []
    for rule_cfg in new_rules_params_list:
        name = rule_cfg['name']
        y_pred = apply_rules_weighted(
            feature_df,
            weight_early_count5=rule_cfg['w_cnt5'],
            weight_early_count6=rule_cfg['w_cnt6'],
            weight_ratio05=rule_cfg['w_r05'],
            weight_ratio06=rule_cfg['w_r06'],
            weight_lost1=rule_cfg['w_l1'],
            weight_lost2=rule_cfg['w_l2'],
            weight_factor20=rule_cfg['w_f20'],
            weight_factor25=rule_cfg['w_f25'],
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

    print("=== [가중치 방식 + 인사이트 기반 10 RULES] 결과 ===")
    for res in results:
        print(f"{res['rule_name']}: precision={res['precision']:.4f}, recall={res['recall']:.4f}")

    # 0.8/0.8 이상 찾기
    strong = [r for r in results if r['precision']>=0.8 and r['recall']>=0.8]
    if strong:
        print(">>> precision/recall >=0.8 달성 룰:")
        for s in strong:
            print(f"   - {s['rule_name']}")
    else:
        print(">>> (주의) 모든 룰에서 precision/recall 0.8 동시에 만족하는 경우 없음.")

if __name__=="__main__":
    main()
