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
                         weight_early_count5=1,
                         weight_early_count6=1,
                         weight_ratio05=1,
                         weight_ratio06=1,
                         weight_lost1=1,
                         weight_lost2=1,
                         weight_factor20=1,
                         weight_factor25=1,
                         threshold=3,
                         must_cond_lost=False):
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
    cond_lost1 = (feature_df['lost_count']>=1)
    cond_lost2 = (feature_df['lost_count']>=2)
    score += cond_lost1.astype(int)*weight_lost1
    score += cond_lost2.astype(int)*weight_lost2

    # 부실금액 배수
    lost_factor = np.where(feature_df['avg_early_loan_amt']>0,
                           feature_df['max_lost_loan_amt']/feature_df['avg_early_loan_amt'],
                           0)
    cond_factor20 = (lost_factor>=2.0)
    cond_factor25 = (lost_factor>=2.5)
    score += cond_factor20.astype(int)*weight_factor20
    score += cond_factor25.astype(int)*weight_factor25

    # 필수조건
    must_condition = np.ones(len(feature_df), dtype=bool)
    if must_cond_lost:
        must_condition = (feature_df['lost_count']>=1) & (feature_df['max_lost_loan_amt']>0)

    predict_kkun = np.where((must_condition)&(score>=threshold),1,0)
    return predict_kkun


def main():
    # 1) CSV Load
    df = load_and_label_data('꾼.csv', '꾼아님.csv')
    feature_df = feature_engineering(df)

    # 2) 파라미터 후보(가중치 범위 등) 정의
    weight_early_count5_list = [1,2]
    weight_early_count6_list = [1,2,3]
    weight_ratio05_list      = [1,2]
    weight_ratio06_list      = [1,2,3]
    weight_lost1_list        = [1,2]
    weight_lost2_list        = [2,3]
    weight_factor20_list     = [1,2]
    weight_factor25_list     = [2,3]
    threshold_list           = [3,4]
    must_cond_lost_list      = [False, True]

    # 3) 30개 조합을 추출(단순 for문으로 순회하다가 30개 넘어가면 break)
    rules_configs = []
    count_rules = 0
    for wec5 in weight_early_count5_list:
        for wec6 in weight_early_count6_list:
            for wr05 in weight_ratio05_list:
                for wr06 in weight_ratio06_list:
                    for wl1 in weight_lost1_list:
                        for wl2 in weight_lost2_list:
                            for wf20 in weight_factor20_list:
                                for wf25 in weight_factor25_list:
                                    for thr in threshold_list:
                                        for mc in must_cond_lost_list:
                                            config = {
                                                'w_cnt5': wec5, 'w_cnt6': wec6,
                                                'w_r05': wr05, 'w_r06': wr06,
                                                'w_l1': wl1,  'w_l2': wl2,
                                                'w_f20': wf20,'w_f25': wf25,
                                                'threshold': thr, 'must_cond_lost': mc
                                            }
                                            rules_configs.append(config)
                                            count_rules += 1
                                            if count_rules>=30:
                                                break
                                        if count_rules>=30: break
                                    if count_rules>=30: break
                                if count_rules>=30: break
                            if count_rules>=30: break
                        if count_rules>=30: break
                    if count_rules>=30: break
                if count_rules>=30: break
            if count_rules>=30: break
        if count_rules>=30: break

    # 4) 각 조합별로 적용
    results = []
    for idx, cfg in enumerate(rules_configs):
        y_pred = apply_rules_weighted(
            feature_df,
            weight_early_count5=cfg['w_cnt5'],
            weight_early_count6=cfg['w_cnt6'],
            weight_ratio05=cfg['w_r05'],
            weight_ratio06=cfg['w_r06'],
            weight_lost1=cfg['w_l1'],
            weight_lost2=cfg['w_l2'],
            weight_factor20=cfg['w_f20'],
            weight_factor25=cfg['w_f25'],
            threshold=cfg['threshold'],
            must_cond_lost=cfg['must_cond_lost']
        )
        y_true = feature_df['is_kkun'].values
        prec = precision_score(y_true, y_pred, pos_label=1)
        rec  = recall_score(y_true, y_pred, pos_label=1)
        
        rule_name = f"rule_{idx+1}"
        results.append({
            'rule_name': rule_name,
            **cfg,
            'precision': prec,
            'recall': rec
        })

    # 5) 결과 출력
    print("=== 30개 룰 결과 ===")
    for r in results:
        print(f"[{r['rule_name']}] thr={r['threshold']}, must={r['must_cond_lost']}, "
              f"wec5={r['w_cnt5']}, wec6={r['w_cnt6']}, wr05={r['w_r05']}, wr06={r['w_r06']}, "
              f"wl1={r['w_l1']}, wl2={r['w_l2']}, wf20={r['w_f20']}, wf25={r['w_f25']} "
              f"=> precision={r['precision']:.4f}, recall={r['recall']:.4f}")

    # precision/recall >=0.8인 룰 찾기
    top_candidates = [r for r in results if (r['precision']>=0.8 and r['recall']>=0.8)]
    if top_candidates:
        print(">>> 정밀도, 재현율 모두 0.8 이상 달성한 룰:")
        for t in top_candidates:
            print(f"   - {t['rule_name']}")
    else:
        print(">>> (알림) 30개 중에는 precision/recall 모두 0.8 이상인 룰이 없습니다.")

if __name__=="__main__":
    main()
