import pandas as pd
import numpy as np
from itertools import groupby

# 강화된 재무 건전성 평가 함수
def enhanced_financial_soundness(group):
    mean_balance = group['balance'].mean()
    median_balance = group['balance'].median()
    stable_ratio = (group['balance'] >= median_balance).mean()
    critical_ratio = (group['balance'] < 500).mean()  # 저 잔고에 집중

    # 조건 강화
    high_path = mean_balance >= 10000 and median_balance >= 2000
    stability_path = mean_balance >= 3000 and stable_ratio >= 0.3 and critical_ratio <= 0.15
    return high_path or stability_path

# 강화된 변동성 패턴 평가 함수
def enhanced_volatility_patterns(group):
    balances = group['balance'].values
    diffs = np.diff(balances)
    max_consecutive_drops = max((sum(1 for _ in g) for k, g in groupby(diffs < 0) if k), default=0)
    low_ratio = (balances < 500).mean()  # 저 잔고 비율

    # 조건 강화
    return max_consecutive_drops <= 15 and low_ratio <= 0.3

# 규칙 평가 함수
def evaluate_enhanced_rules(data):
    enhanced_scores = data.groupby('user_id').apply(
        lambda group: pd.Series({
            'enhanced_financial_soundness': enhanced_financial_soundness(group),
            'enhanced_volatility_patterns': enhanced_volatility_patterns(group),
        })
    )

    enhanced_scores['rule_satisfaction'] = (
        enhanced_scores['enhanced_financial_soundness'] &
        enhanced_scores['enhanced_volatility_patterns']
    )
    
    #enhanced_scores['true_label'] = enhanced_scores.index.isin(positive_cases)

    #true_positives = enhanced_scores[(enhanced_scores['rule_satisfaction'] == True) & (enhanced_scores['true_label'] == True)]
    #false_positives = enhanced_scores[(enhanced_scores['rule_satisfaction'] == True) & (enhanced_scores['true_label'] == False)]
    #false_negatives = enhanced_scores[(enhanced_scores['rule_satisfaction'] == False) & (enhanced_scores['true_label'] == True)]

    #recall = len(true_positives) / (len(true_positives) + len(false_negatives)) if (len(true_positives) + len(false_negatives)) > 0 else 0
    #precision = len(true_positives) / (len(true_positives) + len(false_positives)) if (len(true_positives) + len(false_positives)) > 0 else 0

    return enhanced_scores

# 실행 예제
if __name__ == "__main__":
    file_path = "all_users_daily_balance.csv"
    data = pd.read_csv(file_path)

    # 날짜 형식 변환
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'])

    #positive_cases = [523, 546, 539, 559, 577, 600, 612, 580, 549]
    #negative_cases = [98, 74, 558, 466, 502, 594, 295, 504, 583, 623, 446]

    results = evaluate_enhanced_rules(data)

    #print(f"Enhanced Recall: {recall:.2f}, Enhanced Precision: {precision:.2f}")
    results.to_excel('balance_rule_result.xlsx')