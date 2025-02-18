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
    
    # 날짜 포맷 여러 가지를 시도
    for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%m/%d/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S']:
        try:
            return datetime.strptime(date_str, fmt)
        except:
            pass
    return None

def load_and_label_data(kkun_file_path, non_kkun_file_path):
    """
    의도적 조기상환 꾼 데이터와, 꾼이 아닌 데이터 불러와서 라벨링.
    """
    # 1) 꾼 데이터
    df_kkun = pd.read_csv(kkun_file_path, sep=',', engine='python')  # 구분자, 인코딩 등 필요시 수정
    df_kkun['is_kkun'] = 1
    
    # 2) 꾼 아님 데이터
    df_non_kkun = pd.read_csv(non_kkun_file_path, sep=',', engine='python')
    df_non_kkun['is_kkun'] = 0
    
    # 데이터 합치기
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
    # 날짜 컬럼들을 datetime으로 변환
    df['openedAt_dt'] = df['openedAt'].apply(parse_date)
    df['dueAt_dt'] = df['dueAt'].apply(parse_date)
    df['lastRepaymentAt_dt'] = df['lastRepaymentAt'].apply(parse_date)
    
    # 조기상환으로 볼 기준: (lastRepaymentAt_dt - openedAt_dt) < (dueAt_dt - openedAt_dt)의 50% 이하
    # 단, dueAt_dt가 없거나 lastRepaymentAt_dt가 없으면 계산 불가하므로 제외
    def is_early_repay(row):
        if (row['openedAt_dt'] is not None) and \
           (row['dueAt_dt'] is not None) and \
           (row['lastRepaymentAt_dt'] is not None) and \
           (row['status'] == 'performing'):
            total_term = (row['dueAt_dt'] - row['openedAt_dt']).days
            actual_term = (row['lastRepaymentAt_dt'] - row['openedAt_dt']).days
            if total_term > 0 and actual_term >= 0:  # 음수 방지
                # 예) 전체 기간의 50% 미만으로 상환했으면 '조기 상환'이라 간주
                return 1 if actual_term < 0.5 * total_term else 0
        return 0

    df['early_repay_flag'] = df.apply(is_early_repay, axis=1)
    
    # 그룹바이: userId 단위
    grouping = df.groupby('userId')
    
    # early_repay_count, early_repay인 건들의 평균 대출금액
    early_repay_count = grouping['early_repay_flag'].sum()
    early_repay_avg_loan = grouping.apply(
        lambda g: g[g['early_repay_flag'] == 1]['loanAmount'].mean(skipna=True)
    ).fillna(0)
    
    # lost 상태의 건 중 최대 대출금액
    def get_max_lost_loan_amount(group):
        lost_loans = group[group['status'] == 'lost']['loanAmount']
        if len(lost_loans) > 0:
            return lost_loans.max()
        return 0
    
    max_lost_loan_amt = grouping.apply(get_max_lost_loan_amount)
    
    # 총 대출 건수, lost 대출 건수
    total_loan_count = grouping.size()
    lost_count = grouping.apply(lambda g: np.sum(g['status'] == 'lost'))
    
    # is_kkun 라벨(정답)도 userId 단위로 하나만 필요.
    # (여기서는 모든 row가 같은 userId에 같은 라벨이므로 groupBy 후 unique() 가져옴)
    is_kkun_label = grouping['is_kkun'].unique().apply(lambda x: x[0])
    
    # 새로운 DF 구성
    feature_df = pd.DataFrame({
        'early_repay_count': early_repay_count,
        'avg_early_loan_amt': early_repay_avg_loan,
        'max_lost_loan_amt': max_lost_loan_amt,
        'total_loan_count': total_loan_count,
        'lost_count': lost_count,
        'is_kkun': is_kkun_label
    }).reset_index()  # userId를 다시 칼럼으로
    
    return feature_df

def apply_rules(feature_df):
    """
    개선된 룰 (가중치 기반):
      - 조건별로 점수를 부여하고, 총 점수가 4점 이상이면 꾼(1)으로 분류합니다.
      
      조건 및 부여 점수:
         1) early_repay_count >= 3              --> +1
         2) (early_repay_count / total_loan_count) >= 0.35   --> +1
         3) lost_count >= 3                      --> +2  (부실 건수가 많을수록 꾼일 가능성이 높음)
         4) max_lost_loan_amt >= (avg_early_loan_amt * 3)  --> +1
      
      총점이 4점 이상이면 꾼(1)으로 예측.
      이 방식은 정밀도를 높이면서도, 재현율이 너무 낮아지지 않도록 조정한 예시입니다.
    """
    # 전체 대출 대비 조기상환 비율 계산 (total_loan_count가 0인 경우 1로 대체)
    early_ratio = feature_df['early_repay_count'] / np.where(feature_df['total_loan_count'] > 0, feature_df['total_loan_count'], 1)
    
    # 각 조건별 점수를 계산합니다.
    score = np.zeros(len(feature_df))
    score += (feature_df['early_repay_count'] >= 3).astype(int)                   # +1
    score += (early_ratio >= 0.35).astype(int)                                      # +1
    score += (feature_df['lost_count'] >= 3).astype(int) * 2                        # +2
    score += (feature_df['max_lost_loan_amt'] >= feature_df['avg_early_loan_amt'] * 3).astype(int)  # +1
    
    # 총 점수가 4점 이상이면 꾼(1), 그렇지 않으면 Non-꾼(0)
    predict_kkun = np.where(score >= 4, 1, 0)
    return predict_kkun




def main():
    # 가정: CSV 파일 두 개 (경로는 예시)
    kkun_file_path = '꾼.csv'
    non_kkun_file_path = '꾼아님.csv'
    
    # 1. 데이터 로드 및 라벨링
    df = load_and_label_data(kkun_file_path, non_kkun_file_path)
    
    # 2. 피처 엔지니어링
    feature_df = feature_engineering(df)
    
    # 3. 룰 기반 분류
    feature_df['predict_kkun'] = apply_rules(feature_df)
    
    # 4. 성능 평가(정밀도, 재현율)
    y_true = feature_df['is_kkun'].values
    y_pred = feature_df['predict_kkun'].values
    
    precision = precision_score(y_true, y_pred, pos_label=1)
    recall = recall_score(y_true, y_pred, pos_label=1)
    
    print("=== Classification Report: o1-10 ===")
    print(classification_report(y_true, y_pred, target_names=['Non-꾼(0)', '꾼(1)']))
    print(f"Precision (꾼) : {precision:.4f}")
    print(f"Recall    (꾼) : {recall:.4f}")

if __name__ == "__main__":
    main()