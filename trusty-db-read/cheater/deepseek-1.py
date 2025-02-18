import pandas as pd
from datetime import datetime

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%m/%d/%Y")
    except:
        return None

def classify_borrower(df):
    features = []
    
    for user_id, user_df in df.groupby('userId'):
        loan_count = len(user_df)
        
        early_repayment = 0
        over_repayment = 0
        lost_exists = False
        active_loans = 0
        
        for _, row in user_df.iterrows():
            # 날짜 파싱
            due_date = parse_date(row['dueAt']) if row['dueAt'] else None
            last_repay = parse_date(row['lastRepaymentAt']) if row['lastRepaymentAt'] else None
            
            # 조기 상환 계산
            if due_date and last_repay and last_repay < due_date:
                early_repayment += 1
                
            # 과다 상환 계산
            try:
                loan_amt = float(row['loanAmount'])
                repay_amt = float(row['repaymentAmount']) if row['repaymentAmount'] else 0
                if repay_amt > loan_amt:
                    over_repayment += 1
            except:
                pass
            
            # 부실 대출 확인
            if row['status'] == 'lost':
                lost_exists = True
                
        # 특징 추출
        features.append({
            'userId': user_id,
            'loan_count': loan_count,
            'early_ratio': early_repayment/loan_count if loan_count>0 else 0,
            'over_ratio': over_repayment/loan_count if loan_count>0 else 0,
            'has_lost': lost_exists
        })
    
    feature_df = pd.DataFrame(features)
    
    # 분류 규칙 (정밀도 최적화)
    feature_df['prediction'] = (
        (feature_df['loan_count'] >= 5) &
        (feature_df['early_ratio'] >= 0.7) &
        (feature_df['over_ratio'] >= 0.3) &
        (feature_df['has_lost'])
    )
    
    return feature_df

# 데이터 로드 및 전처리
df_fraud = pd.read_csv('꾼.csv')   # 실제 데이터 경로로 변경 필요
df_normal = pd.read_csv('꾼아님.csv') # 실제 데이터 경로로 변경 필요

# 분류 실행
fraud_features = classify_borrower(df_fraud)
normal_features = classify_borrower(df_normal)

# 성능 평가
tp = fraud_features['prediction'].sum()
fp = normal_features['prediction'].sum()
fn = len(fraud_features) - tp

precision = tp / (tp + fp) if (tp+fp)>0 else 0
recall = tp / (tp + fn) if (tp+fn)>0 else 0

print(f"정밀도: {precision:.4f}")
print(f"재현율: {recall:.4f}")