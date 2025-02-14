import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import classification_report

# 데이터 로드 및 레이블 추가
scammer = pd.read_csv('꾼.csv')
normal = pd.read_csv('꾼아님.csv')
scammer['is_scammer'] = 1
normal['is_scammer'] = 0
df = pd.concat([scammer, normal])

# 날짜 변환
date_cols = ['openedAt', 'dueAt', 'lastRepaymentAt']
for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# 특징 공학
df['loan_duration'] = (df['dueAt'] - df['openedAt']).dt.days
df['repayment_period'] = (df['lastRepaymentAt'] - df['openedAt']).dt.days
df['early_repayment_days'] = df['loan_duration'] - df['repayment_period']
df['repayment_ratio'] = df['repaymentAmount'] / df['loanAmount'].replace(0, np.nan)
df['repayment_ratio'] = df['repayment_ratio'].fillna(0)
df['status_lost'] = df['status'].map({'lost':1, 'performing':0})
df['has_due_date'] = df['dueAt'].notna().astype(int)
df['has_repayment'] = df['lastRepaymentAt'].notna().astype(int)

# 전처리 파이프라인
num_features = ['loanAmount','repayment_ratio','early_repayment_days']
cat_features = ['status_lost','has_due_date','has_repayment']

preprocessor = ColumnTransformer([
    ('num', Pipeline([
        ('impute', SimpleImputer(strategy='median')),
        ('scale', StandardScaler())
    ]), num_features),
    ('cat', Pipeline([
        ('impute', SimpleImputer(strategy='most_frequent')),
        ('encode', OneHotEncoder())
    ]), cat_features)
])

# 모델 훈련
X = df[num_features + cat_features]
y = df['is_scammer']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

model = Pipeline([
    ('preprocessor', preprocessor),
    ('classifier', RandomForestClassifier())
])

model.fit(X_train, y_train)

# 평가
print(classification_report(y_test, model.predict(X_test)))

# 특징 중요도 분석
feature_names = model.named_steps['preprocessor'].get_feature_names_out()
importances = model.named_steps['classifier'].feature_importances_
print("중요한 특징:", sorted(zip(importances, feature_names), reverse=True))