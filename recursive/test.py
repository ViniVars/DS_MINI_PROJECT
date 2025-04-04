import pandas as pd
import numpy as np
import shap
import matplotlib.pyplot as plt

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, StratifiedKFold, GridSearchCV
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler
from sklearn.utils.class_weight import compute_class_weight
from imblearn.over_sampling import SMOTE

# Load dataset
df = pd.read_csv("recursive/DS_MINI_PROJECT2.csv")

# Handle missing values
df.fillna(0, inplace=True)

print(df.dtypes)

# Drop unnecessary columns
df.drop(['id', 'score_date', 'crm_employee_range', 'crm_industry_current', 'new_score_date', 'last_score_date'], axis=1, inplace=True)

# Split into features & target
X = df.drop('churned', axis=1)
y = df['churned']

# Compute class weights before resampling
# class_weights = dict(zip(np.unique(y), compute_class_weight('balanced', classes=np.unique(y), y=y)))

# Apply SMOTE for balancing
smote = SMOTE(sampling_strategy='auto', random_state=42)
X_resample, y_resample = smote.fit_resample(X, y)

# Train-test split after SMOTE
X_train, X_test, y_train, y_test = train_test_split(
    X_resample, y_resample, test_size=0.2, random_state=42, stratify=y_resample
)

# Standardize features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Initialize and train Logistic Regression
lr = LogisticRegression(solver='liblinear', max_iter=2000)
lr.fit(X_train_scaled, y_train)

# Predictions
pred_test = lr.predict(X_test_scaled)
pred_train = lr.predict(X_train_scaled)

# Print classification reports
print("Test Set Classification Report:")
print(classification_report(y_test, pred_test))

print("Train Set Classification Report:")
print(classification_report(y_train, pred_train))

# Hyperparameter tuning
param_grid = {
    'C': [0.01, 0.1, 1, 10, 100],
    'solver': ['liblinear', 'newton-cg'],
    'max_iter': [1000, 2000, 5000],
    'class_weight': ['balanced']
}

grid_search = GridSearchCV(
    LogisticRegression(),
    param_grid,
    cv=5,
    scoring='f1',
    verbose=1,
    n_jobs=-1
)
grid_search.fit(X_train_scaled, y_train)

# Print best parameters
print("Best Hyperparameters:", grid_search.best_params_)

# Train final model with best parameters
best_lr = LogisticRegression(**grid_search.best_params_)
best_lr.fit(X_train_scaled, y_train)

# Final predictions
final_pred = best_lr.predict(X_test_scaled)

# Final model evaluation
print("Final Model Classification Report:")
print(classification_report(y_test, final_pred))

# SHAP Analysis
explainer = shap.Explainer(best_lr, X_train_scaled)
shap_values = explainer(X_test_scaled)


shap_df = pd.DataFrame(shap_values.values, columns=X.columns)
print("\nSHAP Values for Test Set:\n", shap_df.head())

# Summary Plot (Feature Importance)
plt.figure(figsize=(10, 6))
shap.summary_plot(shap_values, X_test, feature_names=X.columns)
plt.show()
