# How to train & args configuration
## train.py

#### 1. Setups
- --train_p_n: The ratio of positive/negative samples in the training set
- --classifier: Optional classifier(extra_tree/decision_tree)
- --cross_validation: Whether to use cross validation
- --predict_month: Whether to use the trained model to predict the data
#### 2. Select feature
- --last_days
- --rel
- --org
- --vp_num
- --invalid_type
- --cover_asn
- --vrp_num
- --vrp_as_num
- --vp_received_num
- --self_pfx_num
- --other_pfx_num
- --self_valid_pfx_num
- --other_valid_pfx_num

#### Configuration that we use
```
python3 train.py --train_p_n=3 --classifier=extra_tree --cross_validation --predict_month --last_days --rel --org --vp_num --invalid_type --self_pfx_num --self_valid_pfx_num
```