# NBA Game Winner and Stats Predictor

By combining SMOTEENN with XGBoost, we were able gain +14% macro average accuracy gain on predicting the winners of NBA games and binary statistic results (e.g. which team has higher rebounds, assists, etc.).

For example, the home team wins 59% of the time, so pure guessing that the home team wins results in 59% accuracy. Our trained model predicts the home team winner correctly with 74% accuracy -- a +15% improvement. See the powerpoint for more details

This was a final project for CMPE 255 @ SJSU.

## Files

```
datasets/
    - *.csv: datasets
    - data.py        : module to retrieve dataframes of our datasets
    - build_*.py     : scripts used to build our training datasets
classifiers/         : *.ipynb trained classifiers
    - boost/
    - decision-tree/
    - random-forest/
    - kmeans/ (not used in final results)
nba-api/             : scripts to call nba api to build datasets (not used in final results)
research/
    - research.ipynb : brief initial data exploration
```
