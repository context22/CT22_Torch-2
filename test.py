import pandas as pd

# Create two Series of datetime64[ns, UTC]
# series1 = pd.Series(pd.date_range('2023-01-01', periods=3, tz='UTC'))
series1 = pd.Series(pd.date_range('2024-12-18 07:29:56+00:00', periods=3, tz='UTC'))
# series2 = pd.Series(pd.date_range('2023-01-02', periods=3, tz='UTC'))
series2 = pd.Series(pd.date_range('2024-12-21 13:29:56+00:00', periods=3, tz='UTC'))
print(series1, series2)
# Subtract the two Series
result = series2 - series1

print(result)

