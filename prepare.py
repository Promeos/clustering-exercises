# Consolidate missing row summary and missing column summary into main function.
import pandas as pd
import numpy as np


def missing_values_summary(df, observations=False):
    '''
    
    '''
    
    # If observations=True, calculate the number of observations that have the
    # same amount of missing values.
    if observations:
        num_columns_missing = df.isnull().sum(axis=1).value_counts().sort_index()
        
    # Calculate the number of missing values for each attribute.
    else:
        # Using `isnull()` and `notnull()` we can calculate the number of missing values and non-null values.
        nulls = df.isnull().sum()
        non_nulls = df.notnull().sum()

        # Add missing values and non-null values together to get the total number values in each column.
        total_values = nulls + non_nulls

        # Create a variable to store the percentage of missing values in each column.
        pct_missing = (nulls/total_values)
    
    # If observations=True: Return groups of observations with the same number of missing values.
    if observations:
        df = pd.DataFrame({'num_cols_missing': num_columns_missing.index,
                           'pct_cols_missing': (num_columns_missing.values/df.shape[0]) * 100,
                           'num_rows': df.isnull().sum(axis=1).value_counts().sort_index()
                          }).reset_index(drop=True)
    # Else: Return the number of missing values for each attribute.
    else:
        df = pd.DataFrame({'attribute':pct_missing.index.values,
                           'num_rows_missing':nulls.values,
                           'pct_rows_missing':pct_missing.values
                          })
    return df