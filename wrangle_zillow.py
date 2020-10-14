import pandas as pd
import numpy as np

from sklearn.impute import SimpleImputer


def missing_values_summary(df, observations=False):
    '''
    
    '''
    
    # If observations=True, calculate the number of observations that have the
    # same amount of missing values.
    if observations:
        num_columns_missing = df.isnull().sum(axis=1).value_counts().sort_index()
        
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
                           'pct_cols_missing': (num_columns_missing.index/df.shape[1]),
                           'num_rows': df.isnull().sum(axis=1).value_counts().sort_index()
                          }).reset_index(drop=True)
    # Else: Return the number of missing values for each attribute.
    else:
        df = pd.DataFrame({'attribute':pct_missing.index.values,
                           'num_rows_missing':nulls.values,
                           'pct_rows_missing':pct_missing.values
                          })
    return df


def handle_missing_values(df, prop_required_column =.75, prop_required_row =.75):
    '''
    
    '''
    
    # drop columns
    df.drop(columns=['calculatedbathnbr', 'id', 'finishedsquarefeet12',
                     'propertylandusetypeid', 'fullbathcnt'
                    ],
            inplace=True)
    #drop properties with no restrooms or bedrooms
    df = df[(df.bedroomcnt > 0) & (df.bathroomcnt > 0)]
    
    # Threshold variable holds the equivalent of 75% of total rows in a dataframe
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    
    # Threshold variable holds the equivalent of 75% of total columns in a dataframe
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df


def impute_missing_data(train, validate, test):
    '''
    
    '''
    numerical_columns = [
    'calculatedfinishedsquarefeet',
    'lotsizesquarefeet',
    'structuretaxvaluedollarcnt',
    'taxvaluedollarcnt',
    'landtaxvaluedollarcnt',
    'taxamount'
]
    
    categorical_columns = [
    "regionidcity",
    "regionidzip",
    "yearbuilt",
    "regionidcity",
    "censustractandblock"
]
    
    imputer = SimpleImputer(strategy='median')
    
    train[numerical_columns] = imputer.fit_transform(train[numerical_columns])
    validate[numerical_columns] = imputer.transform(validate[numerical_columns])
    test[numerical_columns] = imputer.transform(test[numerical_columns])
    
    
    imputer = SimpleImputer(strategy='most_frequent')
    
    train[categorical_columns] = imputer.fit_transform(train[categorical_columns])
    validate[categorical_columns] = imputer.transform(validate[categorical_columns])
    test[categorical_columns] = imputer.transform(test[categorical_columns])
    
    return train, validate, test