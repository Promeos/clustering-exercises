import env
import os
import pandas as pd


###################### SQL Connection w/ Credentials ######################

def get_connection(db, user=env.user, host=env.host, password=env.password):
    '''
    Returns a formatted url with login credentials to access data on a SQL database.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'


###################### Acquire Zillow Data ######################
def get_zillow_data():
    '''
    This function acquires the zillow dataset from Codeup's database using Pandas and SQL.
    It returns the zillow dataset as a Pandas DataFrame.
    
    If the zillow data is stored in the current repository, it returns a Pandas DataFrame.

    If the zillow data does not exist in the current directory, it queries Codeup's database for the dataset.
    A local copy will be created as a csv file in the current directory for future use.
    '''
    
    sql_query ='''
    select *
    from properties_2017
    join(select parcelid,
        logerror,
        max(transactiondate) as lasttransactiondate
        from predictions_2017
        group by parcelid, logerror
        ) as predictions using(parcelid)
    left join `airconditioningtype` using(`airconditioningtypeid`)
    left join `architecturalstyletype` using(`architecturalstyletypeid`)
    left join `buildingclasstype` using(`buildingclasstypeid`)
    left join `heatingorsystemtype` using(`heatingorsystemtypeid`)
    left join `propertylandusetype` using(`propertylandusetypeid`)
    left join `storytype` using(`storytypeid`)
    left join `typeconstructiontype` using(`typeconstructiontypeid`)
    where (latitude is not null
    and longitude is not null);'''
    
    file = 'zillow.csv'
    
    if os.path.isfile(file):
        return pd.read_csv('zillow.csv')
    else:
        df = pd.read_sql(sql_query, get_connection('zillow'))
        df.to_csv('zillow.csv', index=False)
        return df





###################### Acquire Mall Customer Data ######################

def get_mall_customer_data():
    '''
    This function acquires the mall customers dataset from Codeup's database Pandas and SQL.
    It returns the mall customers dataset as a Pandas DataFrame.
    
    If the mall customer data is stored in the current repository, it returns a Pandas DataFrame.

    If the mall customer data does not exist in the current directory, it queries Codeup's database for the dataset.
    A local copy will be created as a csv file in the current directory for future use.
    '''
    
    sql_query = "SELECT * FROM customers;"
    file = 'mall_customers.csv'
    
    if os.path.isfile(file):
        return pd.read_csv('mall_customers.csv')
    else:
        df = pd.read_sql(sql_query, get_connection('mall_customers'))
        df.to_csv('mall_customers.csv', index=False)
        return df

###################### Acquire Telco Data - 2 Year Contracts ######################

def get_telco_data():
    '''
    This function acquires `telco_churn` data from a SQL database using SQL and Pandas.
    
    If the telco_churn data is stored in the current repository, it returns a Pandas DataFrame.

    If the telco_churn data does not exist in the current directory, it queries Codeup's database for the dataset.
    A local copy will be created as a csv file in the current directory for future use.
    
    Data Description:
    Customers with two year contracts at a telecommunications company
    shape: (1695, 4)
    Attributes: customer_id, monthly_charges, tenure
    Target: total_charges
    
    Attribute Definitions:
    customer_id: A unique alpha-numeric ID that identifies a customer
    monthly_charges: The amount a customer pays each month for service - in USD.
    tenure: The length of a customers relationship with telco. Measured in months.
    total_charges: The total amount a customer has been charged over the lifetime of service - in USD
    '''
    
    # verbose acquisition to explicitly state the source of contract_type_id and ensure data integrity
    sql_query = '''
    select customer_id, monthly_charges, tenure, total_charges
    from customers
    where contract_type_id = (
        select contract_type_id
        from contract_types
        where contract_type = 'Two Year'
    );
    '''
    
    file = 'two_year_contracts.csv'
    
    if os.path.isfile(file):
        return pd.read_csv('two_year_contracts.csv')
    else:
        df = pd.read_sql(sql_query, get_connection('telco_churn'))
        df.to_csv('two_year_contracts.csv', index=False)
        return df

###################### Acquire Titanic Data ######################

def get_titanic_data():
    '''
    Acquires the titanic dataset from Codeup's database using Pandas and SQL. 
    Returns the titanic dataset as a Pandas DataFrame.
    
    If the titanic data is stored in the current repository, it returns a Pandas DataFrame.

    If the titanic data does not exist in the current directory, it queries Codeup's database for the titanic dataset.
    A local copy will be created as a csv file in the current directory for future use.
    '''
    file = 'titanic.csv'

    if os.path.isfile(file):
        return pd.read_csv('titanic.csv')
    else:
        df = pd.read_sql('select * from passengers;', get_connection('titanic_db'))
        df.to_csv('titanic.csv', index=False)
        return df

###################### Acquire Iris Data ######################

def get_iris_data():
    '''
    This function acquires the iris dataset from Codeup's database.
    It returns the iris dataset as a Pandas DataFrame.
    
    If the iris data is stored in the current repository, it returns a Pandas DataFrame.

    If the iris data does not exist in the current directory, it queries Codeup's database for the iris dataset.
    A local copy will be created as a csv file in the current directory for future use.
    '''
    file = 'iris.csv'

    if os.path.isfile(file):
        return pd.read_csv('iris.csv')
    else:
        df = pd.read_sql("""
                        select *from measurements
                        join species using(`species_id`);
                        """,
                        get_connection('iris_db'))
        df.to_csv('iris.csv', index=False)
        return df


