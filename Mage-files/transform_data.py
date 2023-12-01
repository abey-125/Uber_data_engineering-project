import pandas as pd 
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    data['tpep_pickup_datetime']=pd.to_datetime(data['tpep_pickup_datetime'])
    data['tpep_dropoff_datetime']=pd.to_datetime(data['tpep_dropoff_datetime'])
    data.drop_duplicates().reset_index(drop=True)
    data['trip_id']=data.index
    date_time_dim=data[['tpep_pickup_datetime','tpep_dropoff_datetime']]

    
    # Creating new columns from the available column
    date_time_dim['tpep_pickup_datetime']=date_time_dim['tpep_pickup_datetime']
    date_time_dim['pick_hour']=date_time_dim['tpep_pickup_datetime'].dt.hour
    date_time_dim['pick_day']=date_time_dim['tpep_pickup_datetime'].dt.day
    date_time_dim['pick_month']=date_time_dim['tpep_pickup_datetime'].dt.month
    date_time_dim['pick_year']=date_time_dim['tpep_pickup_datetime'].dt.year
    date_time_dim['pick_day_of_week']=date_time_dim['tpep_pickup_datetime'].dt.weekday
    date_time_dim['drop_hour']=date_time_dim['tpep_dropoff_datetime'].dt.hour
    date_time_dim['drop_day']=date_time_dim['tpep_dropoff_datetime'].dt.day
    date_time_dim['drop_month']=date_time_dim['tpep_dropoff_datetime'].dt.month
    date_time_dim['drop_year']=date_time_dim['tpep_dropoff_datetime'].dt.year
    date_time_dim['drop_day_of_week']=date_time_dim['tpep_dropoff_datetime'].dt.weekday

    date_time_dim['datetime_id']=date_time_dim.index
    date_time_dim[['datetime_id','tpep_pickup_datetime','pick_hour','pick_day','pick_month','pick_year','pick_day_of_week','tpep_dropoff_datetime','drop_hour','drop_day','drop_month','drop_year','drop_day_of_week']]

    #creating passenger count dimentional table
    passenger_count_dim=data[['passenger_count']]
    passenger_count_dim['passenger_count_id']=passenger_count_dim.index
    passenger_count_dim[['passenger_count_id','passenger_count']]

    # creating trip distance dimentional table
    trip_distance_dim=data[['trip_distance']]
    trip_distance_dim['trip_distance_id']=trip_distance_dim.index
    trip_distance_dim[['trip_distance_id','trip_distance']]

    #creating pick up location dimentional table 
    pickup_location_dm=data[['pickup_longitude','pickup_latitude']]
    pickup_location_dm['pickup_location_id']=data.index
    pickup_location_dm[['pickup_location_id','pickup_longitude','pickup_latitude']]

    #creating dropoff location dimentional table 
    dropof_location_dm=data[['dropoff_longitude','dropoff_latitude']]
    dropof_location_dm['dropoff_location_id']=data.index
    dropof_location_dm[['dropoff_location_id','dropoff_longitude','dropoff_latitude']]

    #creating rate code dimentional table
    rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
    }
    rate_code_dim=data[['RatecodeID']]
    rate_code_dim['ratecode_id']=data.index
    rate_code_dim['rate_code_name']=rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim[['ratecode_id','RatecodeID','rate_code_name']]

    # create payment type dimentional table 
    payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
    }

    payment_type_dim=data[['payment_type']]
    payment_type_dim['payment_type_id']=payment_type_dim.index
    payment_type_dim['payment_type_name']=payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

    #creating the fact table 
    fact_table=data.merge(date_time_dim,left_on='trip_id',right_on='datetime_id')\
    .merge(pickup_location_dm,left_on='trip_id',right_on='pickup_location_id')\
    .merge(dropof_location_dm,left_on='trip_id',right_on='dropoff_location_id')\
    .merge(rate_code_dim,left_on='trip_id',right_on='ratecode_id')\
    .merge(trip_distance_dim,left_on='trip_id',right_on='trip_distance_id')\
    .merge(payment_type_dim,left_on='trip_id',right_on='payment_type_id')\
    .merge(passenger_count_dim,left_on='trip_id',right_on='passenger_count_id')[['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
                'trip_distance_id', 'ratecode_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
                'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                'improvement_surcharge', 'total_amount']]

    #print(fact_table.head())
    # to return all the dataframes we have to convert them as dictionary with key as dataframe name and vale as dictionary

    return {
        "date_time_dim":date_time_dim.to_dict(orient='dict'),
        "passenger_count_dim":passenger_count_dim.to_dict(orient='dict'),
        "trip_distance_dim":trip_distance_dim.to_dict(orient='dict'),
        "pickup_location_dm":pickup_location_dm.to_dict(orient='dict'),
        "dropof_location_dm":dropof_location_dm.to_dict(orient='dict'),
        "rate_code_dim":rate_code_dim.to_dict(orient='dict'),
        "payment_type_dim":payment_type_dim.to_dict(orient='dict'),
        "fact_table":fact_table.to_dict(orient='dict')

    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
