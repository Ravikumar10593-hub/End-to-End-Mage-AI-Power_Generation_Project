import pandas as pd
import json
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
    # Extract the 'df_plant2_gen' and 'df_plant2_weather' lists
    df_plant2_gen_list = json.loads(data['df_plant2_gen'])
    df_plant2_weather_list = json.loads(data['df_plant2_weather'])

    # Create DataFrames
    df_plant2_gen = pd.DataFrame(df_plant2_gen_list)
    df_plant2_weather = pd.DataFrame(df_plant2_weather_list)

    df_plant2_gen['DATE_TIME'] = pd.to_datetime(df_plant2_gen['DATE_TIME'],format="%Y-%m-%d %H:%M:%S")
    df_plant2_weather['DATE_TIME'] = pd.to_datetime(df_plant2_weather['DATE_TIME'],format="%Y-%m-%d %H:%M:%S")

    ## Some Interesting numbers to see:

    ## from dataset 1 (Generation Data):

    # 1. Efficiency of the inverter in converting DC power to AC power. A higher efficiency is desirable
    df_plant2_gen['INVERTER_EFFICIENCY'] = df_plant2_gen['AC_POWER'] / df_plant2_gen['DC_POWER']

    # 2. Total AC power produced by the plant on a daily basis. It helps track daily performance and energy output.
    df_plant2_gen['DAILY_POWER_PRODUCTION_AC'] = df_plant2_gen.groupby(df_plant2_gen['DATE_TIME'].dt.date)['AC_POWER'].transform('sum')

    # 3. Total DC power produced by the plant on a daily basis. It helps track daily performance and energy output.
    df_plant2_gen['DAILY_POWER_PRODUCTION_DC'] = df_plant2_gen.groupby(df_plant2_gen['DATE_TIME'].dt.date)['DC_POWER'].transform('sum')

    # 4. How well the inverter is performing relative to its peak capacity during the day (AC).
    df_plant2_gen['AC_PERFORMANCE_RATIO'] = df_plant2_gen['AC_POWER'] / df_plant2_gen.groupby(df_plant2_gen['DATE_TIME'].dt.date)['AC_POWER'].transform('max')

    # 5. How well the inverter is performing relative to its peak capacity during the day (DC).
    df_plant2_gen['DC_PERFORMANCE_RATIO'] = df_plant2_gen['DC_POWER'] / df_plant2_gen.groupby(df_plant2_gen['DATE_TIME'].dt.date)['DC_POWER'].transform('max')

    # 6. Increase in power generation compared to the previous 15-minute interval. It helps identify performance trends throughout the day.
    df_plant2_gen['DAILY_INCREMENTAL_YIELD'] = df_plant2_gen['DAILY_YIELD'].diff().fillna(0)

    ## from dataset 2 (Sensor Data):

    # 1. Difference between the temperature of the solar module and the ambient temperature. It provides insight into temperature variations affecting module performance.
    df_plant2_weather['TEMP_DIFFERENCE'] = df_plant2_weather['MODULE_TEMPERATURE'] - df_plant2_weather['AMBIENT_TEMPERATURE']

    # 2. Sums up the daily irradiation values, indicating the total sunlight exposure. It's useful for understanding daily sunlight patterns.
    df_plant2_weather['DAILY_IRRADIATION_TOTAL'] = df_plant2_weather.groupby(df_plant2_weather['DATE_TIME'].dt.date)['IRRADIATION'].transform('sum')

    # 3. Sums up the daily irradiation values, indicating the total sunlight exposure. It's useful for understanding daily sunlight patterns.
    df_plant2_weather['DAILY_IRRADIATION_TOTAL'] = df_plant2_weather.groupby(df_plant2_weather['DATE_TIME'].dt.date)['IRRADIATION'].transform('sum')


    
    combined_plant2_df = pd.merge(df_plant2_gen, df_plant2_weather[['DATE_TIME', 'AMBIENT_TEMPERATURE',
       'MODULE_TEMPERATURE', 'IRRADIATION','TEMP_DIFFERENCE',
       'DAILY_IRRADIATION_TOTAL']], on='DATE_TIME', how='inner')

    combined_plant2_df['DATE_TIME'] = combined_plant2_df['DATE_TIME'].dt.strftime('%Y-%m-%d %H:%M:%S') 

    return combined_plant2_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
