import os
import pandas as pd
from mage_ai.io.file import FileIO
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_file(*args, **kwargs):
    """
    Template for loading data from filesystem.
    Load data from 1 file or multiple file directories.

    For multiple directories, use the following:
        FileIO().load(file_directories=['dir_1', 'dir_2'])

    Docs: https://docs.mage.ai/design/data-loading#fileio
    """
    filepath = 'C:\\Users\\kumar\\Desktop\\Code\\ETL\\mage-project\\Data\\'

    ## plant 1
    df_plant1_gen = pd.read_csv(filepath + 'Plant_1_Generation_Data.csv')
    df_plant1_weather = pd.read_csv(filepath + 'Plant_1_Weather_Sensor_Data.csv')


    # Return the loaded DataFrames in a dictionary
    return {
        'df_plant1_gen': df_plant1_gen.to_json(orient='records'),
        'df_plant1_weather': df_plant1_weather.to_json(orient='records')
    }
    # return {'df_plant1_gen': df_plant1_gen,'df_plant1_weather': df_plant1_weather}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
