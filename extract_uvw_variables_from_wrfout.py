import pandas as pd
import xarray as xr
import numpy as np
import os
from glob import glob

# Function to read the CSV and add columns for multiple netCDF variables
def preprocess_csv(csv_path, var_names):
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['Date(yyyy-MM-dd)'])
    #df = df.drop(columns=['Date(yyyy-MM-dd)'])

    # Crea una colonna per ciascuna variabile con valori NaN
    for var_name in var_names:
        df[var_name] = np.nan
    return df

# Function to read the netCDF, extract the variable, and calculate the mean
def preprocess_netcdf(file_path, var_name):
    ds = xr.open_dataset(file_path)

    # Filter for hours 8-12 and bottom_top=0
    ds_filtered = ds.sel(bottom_top=0, Time=ds.Time.dt.hour.isin(range(8, 13)))

    # Stampa le dimensioni e le variabili disponibili per debugging
    print(f"Variabili disponibili nel dataset: {ds.data_vars}")
    print(f"Coordinate disponibili nel dataset: {ds.coords}")

    # Group by date and calculate daily mean
    daily_mean = ds_filtered.groupby('Time.date').mean('Time')

    # Extract the variable as DataFrame
    data = daily_mean[var_name].to_dataframe().reset_index()
    data['date'] = pd.to_datetime(data['date'])

    return data

# Function to merge the CSV and netCDF data and update the column var_name
def process_netcdf_and_update(file_path, csv_df, var_name):
    netcdf_data = preprocess_netcdf(file_path, var_name)
    
    # Rinominare 'west_east_stag' per allinearlo al CSV 'west_east'
    netcdf_data = netcdf_data.rename(columns={'west_east_stag': 'west_east'})
    # netcdf_data = netcdf_data.rename(columns={'south_north_stag': 'south_north'})

    # Perform merge
    csv_df = pd.merge(csv_df, netcdf_data[[var_name, 'date', 'south_north', 'west_east']],
                      on=['date', 'south_north', 'west_east'],
                      how='left',
                      suffixes=('', '_new'))

    # Update the column var_name with new values, keeping old ones where there are no updates
    csv_df[var_name] = csv_df[f'{var_name}_new'].fillna(csv_df[var_name])
    csv_df = csv_df.drop(columns=[f'{var_name}_new'])

    return csv_df

# Function to process all netCDF files in a folder for multiple variables
def process_all_netcdf(folder_path, csv_df, var_names):
    file_pattern = os.path.join(folder_path, 'wrfout_d01_*.nc')
    file_list = glob(file_pattern)
    total_files = len(file_list)

    # Itera su tutte le variabili da processare
    for var_name in var_names:
        for i, file in enumerate(file_list, 1):
            print(f"Processing file {i}/{total_files} for variable {var_name}: {file}")
            csv_df = process_netcdf_and_update(file, csv_df, var_name)

    return csv_df

# Main execution
var_names = ['U', 'V', 'W']  # Lista di variabili da processare
csv_path = <CSV_PATH>  # Percorso del file CSV originale
df = preprocess_csv(csv_path, var_names)

# Folder containing netCDF files
netcdf_folder = <NECDF_FOLDER>
result = process_all_netcdf(netcdf_folder, df, var_names)

# Save the final result
output_file = <OUTPUT_CSV_PATH>
result.to_csv(output_file, index=False)

print(f"Elaborazione completata e file salvato come '{output_file}'.")
