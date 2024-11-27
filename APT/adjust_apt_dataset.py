import pandas as pd
import math

# import input files
apt_input_df = pd.read_excel(io='input_files/APT_data-info-dictionary_final.xlsx',
                             sheet_name='Data revised 25.09')
indicators_df = pd.read_excel(io='input_files/APT_data-info-dictionary_final.xlsx',
                              sheet_name='meta_indicators')

# Adjust date column. Some dates are datetime format and others only display the year. I'm going to create a new
# column with only years.
date_list = []
for dt in apt_input_df.Date:
    try:
        dt_year = dt.year
    except (ValueError, TypeError, AttributeError):
        if math.isnan(dt):
            dt_year = None
        else:
            dt_year = float(dt)
    date_list.append(dt_year)
apt_input_df['Year'] = date_list

# Adjust Indicator column. Remove leading and trailing spaces
apt_input_df['Indicator'] = apt_input_df['Indicator'].str.strip()

# Create df with unique regions/countries:
unique_country = apt_input_df[['Region', 'Country']].drop_duplicates()

# Create array with dates
all_years_df = pd.DataFrame([x for x in range(1984, 2025)], columns=['Year'])

# Merge with unique countries. Create base df for analysis with all possible combinations
base_df_years_country = unique_country.assign(key=1).merge(all_years_df.assign(key=1), on='key').drop('key', axis=1)
base_df = base_df_years_country.assign(key=1).merge(indicators_df.assign(key=1), on='key').drop('key', axis=1)

# Merge with data
expanded_df = base_df.merge(apt_input_df, on=['Region', 'Country', 'Indicator', 'Year'], how='left')
expanded_df.sort_values(by=['Region', 'Country', 'Hierarchy', 'Indicator', 'Year'], inplace=True)
expanded_df.to_csv('output_files/expanded_df.csv', index=False)

# repeat values once indicator = Yes or Partially
expanded_df['new_input'] = expanded_df.groupby(['Region', 'Country', 'Hierarchy', 'Indicator']
                                               )['Input'].ffill()
expanded_df.drop(columns=['Input', 'Date'], inplace=True)
expanded_df.rename(columns={'new_input': 'Input'}, inplace=True)

# adjust NA values
expanded_df.fillna(value={'Input': 'No'}, inplace=True)

# filter only rows with years in [1984, 1994, 2004, 2014, 2024]
expanded_df = expanded_df[expanded_df.Year.isin([1984, 1994, 2004, 2014, 2024])]

# export expanded df
expanded_df.to_csv('output_files/final_apt_df.csv', index=False)
