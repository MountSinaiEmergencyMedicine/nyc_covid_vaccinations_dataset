import glob
import pandas as pd


def collect_excel_files(dataset_name, data_directory):
    files = glob.glob(
        f"{data_directory}/*{[dataset_name]}*.xlsx")
    df_list = []
    for f in files:
        if dataset_name.lower() in f.lower().replace(' ', '_'):
            df = pd.read_excel(f)
            df['date'] = f.split('.xlsx')[0][-10:]
            df.columns = [
                c.lower().replace(
                    ' ', '_') for c in df.columns]
            df.rename(columns={
                'n_partially_vaccinated_cumulative': 'at_least_1_dose',
                'perc_partially': 'perc_at_least_1_dose'}, inplace=True)
            df_list.append(df)
    print('    ...concating excel files')
    data = pd.concat(df_list, sort=False)
    data.columns = [
        c.lower().replace(
            ' ', '_') for c in data.columns]
    data.rename(columns={
        'indicator': 'sum_indicator',
        'perc_at_least_1_dose': 'sum_perc_at_least_1_dose',
        'at_least_1_dose': 'sum_at_least_1_dose',
        'n_fully_vaccinated_cumulative': 'sum_n_fully_vaccinated_cumulative',
        'perc_fully': 'sum_perc_fully',
        'population_estimate': 'sum_population_estimate',
    }, inplace=True)
    return data


def collect_csv_files(dataset_name, data_directory):
    files = glob.glob(
        f"{data_directory}/*{dataset_name}*.csv")
    df_list = []
    for f in files:
        df = pd.read_csv(f)
        df_list.append(df)
    print('    ...concating csv files')
    data = pd.concat(df_list, sort=False)
    return data


def main():
    data_directory = '/Users/danielmsheehan/Dropbox/data/municipal/usa/ny/new_york_city'
    excel_directory = f'{data_directory}/covid_vaccinations/_manual_data'
    csv_directory = f'{data_directory}/covid_vaccinations'

    data_list = ['map_zip']
    # ToDo - if needed ['agepriority_raceeth', 'racepriority_raceeth', 'sexpriority_raceeth', 'date']

    for dataset_name in data_list:
        print(dataset_name)
        csv_df = collect_csv_files(dataset_name, csv_directory)
        print(csv_df.shape)

        excel_df = collect_excel_files(dataset_name, excel_directory)
        print(excel_df.shape)

        df = pd.concat([csv_df, excel_df], sort=True)

        df['date'] = pd.to_datetime(df['date'])

        df = df.sort_values(['date', 'zcta_num'])

        df = df[[
            'date', 'zcta_num',
            'sum_at_least_1_dose', 'sum_indicator',
            'sum_n_fully_vaccinated_cumulative', 'sum_perc_at_least_1_dose',
            'sum_perc_fully', 'sum_population_estimate']]

        df.drop_duplicates(inplace=True)

        df.to_csv('data/covid_map_zip.csv', index=False)


if __name__ == '__main__':
    main()
