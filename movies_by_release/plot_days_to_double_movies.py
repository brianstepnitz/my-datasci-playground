import pandas
import matplotlib.pyplot as plt

def main():
    datafile_name = "movies_by_release/data/counts_by_releasedate.csv"
    dataframe = pandas.read_csv(datafile_name, parse_dates=['release_date', 'doubling_date'])

    start_date = dataframe['release_date'].min()
    end_date = dataframe['release_date'].max()
    all_dates = pandas.date_range(start_date, end_date)

    dataframe_all = pandas.DataFrame(all_dates, columns=['release_date'])
    dataframe_all = pandas.merge(dataframe_all, dataframe, how='left')
    dataframe_all.ffill(inplace=True, limit_area='inside')
    dataframe_all = dataframe_all.dropna()

    plt.figure(figsize=(10, 6))
    plt.plot(dataframe_all['release_date'], (dataframe_all['doubling_date'] - dataframe_all['release_date']).dt.days, marker='o', linestyle='-')
    plt.xlabel('Date')
    plt.ylabel('Days')
    plt.title("Days to Double Movies")
    plt.grid(True)
    plt.show()

if __name__ == '__main__':
    main()