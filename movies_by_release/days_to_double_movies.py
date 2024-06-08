import csv
from datetime import date, timedelta

def main():
    datafile_name = "movies_by_release/data/counts_by_releasedate.csv"
    events = []
    with open(datafile_name) as d:
        csvreader = csv.reader(d)
        for row in csvreader:
            events.append({'date': date.fromisoformat(row[0]), 'count': int(row[1]), 'sum': int(row[2])})

    oneday = timedelta(days=1)
    outfile_name = "movies_by_release/data/days_to_double_movies.csv"
    with open(outfile_name, mode='w', newline='') as o:
        csvwriter = csv.writer(o)
        for index, event in enumerate(events):
            curr_index = index
            while events[curr_index]['sum'] < (event['sum'] * 2) and (curr_index+1) < len(events):
                curr_index += 1
            
            if events[curr_index]['sum'] >= (event['sum'] * 2):
                # Now curr_index is on the event for which its sum is double the sum of the original event.
                doubling_date = events[curr_index]['date']
                days_until_doubling = doubling_date - event['date']

                if (index+1) < len(events):
                    next_date = events[index+1]['date']
                    curr_date = event['date']
                    while curr_date < next_date:
                        csvwriter.writerow([curr_date, days_until_doubling.days, doubling_date])
                        curr_date += oneday
    print("Done!")

if __name__ == "__main__":
    main()