import os
import csv
from datetime import date
from collections import Counter

def main():
    datadir_name = "movies_by_release/data"

    date_counter = Counter()
    for filename in os.listdir(datadir_name):
        if filename.endswith(".csv"):
            with open(f"{datadir_name}/{filename}", encoding='utf-8') as f:
                csvreader = csv.reader(f)
                next(csvreader) # Eat the header row.
                for row in csvreader:
                    d = date.fromisoformat(row[0])
                    date_counter[d] += 1

    with open(f"{datadir_name}/counts_by_releasedate.csv", mode='w', newline='') as f:
        csvwriter = csv.writer(f)
        sum = 0
        for item in sorted(date_counter.items()):
            count = item[1]
            csvwriter.writerow([item[0], item[1], sum])
            sum += count
    
    print("Done!")

if __name__ == "__main__":
    main()