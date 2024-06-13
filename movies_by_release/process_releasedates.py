import os
import csv
from datetime import date
from collections import Counter

def count_releasedates(datadir_name):
    date_counter = Counter()
    for filename in os.listdir(datadir_name):
        if filename.startswith("movies_from_") and filename.endswith(".csv"):
            with open(f"{datadir_name}/{filename}", encoding='utf-8') as f:
                csvreader = csv.reader(f)
                next(csvreader) # Eat the header row.
                for row in csvreader:
                    d = date.fromisoformat(row[0])
                    date_counter[d] += 1
    
    return date_counter

def calc_doublingdates(date_counter):
    sum = 0
    entries = []
    cursor = 0
    for item in sorted(date_counter.items()):
        count = item[1]
        sum += count
        entry = {'release_date': item[0], 'count': count, 'sum': sum}
        entries.append(entry)
        while entry['sum'] >= 2 * entries[cursor]['sum']:
            entries[cursor]['doubling_date'] = entry['release_date']
            cursor += 1

    return entries

def write_entries(outfile_name, entries):
    with open(outfile_name, mode='w', newline='') as f:

        csvwriter = csv.DictWriter(
            f,
            fieldnames=["release_date", "count", "sum", "doubling_date"])
        csvwriter.writeheader()
        for entry in entries:
            csvwriter.writerow(entry)

def main():
    datadir_name="movies_by_release/data"
    date_counter = count_releasedates(datadir_name)

    entries = calc_doublingdates(date_counter)

    write_entries(
        f"{datadir_name}/counts_by_releasedate.csv",
        entries)

if __name__ == "__main__":
    main()