import os
from tqdm import tqdm
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--start_date', type=str, default='2021-01')
parser.add_argument('--end_date', type=str, default='2021-12')
parser.add_argument('--output_dir', type=str, default='reddit')
args = parser.parse_args()


def get_dates(start_date, end_date):
    start_year, start_month = map(int, start_date.split('-'))
    end_year, end_month = map(int, end_date.split('-'))
    year, month = start_year, start_month

    dates = []
    while True:
        dates.append(f'{year}-{month:02d}')
        if (year == end_year) & (month == end_month):
            break

        if month == 12:
            year += 1
            month = 1
        
        else:
            month += 1
            
    return dates

def main(args):
    os.makedirs(args.output_dir, exist_ok=True)
    dates = get_dates(args.start_date, args.end_date)
    for d in tqdm(dates):
        os.system(f'wget https://files.pushshift.io/reddit/comments/RC_{d}.zst -P {args.output_dir}')


if __name__ == '__main__':
    main(args)