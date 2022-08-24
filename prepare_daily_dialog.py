from tqdm import tqdm
from datasets import load_dataset

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--split', type=str, default='train')
parser.add_argument('--sep_ord', type=int, default=1000)
args = parser.parse_args()


def main(args):
    dataset = load_dataset(path='daily_dialog', split=args.split)
    output_file = f'daily_dialog_{args.split}.txt'

    fs = open(output_file, 'a')
    for data in tqdm(dataset):
        dialog = data['dialog']
        dialog = chr(args.sep_ord).join(dialog)
        fs.write(dialog)
        fs.write('\n')
    
    fs.close()

if __name__ == '__main__':
    main(args)