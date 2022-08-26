# AI Hub 감성 대화 말뭉치
# https://aihub.or.kr/aihubdata/data/view.do?currMenu=115&topMenu=100&aihubDataSe=realm&dataSetSn=86

import json
from glob import glob
from tqdm import tqdm

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--split', type=str, default='train')
parser.add_argument('--sep_ord', type=int, default=1000)
args = parser.parse_args()


def main(args):
    f = open(f'emotional-dialog-{args.split}.txt', 'w')

    data = json.load(open(f'emotional-dialog-{args.split}.json', 'r'))
    for d in tqdm(data):
        dialog = [v for v in d['talk']['content'].values() if v]
        dialog = chr(args.sep_ord).join(dialog) 
        f.write(dialog)
        f.write('\n')
    
    f.close()



if __name__ == '__main__':
    main(args)