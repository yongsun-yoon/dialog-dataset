# AI Hub 한국어 SNS 데이터셋 전처리
# https://aihub.or.kr/aihubdata/data/view.do?currMenu=115&topMenu=100&aihubDataSe=realm&dataSetSn=114
import json
from glob import glob
from tqdm import tqdm

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--split', type=str, default='train')
parser.add_argument('--sep_ord', type=int, default=1000)
args = parser.parse_args()


def main(args):
    f = open(f'korean-sns-{args.split}.txt', 'w')

    fpaths = glob(f'korean-sns-{args.split}/*.json')
    for fp in fpaths:
        data = json.load(open(fp, 'r'))['data']
        for d in tqdm(data):
            body = d['body']

            pid, utter, dialog = None, [], []
            for b in body:
                if pid is not None and pid != b['participantID']:
                    dialog.append(' '.join(utter))
                    utter = []
                utter.append(b['utterance'])
                pid = b['participantID']
            dialog.append(' '.join(utter))

            dialog = chr(args.sep_ord).join(dialog) 
            f.write(dialog)
            f.write('\n')

        print(f'{fp} done')
    
    f.close()



if __name__ == '__main__':
    main(args)