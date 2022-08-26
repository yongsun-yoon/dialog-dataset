# 모두의 말뭉치 메신저 데이터

import json
from glob import glob
from tqdm import tqdm

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--sep_ord', type=int, default=1000)
args = parser.parse_args()


def main(args):
    f = open(f'modu-messenger.txt', 'w')

    fpaths = glob('modu-messenger/*.json')
    for fp in tqdm(fpaths):
        data = json.load(open(fp, 'r'))
        for doc in data['document']:
            speaker_id, dialog, utter = None, [], []
            for u in doc['utterance']:
                if speaker_id is not None and speaker_id != u['speaker_id']:
                    dialog.append(' '.join(utter))
                    utter = []
                utter.append(u['form'])
                speaker_id = u['speaker_id']

            dialog = chr(args.sep_ord).join(dialog) 
            f.write(dialog)
            f.write('\n')
    
    f.close()



if __name__ == '__main__':
    main(args)