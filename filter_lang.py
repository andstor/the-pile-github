import multiprocessing
import os
import time
from multiprocessing import Pool, Queue, Process, Manager
import random
from tqdm import tqdm
from collections import Counter
import multiprocessing
import multiprocessing.util
import logging
from threading import Thread
import pickle
import argparse
from pathlib import Path
import ast
import sys
import pandas as pd
import subprocess
import os
import json
from utils import read_lines_from_zst_file
from guesslang import Guess


#multiprocessing.util._logger = multiprocessing.util.log_to_stderr(logging.DEBUG)
#logging.getLogger('transformers').setLevel(logging.ERROR)
logging.getLogger('guesslang').setLevel(logging.ERROR)

def parse_args():
    parser = argparse.ArgumentParser(description="Filter a dataset by programming language")
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        required=True,
        help="The jsonl.zst file to filter.",
    )
    parser.add_argument(
        "--text_column_name",
        type=str,
        default="text",
        help="The name of the text column in the dataset.",
    )
    parser.add_argument(
        "--num_proc",
        type=int,
        default=1,
        help="The number of processes to use for data loading.",
    )
    parser.add_argument(
        "--max_samples",
        type=int,
        default=None,
        help="The number of sub samples to use for data loading.",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="data",
        help="The output directory where the trivial tokens will be saved.",
    )
    parser.add_argument(
        "--split_name",
        type=str,
        default="train",
        help="The name of the data split.",
    )
    parser.add_argument(
        "--chunk_size",
        type=float,
        default=256,
        help="The max accumulated memory size before writing to disk (in MB). Default is 1GB.",
    )

    parser.set_defaults
    args = parser.parse_args()

    return args


def producer_main(que_in, args):
    print(os.getpid(),"starting dataloader")
    #datasets = load_dataset(args.dataset_name, args.dataset_config_name, split=args.split_name, streaming=args.stream, revision=args.revision)
    #dataset = iter(datasets)
    records = map(json.loads, read_lines_from_zst_file(args.file))

    for i, row in enumerate(records):
        if args.max_samples is not None and i > args.max_samples:
            break
        que_in.put(row)

    for i in range(args.num_proc):
        que_in.put(None)

def worker_main(queue_in, queue_out, args):
    print(os.getpid(),"starting worker")
    guess = Guess()
    while True:
        row = queue_in.get(block=True) #block=True means make a blocking call to wait for items in queue
        if row is None:
            break
        #res = func(tmpf, row)
        lang = guess.language_name(row[args.text_column_name])
        data = {
            "text": row[args.text_column_name],
            "meta": {"language": lang}
        }
        queue_out.put(data)
    queue_out.put(None)

def main():
    args = parse_args()
    print(args)
    with Manager() as manager:

        que_in = manager.Queue(maxsize=args.num_proc*10)
        que_out = manager.Queue(maxsize=args.num_proc*10)
        
        # Start producer
        producer = Process(target=producer_main, args=(que_in, args))
        producer.start()

        workers = [Process(target=worker_main, args=(que_in, que_out, args)) for _ in range(args.num_proc)]
        for w in workers:
            w.start()
        
        
        pbar = tqdm(total=args.max_samples)
        #Collect results

        lang_datasets = {}
        sizes = {}

        #for i in range(args.max_samples):
        exit_signals = 0
        while exit_signals < args.num_proc:
            item = que_out.get()
            if item is None:
                exit_signals += 1
            else:
                pbar.update(1)
                
                meta = item["meta"]
                lang = str(meta["language"])
                size = sys.getsizeof(item["text"])

                lang_datasets.setdefault(lang, [])
                lang_datasets[lang].append(item)
                sizes.setdefault(lang, 0)
                sizes[lang] += size
                
                if sizes[lang] >= args.chunk_size * 1000000:
                    output_dir = Path(args.output_dir) / lang / args.split_name
                    output_dir.mkdir(parents=True, exist_ok=True)

                    part = len(list(output_dir.glob("*.parquet")))
                    output_file = output_dir / f"part.{part}.parquet"

                    print(f"Saving {lang} part {part}")
                    df = pd.DataFrame(lang_datasets[lang])
                    df.to_parquet(output_file)
                    lang_datasets[lang] = []
                    sizes[lang] = 0

                    pbar.set_description("")
        pbar.close()
        
        


        # prevent adding anything more to the queue and wait for queue to empty
        #que_in.close()
        #que_in.join_thread()

        #que_out.close()
        #que_out.join_thread()

        # Wait for producer to finish
        producer.join()

        for w in workers:
            w.join()

        # prevent adding anything more to the process pool and wait for all processes to finish

if __name__ == '__main__':
    main()

#Total samples: 214584it [18:35, 192.35it/s]
#Subset samples: 18195it [18:45, 16.17it/s]]