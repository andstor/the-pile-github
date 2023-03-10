from tqdm import tqdm
import io
import zstandard as zstd
from pathlib import Path
import json
import argparse
from utils import read_lines_from_zst_file



def parse_args():
    parser = argparse.ArgumentParser(description="Finetune a transformers model on a causal language modeling task")
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        required=True,
        help="The name of the dataset to use (via the datasets library).",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        required=True,
        default="output",
        help="The output directory where the filtered data will be saved.",
    )
    parser.add_argument(
        "--subset",
        type=str,
        required=True,
        default="Github",
        help="The subset to filter.",
    )

    args = parser.parse_args()

    return args


def main():
    args = parse_args()


    file = Path(args.file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    output_file = Path(output_dir) / file.name

    records = map(json.loads, read_lines_from_zst_file(file))

    DCTX = zstd.ZstdDecompressor(max_window_size=2**31)
    with zstd.open(output_file, mode='w', dctx=DCTX) as writer:
        for record in tqdm(records):
            meta = record['meta']
            if meta['pile_set_name'] == args.subset:
                text = record['text']
                data = {'text': text}
                data = json.dumps(data)
                writer.write(data + '\n')


if __name__ == '__main__':
    main()