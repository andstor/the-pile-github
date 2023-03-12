from glob import glob
from pathlib import Path
import pandas as pd

mem_threshold = 500


#[f.split("/")[1] for f in globbed_files]
lang_dirs = glob(f"data/*", recursive=True)
for lang_dir in lang_dirs:
    print(f"Processing {lang_dir}")
    lang = lang_dir.split("/")[1]
    files = glob(f"{lang_dir}/train/*.parquet", recursive=True)
    res_df = None
    for i in range(0, 30):
        if res_df is None:
            res_df = pd.read_parquet(files[i])
        else:
            df = pd.read_parquet(files[i])
            res_df = pd.concat([res_df, df], ignore_index=True)
        
        if res_df.memory_usage(deep=True).sum() > mem_threshold * 1000000:
            print(f"Saving {lang}")
            output_dir = Path("output") / lang / "train"

            part = len(list(output_dir.glob("*.parquet")))
            output_dir.mkdir(exist_ok=True, parents=True)
            output_file = output_dir / f"part.{part}.parquet"

            res_df.to_parquet(output_file)
            res_df = None
    
    if res_df is not None:
        print(f"Saving {lang}")
        output_dir = Path("output") / lang / "train"

        part = len(list(output_dir.glob("*.parquet")))
        output_dir.mkdir(exist_ok=True, parents=True)
        output_file = output_dir / f"part.{part}.parquet"

        res_df.to_parquet(output_file)

