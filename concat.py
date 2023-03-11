import pandas as pd
from glob import glob
from pathlib import Path

for i in range(0, 30):
    languages = {}
    print(f"Processing data{i:02d}")
    globbed_files = glob(f"data{i:02d}/**/*.parquet", recursive=True)

    # sort 
    #globbed_files.sort( key = lambda x: x.split('/')[0])
    globbed_files.sort()
    print(globbed_files)
    for file in globbed_files:
        path = Path(file)
        language = path.parent.parent.name
        languages.setdefault(language, [])
        languages[language].append(file)

    for language, files in languages.items():
        print(language, len(files))
        df = pd.concat([pd.read_parquet(file) for file in files])
        print(df.shape)
        # mkdir
        save_path = Path(f"data/{language}/train")
        save_path.mkdir(exist_ok=True, parents=True)
        df.to_parquet(save_path / f"part.{i}.parquet")
