import pandas as pd
import os
from glob import glob
import time

RAW_PATH = 'datalake/raw/'
PROCESSED_PATH = 'datalake/processed/'

def convert_json_to_parquet():
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    while True:
        json_files = glob(os.path.join(RAW_PATH, '*.json'))

        if not json_files:
            time.sleep(10)
            continue

        for file in json_files:
            try:
                df = pd.read_json(file)
            except ValueError:
                try:
                    df = pd.read_json(file, lines=True)
                except ValueError:
                    import json
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, dict):
                        df = pd.DataFrame([data])
                    else:
                        print(f'⚠️ Formato de JSON não suportado: {file}')
                        continue
            
            df = df.astype(str)

            estado = 'unknown'
            if 'sigla' in df:
                val = df['sigla'].iloc[0]
                if isinstance(val, str):
                    estado = val
            out_path = os.path.join(PROCESSED_PATH, f'{estado}_{os.path.splitext(os.path.basename(file))[0]}.parquet')

            try:
                df.to_parquet(out_path)
                print(f'✅ Convertido: {file} -> {out_path}')
                os.remove(file)
                print(f'🗑️ Arquivo JSON removido: {file}')
            except Exception as e:
                print(f'❌ Erro ao converter {file}: {e}')
        
        time.sleep(1)

if __name__ == '__main__':
    convert_json_to_parquet()
