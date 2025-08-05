import os
from glob import glob
import shutil
import time

PROCESSED_PATH = 'datalake/processed/'
WAREHOUSE_PATH = 'datawarehouse/'

def init_datawarehouse():
    os.makedirs(WAREHOUSE_PATH, exist_ok=True)

    while True:
        parquet_files = glob(os.path.join(PROCESSED_PATH, '*.parquet'))

        if not parquet_files:
            time.sleep(1)
            continue

        for file in parquet_files:
            try:
                dest = os.path.join(WAREHOUSE_PATH, os.path.basename(file))
                shutil.copy2(file, dest)
                print(f'✅ Arquivo movido para Data Warehouse: {dest}')
                os.remove(file)
                print(f'🗑️ Arquivo Parquet removido do Processed: {file}')
            except Exception as e:
                print(f'❌ Erro ao mover {file}: {e}')

        time.sleep(1) 

if __name__ == '__main__':
    init_datawarehouse()
