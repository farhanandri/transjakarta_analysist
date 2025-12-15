import pandas as pd
from sqlalchemy import create_engine
import os

# String koneksi NeonDB
NEON_CONNECTION_STRING = "postgresql://neondb_owner:npg_fA4yl3uQjaEe@ep-calm-voice-a1vdf20y-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# Skeman
TARGET_SCHEMA = 'new_model' 

# Direktori data new model
SOURCE_DIR = '/opt/airflow/data/tj_new_model/'

# Pemetaan nama file CSV ke nama tabel target di skema new_model
FILE_TO_TABLE_MAPPING = {
    'dim_date.csv': 'dim_date',
    'dim_time.csv': 'dim_time',
    'dim_halte.csv': 'dim_halte', 
    'dim_corridor.csv': 'dim_corridor',
    'dim_card.csv': 'dim_card',
    'fact_transaction.csv': 'fact_transaction'
}

def main_load_new_model_callable():
    
    # Membuat koneksi
    engine = create_engine(NEON_CONNECTION_STRING)
    
    total_loaded_tables = 0
    
    print(f"Memulai proses load data ke skema: {TARGET_SCHEMA}...")

    # Loop melalui setiap file dan load ke tabel yang sesuai
    for file_name, target_table in FILE_TO_TABLE_MAPPING.items():
        source_path = os.path.join(SOURCE_DIR, file_name)
        
        # --- BLOK 1: BACA FILE DENGAN FILE NOT FOUND HANDLER ---
        try:
            # Baca data dengan penanganan parse_dates (PENTING untuk tipe data)
            if 'fact' in target_table:
                 # Fact memiliki 4 kolom datetime/date baru (Date_In/Out & DateTime_In/Out)
                 df = pd.read_csv(source_path, parse_dates=['Date_In', 'Date_Out', 'DateTime_In', 'DateTime_Out'])
            elif 'date' in target_table:
                 # Dim Date hanya FullDate sebagai PK
                 df = pd.read_csv(source_path, parse_dates=['FullDate'])
            else:
                 df = pd.read_csv(source_path)
                 
            print(f"Berhasil membaca {len(df)} baris data dari {file_name}.")
            
        except FileNotFoundError:
            print(f"ERROR: File {source_path} tidak ditemukan. Melewati tabel ini.")
            continue # Lanjut ke file berikutnya jika satu file hilang
        
        # --- BLOK 2: LOAD KE DATABASE DENGAN ERROR HANDLER ---
        try:
            # Memuat data ke SQL (PENTING: schema=TARGET_SCHEMA)
            df.to_sql(
                target_table, 
                engine, 
                if_exists='replace', # untuk overwrite
                index=False,
                schema=TARGET_SCHEMA, # Mengarahkan ke skema 'new_model'
                method='multi'
            )
            print(f"SUCCESS: Data untuk tabel '{TARGET_SCHEMA}.{target_table}' berhasil di-load (OVERWRITE).")
            total_loaded_tables += 1
        except Exception as e:
            print(f"Gagal memuat data ke NeonDB untuk tabel {target_table} di skema {TARGET_SCHEMA}: {e}")
            raise # Raise exception untuk kegagalan database
            
    print(f"\nProses Load Selesai. Total {total_loaded_tables} tabel berhasil di-load ke skema {TARGET_SCHEMA}.")


if __name__ == "__main__":
    main_load_new_model_callable()