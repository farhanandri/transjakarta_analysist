import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import datetime
from shapely.geometry import Point
from dateutil.relativedelta import relativedelta

def main_transform_new_model_callable():
    
    # Membaca csv
    df = pd.read_csv('/opt/airflow/data/tj/dfTransjakarta180kRows.csv')

    # Membaca gis jakarta shp
    gdf_jakbar = gpd.read_file("/opt/airflow/gis_jakarta/KOTA_JAKARTA_BARAT/ADMINISTRASIKECAMATAN_AR_25K.shp")
    gdf_jakpus = gpd.read_file("/opt/airflow/gis_jakarta/KOTA_JAKARTA_PUSAT/ADMINISTRASIKECAMATAN_AR_25K.shp")
    gdf_jaktim = gpd.read_file("/opt/airflow/gis_jakarta/KOTA_JAKARTA_TIMUR/ADMINISTRASIKECAMATAN_AR_25K.shp")
    gdf_jakut = gpd.read_file("/opt/airflow/gis_jakarta/KOTA_JAKARTA_UTARA/ADMINISTRASIKECAMATAN_AR_25K.shp")
    gdf_jaksel = gpd.read_file("/opt/airflow/gis_jakarta/KOTA_JAKARTA_SELATAN/ADMINISTRASIKECAMATAN_AR_25K.shp")

    # Membuat def untuk menentukan city
    def get_city(lon, lat):
        point = Point(lon, lat)
        if gdf_jakbar.contains(point).any():
            return 'Jakarta Barat'
        elif gdf_jakpus.contains(point).any():
            return 'Jakarta Pusat'
        elif gdf_jaktim.contains(point).any():
            return 'Jakarta Timur'
        elif gdf_jakut.contains(point).any():
            return 'Jakarta Utara'
        elif gdf_jaksel.contains(point).any():
            return 'Jakarta Selatan'
        return np.nan # Tambahkan ini untuk nilai yang tidak terdeteksi

    # Membuat kolom baru tap in and out city menggunakan lambda
    df["tapInCity"] = df.apply(lambda x: get_city(x.tapInStopsLon, x.tapInStopsLat) ,axis=1)
    df["tapOutCity"] = df.apply(lambda x: get_city(x.tapOutStopsLon, x.tapOutStopsLat) ,axis=1)

    # Drop kolom yang missing
    df.dropna(subset = ['corridorID','corridorName','tapInStops','payAmount'],inplace=True)

    # Mengubah kolom yang valuenya tanggal
    df['tapInTime'] = df['tapInTime'].astype('datetime64[ns]')
    df['tapOutTime'] = df['tapOutTime'].astype('datetime64[ns]')


    # =========================================================================
    # 1. MEMBUAT DIM DATE (PK = FullDate)
    # =========================================================================

    # Mencari tanggal paling awal dari tap in dan tap out
    min_tap_in_date = df['tapInTime'].min().normalize()
    min_tap_out_date = df['tapOutTime'].min().normalize()
    start_date_dt = min(min_tap_in_date, min_tap_out_date)
    start_date_str = start_date_dt.strftime('%Y-%m-%d')

    # Meng generated +5 tahun dari awal tanggal
    end_date_dt = start_date_dt + relativedelta(years=5) - relativedelta(days=1)
    end_date_str = end_date_dt.strftime('%Y-%m-%d')

    # Insert ke kolom full date untuk tanggal
    dim_date_df = pd.DataFrame({
        'FullDate': pd.date_range(start=start_date_str, end=end_date_str, freq='D')
    })

    # Extract Year, Month, Day, DayOfWeek, DayType
    dim_date_df['Year'] = dim_date_df['FullDate'].dt.year
    dim_date_df['Month'] = dim_date_df['FullDate'].dt.month
    dim_date_df['Day'] = dim_date_df['FullDate'].dt.day
    dim_date_df['DayOfWeek'] = dim_date_df['FullDate'].dt.day_name()
    dim_date_df['DayType'] = np.where(dim_date_df['DayOfWeek'].isin(['Saturday', 'Sunday']), 'Weekend', 'Weekday')
    
    # Assign row untuk missing value (UNKNOWN MEMBER)
    unknown_date = pd.DataFrame({
        # FullDate bertindak sebagai PK/Key, jadi harus unik (tidak boleh NaN)
        'FullDate': [pd.to_datetime('1900-01-01')],
        'Year': [np.nan],
        'Month': [np.nan],
        'Day': [np.nan],
        'DayOfWeek': ['NULL_DATE'],
        'DayType': ['UNKNOWN']
    })    
    
    # Menggabungkan missing ke dim date
    dim_date_df = pd.concat([unknown_date, dim_date_df], ignore_index=True)
    
    # Mengubah data tipe
    dim_date_df['FullDate'] = dim_date_df['FullDate'].astype('datetime64[ns]')
    # Menggunakan Int64 (huruf besar I) untuk mendukung NaN pada kolom integer
    dim_date_df['Year'] = dim_date_df['Year'].astype('Int64')
    dim_date_df['Month'] = dim_date_df['Month'].astype('Int64')
    dim_date_df['Day'] = dim_date_df['Day'].astype('Int64')
    
    # Menentukan ID/Key untuk Unknown Member
    UNKNOWN_DATE_KEY = dim_date_df['FullDate'].iloc[0] 
    
    # Mengurutkan kolom (DimDateID Dihapus)
    dim_date_df = dim_date_df[['FullDate', 'Year', 'Month', 'Day', 'DayOfWeek', 'DayType']]
    # Dim Date Selesai


    # =========================================================================
    # 2. MEMBUAT DIM TIME (PK = HourOfDay)
    # =========================================================================
    
    # Membuat variabel place holder untuk jam
    time_data = []
    for h in range(24):
        hour = h
        time_slot = 'Morning Peak (05:00-08:59)' if 5 <= hour < 9 else ('Mid-day Off-Peak (09:00-15:59)' if 9 <= hour < 16 else ('Afternoon Peak (16:00-19:59)' if 16 <= hour < 20 else 'Night/Early Morning (20:00-04:59)'))
        time_data.append([hour, time_slot])
    
    # Menginsert data place holder ke dim_time        
    dim_time_df = pd.DataFrame(time_data, columns=['HourOfDay', 'TimeSlot'])
    
    # Membuat unknown member
    unknown_time_member = pd.DataFrame({
        # HourOfDay bertindak sebagai PK/Key, jadi harus unik (tidak boleh NaN)
        'HourOfDay': [99], # Menggunakan angka unik di luar 0-23
        'TimeSlot': ['NULL_TIME']
    })
    
    dim_time_df = pd.concat([unknown_time_member,dim_time_df], ignore_index=True)
    
    # Mengubah id ke integer
    dim_time_df['HourOfDay'] = dim_time_df['HourOfDay'].astype('int64')

    # ID 99 untuk fact
    UNKNOWN_TIME_KEY = dim_time_df['HourOfDay'].iloc[0]
    
    # Mengurutkan agar ID di depan
    dim_time_df = dim_time_df[['HourOfDay', 'TimeSlot']]
    # Dim Time Selesai

    # =========================================================================
    # 3. MEMBUAT DIM CORRIDOR 
    # =========================================================================

    # Mengambil data CorridorID, CorridorName, direction dari df dan drop_dulicated dan reset indeks
    dim_corridor_df = df[['corridorID', 'corridorName', 'direction']].drop_duplicates(subset=['corridorID']).reset_index(drop=True)
    dim_corridor_df.dropna(subset=['corridorID'], inplace=True)

    # Membuat ID dim_corridor (Surrogate Key)
    dim_corridor_df['DimCorridorID'] = range(1, len(dim_corridor_df) + 1)
    
    # Mengubah tipe data ke integer
    dim_corridor_df['DimCorridorID'] = dim_corridor_df['DimCorridorID'].astype('int64')

    # Mengurutkan ID agar di depan
    dim_corridor_df = dim_corridor_df[['DimCorridorID', 'corridorID', 'corridorName', 'direction']]
    # Dim Corridor Selesai

    # =========================================================================
    # 4. MEMBUAT DIM HALTE
    # =========================================================================

    # Mengcopy data tap in dan tap out
    tap_in_data = df[[
        'tapInStops', 'tapInStopsName', 'tapInStopsLat', 'tapInStopsLon','tapInCity'
    ]].rename(columns={
        'tapInStops': 'StopID',
        'tapInStopsName': 'StopName',
        'tapInStopsLat': 'Latitude',
        'tapInStopsLon': 'Longitude',
        'tapInCity' : 'City'
    })

    tap_out_data = df[[
        'tapOutStops', 'tapOutStopsName', 'tapOutStopsLat', 'tapOutStopsLon','tapOutCity'
    ]].rename(columns={
        'tapOutStops': 'StopID',
        'tapOutStopsName': 'StopName',
        'tapOutStopsLat': 'Latitude',
        'tapOutStopsLon': 'Longitude',
        'tapOutCity' : 'City'
    })

    # Menggabungkan data tap in dan tap out
    dim_halte_df = pd.concat([tap_in_data, tap_out_data]).drop_duplicates(subset=['StopID']).reset_index(drop=True)
    
    # Membuat unknown member
    unknown_member = pd.DataFrame({
        'StopID': ['NULL_KEY'],
        'StopName': [np.nan],
        'Latitude': [np.nan],
        'Longitude': [np.nan],
        'City': [np.nan]
    })        
    
    # Menggabungkan unknown member
    dim_halte_df = pd.concat([unknown_member,dim_halte_df], ignore_index=True)
    
    dim_halte_df.dropna(subset=['StopID'], inplace=True)

    # Membuat ID dim_location (Surrogate Key)
    dim_halte_df['DimHalteID'] = range(0, len(dim_halte_df))
    dim_halte_df['DimHalteID'] = dim_halte_df['DimHalteID'].astype(int)
    dim_halte_df['Latitude'] = dim_halte_df['Latitude'].astype('float64')
    dim_halte_df['Longitude'] = dim_halte_df['Longitude'].astype('float64')
    
    # Mengurutkan agar ID di depan
    dim_halte_df = dim_halte_df[['DimHalteID', 'StopID', 'StopName', 'Latitude', 'Longitude', 'City']]
    
    # ID 0 untuk fact
    UNKNOWN_HALTE_ID = dim_halte_df['DimHalteID'].iloc[0]
    # Dim Halte Selesai

    # =========================================================================
    # 5. MEMBUAT DIM CARD
    # =========================================================================

    # Mengcopy data card
    dim_card_df = df[['payCardID', 'payCardBank', 'payCardName', 'payCardSex', 'payCardBirthDate']].drop_duplicates().reset_index(drop=True)

    # Mengubah tipe data 'payCardBirthDate' ke Int64 untuk menangani NaN
    dim_card_df['payCardBirthDate'] = dim_card_df['payCardBirthDate'].astype('Int64')
    
    # Membuat variabel year sekarang
    current_year = datetime.now().year

    # Membuat umur dari year now - birthdate
    dim_card_df['Age'] = dim_card_df['payCardBirthDate'].apply(lambda x: current_year - x if pd.notna(x) else np.nan)
    
    # Membuat group berdasarkan umur
    dim_card_df['AgeGroup'] = dim_card_df['Age'].apply(
        lambda x: 'Youth (<18)' if x < 18 else ('Young Adult (18-35)' if 18 <= x <= 35 else ('Middle Age (36-55)' if 36 <= x <= 55 else ('Senior (>55)' if pd.notna(x) else 'Unknown')))
    )

    # Membuat id dim_corridor
    dim_card_df['DimCardID'] = range(1, len(dim_card_df) + 1)
    
    # Mengubah tipe data ke integer
    dim_card_df['DimCardID'] = dim_card_df['DimCardID'].astype('int64')

    # Mengurutkan ID agar di depan
    dim_card_df = dim_card_df[['DimCardID', 'payCardID', 'payCardBank', 'payCardName', 'payCardSex', 'payCardBirthDate', 'AgeGroup']]
    # Dim Card Selesai

    # =========================================================================
    # 6. MEMBUAT FACT TRANSACTION
    # =========================================================================
    
    fact_transaction = df.copy()
    
    # 3. Kolom baru datetimein dan datetimeout (Full Timestamp)
    fact_transaction['DateTime_In'] = fact_transaction['tapInTime']
    fact_transaction['DateTime_Out'] = fact_transaction['tapOutTime']

    # 4. TripDurationMinutes dihitung dari DateTime_Out dan DateTime_In
    fact_transaction['TripDurationMinutes'] = (fact_transaction['DateTime_Out'] - fact_transaction['DateTime_In']).dt.total_seconds() / 60

    # Menghitung Foreign Key Date (tanggal saja) dan Time (jam saja)
    fact_transaction['Date_In'] = fact_transaction['tapInTime'].dt.normalize()
    fact_transaction['Date_Out'] = fact_transaction['tapOutTime'].dt.normalize()
    fact_transaction['Hour_In'] = fact_transaction['tapInTime'].dt.hour
    fact_transaction['Hour_Out'] = fact_transaction['tapOutTime'].dt.hour

    # Membuat Transaksi Nol (IsFreeTrip)
    fact_transaction['IsFreeTrip'] = (fact_transaction['payAmount'].fillna(0) == 0.0).astype(int)

    # Membuat ID fact_transaction
    fact_transaction['FactKey'] = range(1, len(fact_transaction) + 1)

    # 1. JOIN DIM DATE (Menggunakan FullDate sebagai FK/PK)
    # Join dim_date tap in (FK: Date_In, PK: FullDate)
    fact_transaction = pd.merge(fact_transaction, dim_date_df[['FullDate']],
                        left_on='Date_In', right_on='FullDate', how='left').rename(columns={'FullDate': 'Date_In_FK'}).drop(columns=['Date_In'])

    # Join dim_date tap out (FK: Date_Out, PK: FullDate)
    fact_transaction = pd.merge(fact_transaction, dim_date_df[['FullDate']],
                        left_on='Date_Out', right_on='FullDate', how='left').rename(columns={'FullDate': 'Date_Out_FK'}).drop(columns=['Date_Out'])
    
    # 
    fact_transaction = fact_transaction.rename(columns={'Date_In_FK': 'Date_In', 'Date_Out_FK': 'Date_Out'})
    
    # Fill unknown (gunakan UNKNOWN_DATE_KEY)
    fact_transaction['Date_Out'] = fact_transaction['Date_Out'].fillna(UNKNOWN_DATE_KEY)
    
    # 2. JOIN DIM TIME (Menggunakan HourOfDay sebagai FK/PK)
    # Join dim_time tap in (FK: Hour_In, PK: HourOfDay)
    fact_transaction = pd.merge(fact_transaction, dim_time_df[['HourOfDay']],
                        left_on='Hour_In', right_on='HourOfDay', how='left').rename(columns={'HourOfDay': 'Hour_In_FK'}).drop(columns=['Hour_In'])
    
    # Join dim_time tap out (FK: Hour_Out, PK: HourOfDay)
    fact_transaction = pd.merge(fact_transaction, dim_time_df[['HourOfDay']],
                        left_on='Hour_Out', right_on='HourOfDay', how='left').rename(columns={'HourOfDay': 'Hour_Out_FK'}).drop(columns=['Hour_Out'])
    
    # Renaming back the FKs to match request (Hour_In/Hour_Out)
    fact_transaction = fact_transaction.rename(columns={'Hour_In_FK': 'Hour_In', 'Hour_Out_FK': 'Hour_Out'})
    
    # Fill unknown ( UNKNOWN_TIME_KEY)
    fact_transaction['Hour_Out'] = fact_transaction['Hour_Out'].fillna(UNKNOWN_TIME_KEY)
    
    # Convert hours to Int64 (karena ada NaN dari fillna/merge)
    fact_transaction['Hour_In'] = fact_transaction['Hour_In'].astype('int64')
    fact_transaction['Hour_Out'] = fact_transaction['Hour_Out'].astype('int64')

    # JOIN Surrogate Keys (Dim Halte, Corridor, Card)
    
    # Join dim_halte tap in
    fact_transaction = pd.merge(fact_transaction, dim_halte_df[['DimHalteID', 'StopID']],
                        left_on='tapInStops', right_on='StopID', how='left').rename(columns={'DimHalteID': 'DimHalteID_In'}).drop(columns=['StopID', 'tapInStops'])

    # Join dim_halte tap out
    fact_transaction = pd.merge(fact_transaction, dim_halte_df[['DimHalteID', 'StopID']],
                        left_on='tapOutStops', right_on='StopID', how='left').rename(columns={'DimHalteID': 'DimHalteID_Out'}).drop(columns=['StopID', 'tapOutStops'])
    
    # Fill unknown (DimHalteID_Out)
    fact_transaction['DimHalteID_In'] = fact_transaction['DimHalteID_In'].fillna(UNKNOWN_HALTE_ID)
    fact_transaction['DimHalteID_Out'] = fact_transaction['DimHalteID_Out'].fillna(UNKNOWN_HALTE_ID)
    
    # Mengubah ke int
    fact_transaction['DimHalteID_In'] = fact_transaction['DimHalteID_In'].astype('int64')
    fact_transaction['DimHalteID_Out'] = fact_transaction['DimHalteID_Out'].astype('int64')

    # Join dim_corridor
    fact_transaction = pd.merge(fact_transaction, dim_corridor_df[['DimCorridorID', 'corridorID']],
                        on='corridorID', how='left').drop(columns=['corridorID'])
    # Konversi ke int64 setelah merge
    fact_transaction['DimCorridorID'] = fact_transaction['DimCorridorID'].astype('int64')

    # Join dim_card
    fact_transaction = pd.merge(fact_transaction, dim_card_df[['DimCardID', 'payCardID']],
                        on='payCardID', how='left').drop(columns=['payCardID'])
    # Konversi ke int64 setelah merge
    fact_transaction['DimCardID'] = fact_transaction['DimCardID'].astype('int64')

    # Menentukan kolom fact akhir
    fact_cols = [
        'FactKey', 'transID',
        
        # Natural Keys (Foreign Keys)
        'Date_In', 'Date_Out', 'Hour_In', 'Hour_Out',
        
        # Surrogate Keys (Foreign Keys)
        'DimHalteID_In', 'DimHalteID_Out', 'DimCorridorID', 'DimCardID',

        # Measures & Attributes
        'DateTime_In', 'DateTime_Out', # 3. & 4. Kolom Full Datetime untuk durasi
        'stopStartSeq','stopEndSeq',
        'payAmount', 'TripDurationMinutes', 'IsFreeTrip'
    ]
    
    # Assign kolom yang digunakan
    fact_transaction = fact_transaction[fact_cols]
    
    # =========================================================================
    # 7. SIMPAN DATA
    # =========================================================================

    OUTPUT_PATH = '/opt/airflow/data/tj_new_model/'

    dim_date_df.to_csv(OUTPUT_PATH + 'dim_date.csv', index=False)
    dim_time_df.to_csv(OUTPUT_PATH + 'dim_time.csv', index=False)
    dim_halte_df.to_csv(OUTPUT_PATH + 'dim_halte.csv', index=False)
    dim_corridor_df.to_csv(OUTPUT_PATH + 'dim_corridor.csv', index=False)
    dim_card_df.to_csv(OUTPUT_PATH + 'dim_card.csv', index=False)
    
    fact_transaction.to_csv(OUTPUT_PATH + 'fact_transaction.csv', index=False)
    
    
    
if __name__ == "__main__":
    main_transform_new_model_callable()