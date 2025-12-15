from great_expectations.data_context import FileDataContext
from datetime import datetime


CHECKPOINT_MAPPING = {
    'dim_card' : 'checkpoint_dim_card',
    'dim corridor' : 'checkpoint_dim_corridor',
    'dim halte' : 'checkpoint_dim_halte',
    'dim_date': 'checkpoint_dim_date',
    'dim_location': 'checkpoint_dim_location',
    'fact_transaction': 'checkpoint_fact_transaction'
}

def main_validate_new_model_callable():
    
    context = FileDataContext(context_root_dir='/opt/airflow/gx/')
    
    
    # for asset_name, checkpoint_name in CHECKPOINT_MAPPING.items():
    #     print(f"--- Memvalidasi {asset_name} menggunakan Checkpoint: {checkpoint_name} ---")
        
    #     results = context.run_checkpoint(
    #         checkpoint_name=checkpoint_name
    #     )

    #     # CEK HASIL
    #     if not results.success:
    #         print(f" Validasi Gagal untuk {asset_name}!")
    #         all_success = False
    #     else:
    #         print(f" Validasi Sukses untuk {asset_name}.")
    
    
    
    current_datetime = datetime.now()
    
    datasource_name = f'fact_transaction_{current_datetime}'
    datasource = context.sources.add_pandas(datasource_name)

    # Give a name to a data asset
    asset_name = f'fact_transaction_{current_datetime}'
    path_to_data = '/opt/airflow/data/tj_new_model/fact_transaction.csv'
    asset = datasource.add_csv_asset(asset_name, filepath_or_buffer=path_to_data)
    
    

    # Build batch request
    batch = asset.build_batch_request()
    
    
    checkpoint = context.add_or_update_checkpoint(
        name = f'checkpoint_fact_transaction_{current_datetime}',
        batch_request = batch,
        expectation_suite_name = 'validator_fact_transaction'
        
    )
    
    checkpoint_result = checkpoint.run()
    
    
    # ------------------------------------
    current_datetime = datetime.now()
    
    datasource_name = f'dim_date_{current_datetime}'
    datasource = context.sources.add_pandas(datasource_name)

    # Give a name to a data asset
    asset_name = f'dim_date_{current_datetime}'
    path_to_data = '/opt/airflow/data/tj_new_model/dim_date.csv'
    asset = datasource.add_csv_asset(asset_name, filepath_or_buffer=path_to_data)
    

    # Build batch request
    batch = asset.build_batch_request()
    
    
    checkpoint = context.add_or_update_checkpoint(
        name = f'checkpoint_dim_date_{current_datetime}',
        batch_request = batch,
        expectation_suite_name = 'validator_dim_date'
        
    )

    checkpoint_result = checkpoint.run()
    
    # ------------------------------------
    current_datetime = datetime.now()
    
    datasource_name = f'dim_halte_{current_datetime}'
    datasource = context.sources.add_pandas(datasource_name)

    # Give a name to a data asset
    asset_name = f'dim_halte_{current_datetime}'
    path_to_data = '/opt/airflow/data/tj_new_model/dim_halte.csv'
    asset = datasource.add_csv_asset(asset_name, filepath_or_buffer=path_to_data)
    

    # Build batch request
    batch = asset.build_batch_request()
    
    
    checkpoint = context.add_or_update_checkpoint(
        name = f'checkpoint_dim_halte_{current_datetime}',
        batch_request = batch,
        expectation_suite_name = 'validator_dim_halte'
        
    )

    checkpoint_result = checkpoint.run()
    
    
    # ------------------------------------
    current_datetime = datetime.now()
    
    datasource_name = f'dim_time_{current_datetime}'
    datasource = context.sources.add_pandas(datasource_name)

    # Give a name to a data asset
    asset_name = f'dim_time_{current_datetime}'
    path_to_data = '/opt/airflow/data/tj_new_model/dim_time.csv'
    asset = datasource.add_csv_asset(asset_name, filepath_or_buffer=path_to_data)
    

    # Build batch request
    batch = asset.build_batch_request()
    
    
    checkpoint = context.add_or_update_checkpoint(
        name = f'checkpoint_dim_time_{current_datetime}',
        batch_request = batch,
        expectation_suite_name = 'validator_dim_time'
        
    )

    checkpoint_result = checkpoint.run()
    
    # ------------------------------------
    current_datetime = datetime.now()
    
    datasource_name = f'dim_card_{current_datetime}'
    datasource = context.sources.add_pandas(datasource_name)

    # Give a name to a data asset
    asset_name = f'dim_card_{current_datetime}'
    path_to_data = '/opt/airflow/data/tj_new_model/dim_card.csv'
    asset = datasource.add_csv_asset(asset_name, filepath_or_buffer=path_to_data)
    

    # Build batch request
    batch = asset.build_batch_request()
    
    
    checkpoint = context.add_or_update_checkpoint(
        name = f'checkpoint_dim_card_{current_datetime}',
        batch_request = batch,
        expectation_suite_name = 'validator_dim_card'
    )

    checkpoint_result = checkpoint.run()
    
    # ------------------------------------
    current_datetime = datetime.now()
    
    datasource_name = f'dim_corridor_{current_datetime}'
    datasource = context.sources.add_pandas(datasource_name)

    # Give a name to a data asset
    asset_name = f'dim_corridor_{current_datetime}'
    path_to_data = '/opt/airflow/data/tj_new_model/dim_corridor.csv'
    asset = datasource.add_csv_asset(asset_name, filepath_or_buffer=path_to_data)
    

    # Build batch request
    batch = asset.build_batch_request()
    
    
    checkpoint = context.add_or_update_checkpoint(
        name = f'checkpoint_dim_corridor_{current_datetime}',
        batch_request = batch,
        expectation_suite_name = 'validator_dim_corridor'
        
    )

    checkpoint_result = checkpoint.run()
    
    

if __name__ == "__main__":
    main_validate_new_model_callable()