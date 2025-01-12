# SCD Type 2 Demo Pipeline

This repository contains the pipeline `scd_type2_demo`, which demonstrates the implementation of SCD (Slowly Changing Dimension) Type 2 logic using Azure Data Factory.

## Linked Services Configuration

1. **LS_AzureKeyVault**  
   - Establishes a connection to Azure Key Vault.
   - Defines the following secrets:
     - `datalakestgacckey` - Datalake Storage Account Key
     - `sqlserverpwd` - Azure SQL Database Password

2. **LS_ADLSG2**  
   - Establishes a connection to Azure Data Lake Gen2.

3. **LS_AZURE_SQL_DAMG7370**  
   - Establishes a connection to Azure SQL Database.

## Datasets

1. **Custom_Delim_Dataset**  
   - Linked Service: `LS_ADLSG2`
   - File Type: `DelimitedText`
   - Parameters:
     - `file_delim`
     - `file_path`
     - `file_name`  
   Parameters are defined at runtime.

2. **DS_AZURE_SQL_DAMG7370**  
   - Linked Service: `LS_AZURE_SQL_DAMG7370`
   - Usage: Used to write results to the Azure SQL Database.
   - Parameter:  
     - `table_name` - Name of the target table (defined at runtime).

3. **Custom_Parquet**  
   - File Type: `Parquet`
   - Linked Service: `LS_ADLSG2`
   - Parameters:
     - `file_path`
     - `file_name`  
   These parameters are passed during runtime.

## Dataflows

1. **scd_type2_adw_cost_history**  
   - Implements SCD Type 2 logic.
   - Input Parameter:
     - `stg_file` - File path provided at runtime by the main pipeline `scd_type2_demo`.
   - Output: Written to the Parquet container.

2. **fail_scd_type2_adw_cost_history**  
   - Used for testing and prototyping purposes only.
   - No dependencies or implementation links in the pipeline.

## Pipeline: scd_type2_demo

This is the main pipeline that orchestrates the execution of SCD Type 2 logic.  

### Input Parameters:
- `src_file` - Source file name.
- `src_directory` - Source directory.
- `tgt_table` - Target table name in the database.
- `stg_file` - Intermediate file name for Parquet output.

### Workflow:
1. Input parameters are passed to the respective datasets and dataflows.
2. The `scd_type2_adw_cost_history` dataflow processes the input and generates Parquet output.
3. Results are written to the specified Azure SQL Database table.
