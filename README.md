# JsonToParquet

Right-click or `Ctrl + click` the button below to open the Azure Portal in a new tab and begin deployment.

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FJcardif%2FJsonToParquet%2Fmain%2Fsrc%2Fdeploy%2Fmain.json)

Convert parquet to delta lake

```python
# Obtain the list of file paths using wholeTextFiles
file_list = spark.sparkContext.wholeTextFiles('Files/metadata').keys().collect()

# Iterate through the list of file paths and print the file names
for file_path in file_list:
    # Extract the file name from the file path without extension
    file_name = file_path.split('/')[-1].split('.')[0]

    # read the parquet file
    parquet_df = spark.read.parquet(file_path)

    # Migrate a parquet data late to delta lake
    parquet_df.write.format('delta').save(f'Files/metadata/delta/{file_name}')
```