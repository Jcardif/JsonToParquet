using System.IO.Compression;
using System.Net;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using JsonToParquet.Functions.Models;
using JsonToParquet.Functions.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Parquet.Data;
using Parquet.Serialization;

namespace JsonToParquet.Functions
{
    public class JsonToParquetHttpTrigger
    {
        private readonly ILogger _logger;

        public JsonToParquetHttpTrigger(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<JsonToParquetHttpTrigger>();
        }

        [Function("JsonToParquetHttpTrigger")]
        public async Task<HttpResponseData> RunAsync([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            var response = req.CreateResponse(HttpStatusCode.OK);
            
            response.Headers.Add("Content-Type", "application/json");

            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var keyVaultName = config["KeyVaultName"];
            var keyVaultUri = $"https://{keyVaultName}.vault.azure.net/";

            var client = new SecretClient(new Uri(keyVaultUri), new DefaultAzureCredential());
            var adlsConnStringSecret = await client.GetSecretAsync("ADLS-ConnectionString");

            var destConnectionString = adlsConnStringSecret.Value.Value;

            var sourceContainer = GetSourceContainer();
            var destDirectory = await GetDestinationDirectoryAsync(destConnectionString);

            // s01 to s011
            var metadataZippedFiles = new string[]
            {
                "SnapshotSerengetiS01.json.zip",
                "SnapshotSerengetiS02.json.zip",
                "SnapshotSerengetiS03.json.zip",
                "SnapshotSerengetiS04.json.zip",
                "SnapshotSerengetiS05.json.zip",
                "SnapshotSerengetiS06.json.zip",
                "SnapshotSerengetiS07.json.zip",
                "SnapshotSerengetiS08.json.zip",
                "SnapshotSerengetiS09.json.zip",
                "SnapshotSerengetiS10.json.zip",
                "SnapshotSerengetiS11.json.zip",
            };

            foreach(var file in metadataZippedFiles)
            {
                var sourceBlob = sourceContainer.GetBlockBlobReference(file);
                var destBlob = destDirectory.GetBlockBlobReference(file.Replace(".json.zip", ".parquet"));
                
                using (var stream = await destBlob.OpenWriteAsync())
                {
                    await LoadDataAsync(sourceBlob, stream);

                    _logger.LogInformation($"Uploading {file} to {destBlob.Uri}");
                    await stream.FlushAsync();
                }
            }

            return response;
        }

        private async Task<CloudBlobDirectory> GetDestinationDirectoryAsync(string destConnectionString)
        {
            // Create a FileEndpoint for the destination ADLS
            CloudStorageAccount destStorageAccount = CloudStorageAccount.Parse(destConnectionString);

            var destBlobClient = destStorageAccount.CreateCloudBlobClient();
            var destContainer = destBlobClient.GetContainerReference("snapshot-serengeti");

            if (!await destContainer.ExistsAsync())
            {
                await destContainer.CreateIfNotExistsAsync();
            }

            var destDirectory = destContainer.GetDirectoryReference("metadata");


            return destDirectory;
        }

        private CloudBlobContainer GetSourceContainer()
        {
            // Create a BlobEndpoint for the source container
            string sourceConnectionString = "BlobEndpoint=https://lilablobssc.blob.core.windows.net;";
            CloudStorageAccount sourceStorageAccount = CloudStorageAccount.Parse(sourceConnectionString);
            CloudBlobClient sourceBlobClient = sourceStorageAccount.CreateCloudBlobClient();
            return sourceBlobClient.GetContainerReference("snapshotserengeti-v-2-0");
        }

        private async Task<Stream> LoadDataAsync(CloudBlockBlob sourceBlob, Stream destStream)
        {
            using (var sourceStream = sourceBlob.OpenRead())
            using (var archive = new ZipArchive(sourceStream))
            {
                var entry = archive.Entries.First();
                var entryStream = entry.Open();
                var json = new StreamReader(entryStream).ReadToEnd();

                var serengetiData = JsonConvert.DeserializeObject<SerengetiData>(json);

                // Create parquet file
                return await ParquetFileCreator.CreateParquetFileAsync(serengetiData, destStream, _logger, $"{entry.Name.Replace(".json", ".parquet")})");
            }
        }
    }
}
