using System.IO.Compression;
using System.Net;
using JsonToParquet.Functions.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Parquet.Data;

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
        public HttpResponseData Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            var response = req.CreateResponse(HttpStatusCode.OK);
            
            response.Headers.Add("Content-Type", "application/json");

            // Create a BlobEndpoint for the source container
            string sourceConnectionString = "BlobEndpoint=https://lilablobssc.blob.core.windows.net;";
            CloudStorageAccount sourceStorageAccount = CloudStorageAccount.Parse(sourceConnectionString);
            CloudBlobClient sourceBlobClient = sourceStorageAccount.CreateCloudBlobClient();
            var sourceContainer = sourceBlobClient.GetContainerReference("snapshotserengeti-v-2-0");

            var sourceBlob = sourceContainer.GetBlockBlobReference("SnapshotSerengetiS01.json.zip");

            using (var sourceStream = sourceBlob.OpenRead())
            using (var archive = new ZipArchive(sourceStream))
            {
                foreach (var entry in archive.Entries)
                {
                   var entryStream = entry.Open();

                   var json = new StreamReader(entryStream).ReadToEnd();

                   var serengetiData = JsonConvert.DeserializeObject<SerengetiData>(json);
                   response.WriteString($"Images : {serengetiData.Images.Count} Annotations : {serengetiData.Annotations.Count}");

                   // Create parquet file
                }
            }

            return response;
        }
    }
}
