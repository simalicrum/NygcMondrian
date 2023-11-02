using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure;
using Azure.Identity;
using System.Text.Json;
using YamlDotNet.Serialization;
using CsvHelper;
using System.Globalization;
using System.Text.RegularExpressions;
using Mondrian.Metadata;
using Mondrian.Models.Input;
using Mondrian.Models.Source;

Console.WriteLine("Hello, World!");

string containerUri = "https://rrdevcfa2047244445.blob.core.windows.net/inputs/";
string prefix = "nygc-align/Project_VLI_15187_B01_CUS_Lane.2022-06-24/";
int segmentSize = 5000;
DefaultAzureCredential credential = new();
BlobContainerClient blobContainerClient = new(
        new Uri(containerUri),
        credential);

var resultSegment = blobContainerClient.GetBlobsAsync(prefix: prefix).AsPages(default, segmentSize); ;
var cellRecords = new Dictionary<CellRecord, InputCell>();
await foreach (Page<BlobItem> blobPage in resultSegment)
{
  foreach (BlobItem blobItem in blobPage.Values)
  {
    if (!blobItem.Name.EndsWith(".fastq.gz") || Path.GetFileName(blobItem.Name).StartsWith("Empty"))
    {
      continue;
    }

  }
}