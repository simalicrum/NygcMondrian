using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Mondrian.Models.Source;
using System.Text.RegularExpressions;

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
    string fileName = Path.GetFileName(blobItem.Name);
    string[] fileNameParts = fileName.Split('.');
    string[] baseNameParts = fileNameParts[0].Split('_');
    string pattern = @"(?<Sample>.*)-131146A-R(?<Row>\d{2})-C(?<Col>\d{2})";
    Match match = Regex.Match(baseNameParts[0], pattern);
    string[] indexSeqParts = baseNameParts[1].Split('-');
    string[] controls = new string[] { "NTC", "A-NCC", "B-NCC", "C-NCC", "gDNA", "hTERT" };
    bool isControl = controls.Contains(match.Groups[1].Value);
    CellRecord cellRecord = new(baseNameParts[0], baseNameParts[2], baseNameParts[3]);
    string fastqPath = $"/{blobContainerClient.AccountName}/{blobContainerClient.Name}/{blobItem.Name}";
    if (cellRecords.ContainsKey(cellRecord))
    {
      switch (fileNameParts[1])
      {
        case "R1":
          cellRecords[cellRecord].Lane.fastq1 = fastqPath;
          break;
        case "R2":
          cellRecords[cellRecord].Lane.fastq2 = fastqPath;
          break;
        default:
          break;
      }
    }
    else
    {
      var Cell = new Mondrian.Models.Aggregate.Cell()
      {
        cell_id = baseNameParts[0],
        column = int.Parse(match.Groups[2].Value),
        condition = isControl ? match.Groups[1].Value : match.Groups[1].Value.Split('-')[1],
        is_control = isControl,
        library_id = "131146A",
        primer_i5 = indexSeqParts[0],
        primer_i7 = indexSeqParts[1],
        row = int.Parse(match.Groups[3].Value),
        sample_id = match.Groups[1].Value,
        sample_type = isControl ? match.Groups[1].Value : match.Groups[1].Value.Split('-')[1],
        lanes = new List<Mondrian.Models.Input.Lane>()
      };
      switch (match.Groups[3].Value)
      {
        case "R_1":
          cellRecords.Add(cellRecord,
          new InputCell()
          {
            CellId = baseNameParts[0],
            Cell = (Mondrian.Models.Aggregate.Cell)Cell,
            Lane = new Mondrian.Models.Aggregate.Lane()
            {
              fastq1 = fastqPath,
              fastq2 = "",
              flowcell_id = baseNameParts[2],
              lane_id = cellRecord.Lane
            }
          }
          );
          break;
        case "R_2":
          cellRecords.Add(cellRecord,
          new InputCell()
          {
            CellId = baseNameParts[0],
            Cell = (Mondrian.Models.Aggregate.Cell)Cell,
            Lane = new Mondrian.Models.Aggregate.Lane()
            {
              fastq1 = "",
              fastq2 = fastqPath,
              flowcell_id = baseNameParts[2],
              lane_id = cellRecord.Lane
            }
          });
          break;
        default:
          break;
      }
    }
  }
}
