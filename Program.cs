using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Mondrian.Metadata;
using Mondrian.Models.Input;
using Mondrian.Models.Source;
using System.Text.RegularExpressions;



string containerUri = "https://rrdevcfa2047244445.blob.core.windows.net/inputs/";
string prefix = "nygc-align/Project_VLI_15187_B01_CUS_Lane.2022-06-24/";
int segmentSize = 5000;
Console.WriteLine("Grabbing Azure credentials for Blob Storage..");
DefaultAzureCredential credential = new();
BlobContainerClient blobContainerClient = new(
        new Uri(containerUri),
        credential);

var resultSegment = blobContainerClient.GetBlobsAsync(prefix: prefix).AsPages(default, segmentSize); ;
var cellRecords = new Dictionary<CellRecord, InputCell>();
Console.WriteLine($"Starting file name read..");
await foreach (Page<BlobItem> blobPage in resultSegment)
{
  Console.WriteLine("Reading 5000 file names..");
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
      switch (fileNameParts[1])
      {
        case "R1":
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
        case "R2":
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
List<InputCell> InputCells = cellRecords.Values.ToList();
// Generate metadata.yaml and write to file
Console.WriteLine("Writing metadata.yaml");
Metadata metadata = new(InputCells);
using (StreamWriter outputFile = new("nygcalign-metadata.yaml"))
{
  await outputFile.WriteAsync(metadata.Yaml);
}

// Generate inputs.json and write to file
AlignmentWorkflow alignmentWorkflow = new()
{
  docker_image = "quay.io/mondrianscwgs/alignment:v0.0.84",
  metadata_yaml = "/rrdevcfa2047244445/inputs/scy-263/scy263-metadata.yaml",
  reference = new ReferenceGenome()
  {
    genome_name = "human",
    reference = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/human/GRCh37-lite.fa",
    reference_fa_fai = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/human/GRCh37-lite.fa.fai",
    reference_fa_amb = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/human/GRCh37-lite.fa.amb",
    reference_fa_ann = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/human/GRCh37-lite.fa.ann",
    reference_fa_bwt = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/human/GRCh37-lite.fa.bwt",
    reference_fa_pac = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/human/GRCh37-lite.fa.pac",
    reference_fa_sa = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/human/GRCh37-lite.fa.sa",
  },
  supplimentary_references = new List<ReferenceGenome>
  {
    new ReferenceGenome()
    {
      genome_name = "mouse",
      reference = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/mouse/mm10_build38_mouse.fasta",
      reference_fa_fai = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/mouse/mm10_build38_mouse.fasta.fai",
      reference_fa_amb = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/mouse/mm10_build38_mouse.fasta.amb",
      reference_fa_ann = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/mouse/mm10_build38_mouse.fasta.ann",
      reference_fa_bwt = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/mouse/mm10_build38_mouse.fasta.bwt",
      reference_fa_pac = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/mouse/mm10_build38_mouse.fasta.pac",
      reference_fa_sa = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/mouse/mm10_build38_mouse.fasta.sa"
    },
    new ReferenceGenome()
    {
      genome_name = "salmon",
      reference = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/salmon/GCF_002021735.1_Okis_V1_genomic.fna",
      reference_fa_fai = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/salmon/GCF_002021735.1_Okis_V1_genomic.fna.fai",
      reference_fa_amb = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/salmon/GCF_002021735.1_Okis_V1_genomic.fna.amb",
      reference_fa_ann = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/salmon/GCF_002021735.1_Okis_V1_genomic.fna.ann",
      reference_fa_bwt = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/salmon/GCF_002021735.1_Okis_V1_genomic.fna.bwt",
      reference_fa_pac = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/salmon/GCF_002021735.1_Okis_V1_genomic.fna.pac",
      reference_fa_sa = "/rrdevcfa2047244445/datasets/reference/mondrian-ref-GRCh37/salmon/GCF_002021735.1_Okis_V1_genomic.fna.sa"
    }
  },
  fastq_files = new List<Mondrian.Models.Input.Cell>()
};
Console.WriteLine("Writing inputs.json");
Inputs inputs = new(InputCells, alignmentWorkflow);
using (StreamWriter outputFile = new("nygcalign-inputs.json"))
{
  await outputFile.WriteAsync(inputs.Json);
}

