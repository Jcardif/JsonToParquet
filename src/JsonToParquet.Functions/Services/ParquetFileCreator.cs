using System.IO.Compression;
using JsonToParquet.Functions.Models;
using Microsoft.Extensions.Logging;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace JsonToParquet.Functions.Services
{
    public class ParquetFileCreator
    {
        public static async Task<Stream> CreateParquetFileAsync(SerengetiData data, Stream fileStream, ILogger _logger, string fileName)
        {
            var aggregatedJoin = data.Annotations
                .Join(data.Images, a=>a.ImageId, i=>i.Id, (a,i)=>new {
                    a.SequenceLevelAnnotation,
                    a.Id,
                    a.CategoryId,
                    a.SeqId,
                    a.Season,
                    a.Datetime,
                    a.SubjectId,
                    a.Count,
                    a.Standing,
                    a.Resting,
                    a.Moving,
                    a.Interacting,
                    a.YoungPresent,
                    a.ImageId,
                    a.Location,
                    i.FileName,
                    i.FrameNum,
                    i.Width,
                    i.Height,
                    i.Corrupt,
                    i.SeqNumFrames,
                }).ToList();
                

            // Create a Parquet schema
            var schema = new ParquetSchema(
                new DataField<bool>("sequence_level_annotation"),
                new DataField<string>("id"),
                new DataField<long>("category_id"),
                new DataField<string>("seq_id"),
                new DataField<string>("season"),
                new DataField<string>("datetime"),
                new DataField<string>("subject_id"),
                new DataField<string>("count"),
                new DataField<string>("standing"),
                new DataField<string>("resting"),
                new DataField<string>("moving"),
                new DataField<string>("interacting"),
                new DataField<string>("young_present"),
                new DataField<string>("image_id"),
                new DataField<string>("location"),
                new DataField<string>("file_name"),
                new DataField<long>("frame_num"),
                new DataField<long>("width"),
                new DataField<long>("height"),
                new DataField<bool>("corrupt"),
                new DataField<long>("seq_num_frames"));

            // Create all the data columns
            var sequenceLevelAnnotations = new DataColumn(
                (DataField)schema[0], 
                aggregatedJoin.Select(a => a.SequenceLevelAnnotation).ToArray());

            var ids = new DataColumn(
                (DataField)schema[1],
                aggregatedJoin.Select(a => a.Id).ToArray());

            var categoryIds = new DataColumn(
                (DataField)schema[2],
                aggregatedJoin.Select(a => a.CategoryId).ToArray());

            var seqIds = new DataColumn(
                (DataField)schema[3],
                aggregatedJoin.Select(a => a.SeqId).ToArray());
            
            var seasons = new DataColumn(
                (DataField)schema[4],
                aggregatedJoin.Select(a => a.Season).ToArray());
            
            var datetimes = new DataColumn(
                (DataField)schema[5],
                aggregatedJoin.Select(a => a.Datetime.ToString()).ToArray());

            var subjectIds = new DataColumn(
                (DataField)schema[6],
                aggregatedJoin.Select(a => a.SubjectId).ToArray());

            var counts = new DataColumn(
                (DataField)schema[7],
                aggregatedJoin.Select(a => a.Count.ToString()).ToArray());

            var standings = new DataColumn( 
                (DataField)schema[8],
                aggregatedJoin.Select(a => a.Standing.ToString()).ToArray());

            var restings = new DataColumn(
                (DataField)schema[9],
                aggregatedJoin.Select(a => a.Resting.ToString()).ToArray());

            var movings = new DataColumn(
                (DataField)schema[10],
                aggregatedJoin.Select(a => a.Moving.ToString()).ToArray());

            var interactings = new DataColumn(
                (DataField)schema[11],
                aggregatedJoin.Select(a => a.Interacting.ToString()).ToArray());
            
            var youngPresents = new DataColumn(
                (DataField)schema[12],
                aggregatedJoin.Select(a => a.YoungPresent.ToString()).ToArray());
            
            var imageIds = new DataColumn(
                (DataField)schema[13],
                aggregatedJoin.Select(a => a.ImageId).ToArray());

            var locations = new DataColumn(
                (DataField)schema[14],
                aggregatedJoin.Select(a => a.Location).ToArray());

            var fileNames = new DataColumn(
                (DataField)schema[15],
                aggregatedJoin.Select(a => a.FileName).ToArray());

            var frameNums = new DataColumn(
                (DataField)schema[16],
                aggregatedJoin.Select(a => a.FrameNum).ToArray());

            var widths = new DataColumn(
                (DataField)schema[17],
                aggregatedJoin.Select(a => a.Width).ToArray());

            var heights = new DataColumn(
                (DataField)schema[18],
                aggregatedJoin.Select(a => a.Height).ToArray());

            var corrupts = new DataColumn(
                (DataField)schema[19],
                aggregatedJoin.Select(a => a.Corrupt).ToArray());
            
            var seqNumFrames = new DataColumn(
                (DataField)schema[20],
                aggregatedJoin.Select(a => a.SeqNumFrames).ToArray());

            _logger.LogInformation($"Writing {aggregatedJoin.Count} items to Parquet file {fileName}");


            // Write the data to a Parquet file
            using (ParquetWriter writer = await ParquetWriter.CreateAsync(schema, fileStream))
            {
                writer.CompressionLevel = CompressionLevel.SmallestSize;

                using (ParquetRowGroupWriter groupWriter = writer.CreateRowGroup())
                {
                    await groupWriter.WriteColumnAsync(sequenceLevelAnnotations);
                    await groupWriter.WriteColumnAsync(ids);
                    await groupWriter.WriteColumnAsync(categoryIds);
                    await groupWriter.WriteColumnAsync(seqIds);
                    await groupWriter.WriteColumnAsync(seasons);
                    await groupWriter.WriteColumnAsync(datetimes);
                    await groupWriter.WriteColumnAsync(subjectIds);
                    await groupWriter.WriteColumnAsync(counts);
                    await groupWriter.WriteColumnAsync(standings);
                    await groupWriter.WriteColumnAsync(restings);
                    await groupWriter.WriteColumnAsync(movings);
                    await groupWriter.WriteColumnAsync(interactings);
                    await groupWriter.WriteColumnAsync(youngPresents);
                    await groupWriter.WriteColumnAsync(imageIds);
                    await groupWriter.WriteColumnAsync(locations);
                    await groupWriter.WriteColumnAsync(fileNames);
                    await groupWriter.WriteColumnAsync(frameNums);
                    await groupWriter.WriteColumnAsync(widths);
                    await groupWriter.WriteColumnAsync(heights);
                    await groupWriter.WriteColumnAsync(corrupts);
                    await groupWriter.WriteColumnAsync(seqNumFrames);
                }
            }

            return fileStream;
            
        }
    }

}