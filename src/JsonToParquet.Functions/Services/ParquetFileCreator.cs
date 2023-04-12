using JsonToParquet.Functions.Models;
using Parquet;
using Parquet.Data;

namespace JsonToParquet.Functions.Services
{
    public class ParquetFileCreator
    {
        public static void CreateParquetFile(List<Annotation> annotations, string filePath)
        {
            // Create a Parquet schema
            var schema = new Schema(
                new DataField<bool>("sequence_level_annotation"),
                new DataField<string>("id"),
                new DataField<long>("category_id"),
                new DataField<string>("seq_id"),
                new DataField<string>("season"),
                new DataField<DateTimeOffset>("datetime"),
                new DataField<string>("subject_id"),
                new DataField<object>("count"),
                new DataField<object>("standing"),
                new DataField<object>("resting"),
                new DataField<object>("moving"),
                new DataField<object>("interacting"),
                new DataField<object>("young_present"),
                new DataField<string>("image_id"),
                new DataField<string>("location")
            );

            // Create a Parquet file writer
            using (var writer = new ParquetWriter(schema, filePath))
            {
                // Write data to the Parquet file
                using (var rowGroupWriter = writer.CreateRowGroup())
                {
                    foreach (var annotation in annotations)
                    {
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<bool>("sequence_level_annotation"), new[] { annotation.SequenceLevelAnnotation }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<string>("id"), new[] { annotation.Id }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<long>("category_id"), new[] { annotation.CategoryId }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<string>("seq_id"), new[] { annotation.SeqId }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<string>("season"), new[] { annotation.Season }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<DateTimeOffset>("datetime"), new[] { annotation.Datetime }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<string>("subject_id"), new[] { annotation.SubjectId }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<object>("count"), new[] { annotation.Count }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<object>("standing"), new[] { annotation.Standing }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<object>("resting"), new[] { annotation.Resting }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<object>("moving"), new[] { annotation.Moving }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<object>("interacting"), new[] { annotation.Interacting }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<object>("young_present"), new[] { annotation.YoungPresent }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<string>("image_id"), new[] { annotation.ImageId }));
                        rowGroupWriter.WriteColumn(new DataColumn(new DataField<string>("location"), new[] { annotation.Location }));
                    }
                }
            }
        }
    }

}