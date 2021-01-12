using org.ohdsi.cdm.framework.common.Enums;
using System.IO;
using System.Text;

namespace FHIRtoCDM
{
    public class FileSaver : Saver
    {
        private string _fileName;
        private string _folder;

        public FileSaver(CdmVersions cdm, string folder) : base(cdm)
        {
            _folder = folder;

            if (!Directory.Exists(folder))
                Directory.CreateDirectory(folder);
        }

        public override void Write(int? chunkId, int? subChunkId, System.Data.IDataReader reader, string tableName)
        {
            if (chunkId.HasValue)
                _fileName = Path.Combine(_folder, $"{tableName}_{chunkId}.csv");
            else
                _fileName = Path.Combine(_folder, $"{tableName}.csv");

            var isFirstRow = true;
            using (var writer = new StreamWriter(_fileName))
            {
                while (reader.Read())
                {
                    if (isFirstRow)
                    {
                        var header = new StringBuilder();
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            header.Append(reader.GetName(i));
                            if (i != reader.FieldCount - 1) header.Append("\t");
                        }
                        writer.WriteLine(header);
                        isFirstRow = false;
                    }

                    var row = new StringBuilder();
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        row.Append(reader.GetValue(i));
                        if (i != reader.FieldCount - 1) row.Append("\t");
                    }
                    writer.WriteLine(row);
                }
            }
        }

        public override void Rollback()
        {
            if (File.Exists(_fileName))
                File.Delete(_fileName);
        }
    }
}
