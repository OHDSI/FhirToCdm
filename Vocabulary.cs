using Npgsql;
using org.ohdsi.cdm.framework.common.Lookups;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace FHIRtoCDM
{
    public class Vocabulary
    {
        private readonly Dictionary<string, Lookup> _lookups = new Dictionary<string, Lookup>();
        private string _vocabularySchema;
        private string _vocabularyConnectionString;

        private static LookupValue CreateLookupValue(IDataRecord reader)
        {
            //source_code,	target_concept_id,	target_domain_id,	validstartdate,	validenddate,	source_vocabulary_id,	source_target_concept_id,	source_validstartdate,	source_validenddate,	ingredient_concept_id
            //    1                       2                 3                  4               5               6                       7                                 8                    9                        10


            //source_code,	target_concept_id,	target_domain_id,	validstartdate,	validenddate
            //    1                       2                 3                  4               5


            var sourceCode = string.Intern(reader[0].ToString().Trim());
            int conceptId = -1;
            if (int.TryParse(reader[1].ToString(), out var cptId))
                conceptId = cptId;

            if (!DateTime.TryParse(reader[3].ToString(), out var validStartDate))
                validStartDate = DateTime.MinValue;

            if (!DateTime.TryParse(reader[4].ToString(), out var validEndDate))
                validEndDate = DateTime.MaxValue;

            var lv = new LookupValue
            {
                ConceptId = conceptId,
                SourceCode = sourceCode,
                Domain = string.Intern(reader[2].ToString().Trim()),
                ValidStartDate = validStartDate,
                ValidEndDate = validEndDate,
                Ingredients = new HashSet<int>()
            };

            if (reader.FieldCount > 5)
            {
                lv.SourceConceptId = int.TryParse(reader[6].ToString(), out var scptId) ? scptId : 0;

                if (int.TryParse(reader[9].ToString(), out var ingredient))
                    lv.Ingredients.Add(ingredient);
            }


            return lv;
        }

        private void Load(string lookup)
        {
            if (!string.IsNullOrEmpty(lookup))
            {
                if (!_lookups.ContainsKey(lookup))
                {
                    string sql = string.Empty;

                    var baseSql = string.Empty;
                    var sqlFileDestination = string.Empty;

                    baseSql = File.ReadAllText(@"Lookups\Base.sql");

                    sqlFileDestination = Path.Combine("Lookups", lookup + ".sql");

                    sql = File.ReadAllText(sqlFileDestination);

                    sql = sql.Replace("{base}", baseSql);
                    sql = sql.Replace("{sc}", _vocabularySchema);

                    try
                    {
                        Console.WriteLine(lookup + " - Loading...");

                        var timer = new Stopwatch();
                        timer.Start();

                        FillVocabulary(lookup, sql);

                        timer.Stop();
                        Console.WriteLine($"DONE - {timer.ElapsedMilliseconds} ms | KeysCount={_lookups[lookup].KeysCount}");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Lookup error [file]: " + sqlFileDestination);
                        Console.WriteLine("Lookup error [query]: " + sql);

                        throw;
                    }
                }
            }
        }

        private void FillVocabulary(string lookup, string sql)
        {
            var odbc = new OdbcConnectionStringBuilder(_vocabularyConnectionString);
            
            try
            {
                if (_vocabularyConnectionString.ToLower().Contains("postgre"))
                {
                    var connectionStringTemplate = "Server={server};Port=5432;Database={database};User Id={username};Password={password};SslMode=Require;Trust Server Certificate=true";
                    var npgsqlConnectionString = connectionStringTemplate.Replace("{server}", odbc["server"].ToString())
                        .Replace("{database}", odbc["database"].ToString()).Replace("{username}", odbc["uid"].ToString())
                        .Replace("{password}", odbc["pwd"].ToString());

                    Console.WriteLine("npgsqlConnectionString=" + npgsqlConnectionString);

                    using (var connection = new NpgsqlConnection(npgsqlConnectionString))
                    {
                        connection.Open();
                        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 0 };
                        Fill(lookup, command);
                    }

                    return;
                }
            }
            catch (Exception e)
            {
                try
                {
                    Console.WriteLine(e.Message);

                    var connectionStringTemplate = "Server={server};Port=5432;Database={database};User Id={username};Password={password};SslMode=Disable;";
                    var npgsqlConnectionString = connectionStringTemplate.Replace("{server}", odbc["server"].ToString())
                        .Replace("{database}", odbc["database"].ToString()).Replace("{username}", odbc["uid"].ToString())
                        .Replace("{password}", odbc["pwd"].ToString());

                    Console.WriteLine("npgsqlConnectionString=" + npgsqlConnectionString);

                    using (var connection = new NpgsqlConnection(npgsqlConnectionString))
                    {
                        connection.Open();
                        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 0 };
                        Fill(lookup, command);
                    }

                    return;
                }
                catch (Exception e1)
                {
                    Console.WriteLine(e1.Message);
                }
            }

            using (var connection = new OdbcConnection(_vocabularyConnectionString))
            {
                connection.Open();

                using var command = new OdbcCommand(sql, connection) { CommandTimeout = 0 };
                Fill(lookup, command);
            }
        }

        private void Fill(string lookup, DbCommand command)
        {
            using var reader = command.ExecuteReader();
            Console.WriteLine(lookup + " - filling");
            var l = new Lookup();
            while (reader.Read())
            {
                var lv = CreateLookupValue(reader);
                l.Add(lv);
            }

            _lookups.Add(lookup, l);
        }

        public void Fill(string vocabularyConnectionString, string vocabularySchema)
        {
            _vocabularyConnectionString = vocabularyConnectionString;
            _vocabularySchema = vocabularySchema;

            foreach (var file in Directory.GetFiles("Lookups"))
            {
                var fi = new FileInfo(file);

                if (file == @"Lookups\Base.sql")
                    continue;

                Load(fi.Name.Replace(".sql", ""));
            }
        }


        public List<LookupValue> Lookup(string sourceValue, string key, DateTime eventDate)
        {
            if (!_lookups.ContainsKey(key))
                return new List<LookupValue>();

            return _lookups[key].LookupValues(sourceValue, eventDate).ToList();
        }

    }
}
