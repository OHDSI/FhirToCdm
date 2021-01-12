using CommandLine;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using org.ohdsi.cdm.framework.common.Base;
using org.ohdsi.cdm.framework.common.Builder;
using org.ohdsi.cdm.framework.common.Enums;
using org.ohdsi.cdm.framework.common.Extensions;
using org.ohdsi.cdm.framework.common.Omop;
using ShellProgressBar;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using omop = org.ohdsi.cdm.framework.common.Omop;

namespace FHIRtoCDM
{
    class Options
    {
        [Option('f', "fhir", Required = true, HelpText = "Fhir files location.")]
        public string FhirFolder { get; set; }

        [Option('v', "vocabulary", Required = true, HelpText = "ODBC connection string to the vocabulary database.")]
        public string VocabularyConnectionString { get; set; }

        [Option('s', "schema", Required = true, HelpText = "Vocabulary database schema.")]
        public string VocabularySchema { get; set; }

        [Option('c', "cdm", Required = false, HelpText = "CDM version. (V52, V53, V6)", Default = CdmVersions.V53)]
        public CdmVersions Cdm { get; set; }

        [Option('r', "result", Required = false, HelpText = "Result folder name.", Default = "CDM")]
        public string Result { get; set; }

        [Option('u', "chunk", Required = false, HelpText = "Chunk size.", Default = 10000)]
        public int Chunk { get; set; }
    }

    class Program
    {
        private static Dictionary<string, long> _personIds = new Dictionary<string, long>();
        private static string _resultFolder;
        private static CdmVersions _cdm;
        private static int _chunkSize;

        static void Main(string[] args)
        {
            var vocabularyConnectionString = "";
            var vocabularySchema = "";
            var fhirFilesFolder = "";

            var r = Parser.Default.ParseArguments<Options>(args)
                  .WithParsed<Options>(o =>
                  {
                      _chunkSize = o.Chunk;
                      _cdm = o.Cdm;
                      _resultFolder = o.Result;
                      fhirFilesFolder = o.FhirFolder;
                      vocabularySchema = o.VocabularySchema;
                      vocabularyConnectionString = o.VocabularyConnectionString;
                  });

            if (r.Tag.ToString() != "Parsed")
                return;

            Console.WriteLine("Loading Vocabulary...");
            var vocabulary = new Vocabulary();
            vocabulary.Fill(vocabularyConnectionString, vocabularySchema);
            Console.WriteLine("Vocabulary DONE.");

            var cnt = 0;
            long personId = 0;
            long visitId = 0;
            var chunkId = 0;

            var fhirToCdm = new FhirToCdmMappings(vocabulary);
            ChunkPart chunk = new ChunkPart(chunkId, () => new CdmPersonBuilder(), "0", 0);
            ChunkData chunkData = new ChunkData(chunkId, 0);
            var offsetManager = new KeyMasterOffsetManager(chunkId, 0, 0);
            var location = new Dictionary<string, omop.Location>();

            //var cursorTop = Console.CursorTop + 1;
            Console.WriteLine("Processing:");
            var fhirFiles = Directory.GetFiles(fhirFilesFolder, "*.json");
            var fhirParser = new FhirJsonParser();

            var options = new ProgressBarOptions
            {
                ProgressCharacter = '─',
                ProgressBarOnBottom = true
            };
            using (var pbar = new ProgressBar(fhirFiles.Length - 1, "Processing FHIR files...", options))
            {
                foreach (var file in fhirFiles)
                {
                    pbar.Tick($"{cnt} from {fhirFiles.Length} were processed");

                    if (cnt > 0 && cnt % _chunkSize == 0)
                    {
                        Build(chunk, chunkData, offsetManager);
                        Save(chunkData, offsetManager);
                        chunkId++;
                        chunk = new ChunkPart(chunkId, () => new CdmPersonBuilder(), "0", 0);
                        chunkData = new ChunkData(chunkId, 0);
                        offsetManager = new KeyMasterOffsetManager(chunkId, 0, 0);

                        //cursorTop = Console.CursorTop + 1;
                    }

                    var visits = new Dictionary<string, VisitOccurrence>();
                    cnt++;
                    var fhir = (Bundle)fhirParser.Parse(File.ReadAllText(file));

                    var personAndLocations = fhirToCdm.CreatePersonAndLocations(fhir).ToList();
                    if (personAndLocations.Count == 0)
                        continue;

                    foreach (var pl in personAndLocations)
                    {
                        pl.Item1.PersonId = personId;
                        _personIds.Add(pl.Item1.PersonSourceValue, personId);
                        AddEntity(chunk, pl.Item1);

                        foreach (var l in pl.Item2)
                        {
                            var key = $"{l.City};{l.State};{l.Zip};{l.Country}";

                            if (!location.ContainsKey(key))
                                location.Add(key, l);
                        }
                        personId++;
                    }

                    foreach (var item in fhirToCdm.CreateVisitOccurence(fhir, _personIds))
                    {
                        item.Value.Id = visitId;
                        visits.Add(item.Key, item.Value);
                        AddEntity(chunk, item.Value);
                        visitId++;
                    }

                    foreach (var i in fhirToCdm.CreateConditionOccurrence(fhir, _personIds, visits))
                    {
                        AddEntity(chunk, i);
                    }

                    foreach (var i in fhirToCdm.CreateDrugExposure(fhir, _personIds, visits))
                    {
                        AddEntity(chunk, i);
                    }

                    foreach (var i in fhirToCdm.CreateProcedureOccurrence(fhir, _personIds, visits))
                    {
                        AddEntity(chunk, i);
                    }

                    foreach (var i in fhirToCdm.CreateObservation(fhir, _personIds, visits))
                    {
                        AddEntity(chunk, i);
                    }

                    foreach (var i in fhirToCdm.CreateMeasurement(fhir, _personIds, visits))
                    {
                        AddEntity(chunk, i);
                    }

                    //Console.CursorTop = cursorTop;
                    //Console.CursorLeft = 0;
                    //Console.WriteLine($"{cnt} from {fhirFiles.Length}");

                }
            }


            Build(chunk, chunkData, offsetManager);
            Save(chunkData, offsetManager);

            Console.WriteLine("Saving lookups...");
            int id = 0;
            foreach (var item in location.Values)
            {
                item.Id = id;
                id++;
            }

            var saver = new FileSaver(_cdm, _resultFolder);
            saver.SaveEntityLookup(_cdm, location.Values.ToList(), new List<CareSite>(), new List<Provider>());

            Console.WriteLine("Lookups was saved ");
        }

        private static void AddEntity(ChunkPart chunk, IEntity entity)
        {
            bool added;
            var pb = chunk.PersonBuilders.GetOrAdd(
                entity.PersonId,
                key => new Lazy<IPersonBuilder>(() => new CdmPersonBuilder()),
                out added).Value;


            if (entity != null)
                chunk.PersonBuilders[entity.PersonId].Value.AddData(entity);
        }

        private static void Build(ChunkPart chunk, ChunkData chunkData, KeyMasterOffsetManager offsetManager)
        {
            Console.WriteLine($"Building CDM...");
            var timer = new Stopwatch();
            timer.Start();
            foreach (var pb in chunk.PersonBuilders)
            {
                var result = pb.Value.Value.Build(chunkData, offsetManager);
                chunkData.AddAttrition(pb.Key, result);
            }
            timer.Stop();
            chunk.PersonBuilders.Clear();
            chunk.PersonBuilders = null;
            Console.WriteLine($"Building CDM chunkId={chunkData.ChunkId} - complete | {timer.ElapsedMilliseconds}ms");
        }

        private static void Save(ChunkData chunkData, KeyMasterOffsetManager offsetManager)
        {
            Console.WriteLine($"Saving chunkId={chunkData.ChunkId} ...");

            if (chunkData.Persons.Count == 0)
            {
                chunkData.Clean();
                return;
            }

            var saver = new FileSaver(_cdm, _resultFolder);
            var timer = new Stopwatch();
            timer.Start();
            saver.Save(chunkData, offsetManager);
            timer.Stop();

            Console.WriteLine($"Saving chunkId={chunkData.ChunkId} - complete | {timer.ElapsedMilliseconds}ms");

            chunkData.Clean();
            GC.Collect();
        }
    }
}
