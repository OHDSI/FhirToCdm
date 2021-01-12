using org.ohdsi.cdm.framework.common.Builder;
using org.ohdsi.cdm.framework.common.DataReaders.v5;
using org.ohdsi.cdm.framework.common.DataReaders.v5.v52;
using org.ohdsi.cdm.framework.common.DataReaders.v5.v53;
using org.ohdsi.cdm.framework.common.Enums;
using org.ohdsi.cdm.framework.common.Omop;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using cdm6 = org.ohdsi.cdm.framework.common.DataReaders.v6;

namespace FHIRtoCDM
{
    public class Saver
    {
        private KeyMasterOffsetManager _offsetManager;
        private CdmVersions _cdm;
        public Saver(CdmVersions cdm)
        {
            _cdm = cdm;
        }

        public void Save(ChunkData chunk, KeyMasterOffsetManager offsetManager)
        {
            _offsetManager = offsetManager;
            SaveSync(chunk);
        }

        protected IEnumerable<IDataReader> CreateDataReader(ChunkData chunk, string table)
        {
            if (_cdm == CdmVersions.V6)
            {
                switch (table)
                {
                    case "PERSON":
                        {
                            if (chunk.Persons.Count > 0)
                            {
                                yield return new cdm6.PersonDataReader(chunk.Persons);
                            }
                            break;
                        }

                    case "OBSERVATION_PERIOD":
                        {
                            if (chunk.ObservationPeriods.Count > 0)
                            {
                                yield return new cdm6.ObservationPeriodDataReader(chunk.ObservationPeriods, _offsetManager);
                            }
                            break;
                        }

                    case "PAYER_PLAN_PERIOD":
                        {
                            if (chunk.PayerPlanPeriods.Count > 0)
                            {
                                yield return new cdm6.PayerPlanPeriodDataReader(chunk.PayerPlanPeriods, _offsetManager);
                            }
                            break;
                        }

                    case "DRUG_EXPOSURE":
                        {
                            if (chunk.DrugExposures.Count > 0)
                            {
                                yield return new cdm6.DrugExposureDataReader(chunk.DrugExposures, _offsetManager);
                            }
                            break;
                        }

                    case "OBSERVATION":
                        {
                            if (chunk.Observations.Count > 0)
                            {
                                yield return new cdm6.ObservationDataReader(chunk.Observations, _offsetManager);
                            }
                            break;
                        }

                    case "VISIT_OCCURRENCE":
                        {
                            if (chunk.VisitOccurrences.Count > 0)
                            {
                                yield return new cdm6.VisitOccurrenceDataReader(chunk.VisitOccurrences, _offsetManager);
                            }
                            break;
                        }

                    case "VISIT_DETAIL":
                        {
                            if (chunk.VisitDetails.Count > 0)
                            {
                                yield return new cdm6.VisitDetailDataReader(chunk.VisitDetails, _offsetManager);
                            }
                            break;
                        }

                    case "PROCEDURE_OCCURRENCE":
                        {
                            if (chunk.ProcedureOccurrences.Count > 0)
                            {
                                yield return new cdm6.ProcedureOccurrenceDataReader(chunk.ProcedureOccurrences, _offsetManager);
                            }
                            break;
                        }

                    case "DRUG_ERA":
                        {
                            if (chunk.DrugEra.Count > 0)
                            {
                                yield return new cdm6.DrugEraDataReader(chunk.DrugEra, _offsetManager);
                            }
                            break;
                        }

                    case "CONDITION_ERA":
                        {
                            if (chunk.ConditionEra.Count > 0)
                            {
                                yield return new cdm6.ConditionEraDataReader(chunk.ConditionEra, _offsetManager);
                            }
                            break;
                        }

                    case "DEVICE_EXPOSURE":
                        {
                            if (chunk.DeviceExposure.Count > 0)
                            {
                                yield return new cdm6.DeviceExposureDataReader(chunk.DeviceExposure, _offsetManager);
                            }
                            break;
                        }

                    case "MEASUREMENT":
                        {
                            if (chunk.Measurements.Count > 0)
                            {
                                yield return new cdm6.MeasurementDataReader(chunk.Measurements, _offsetManager);
                            }
                            break;
                        }

                    case "COHORT":
                        {
                            if (chunk.Cohort.Count > 0)
                            {
                                yield return new cdm6.CohortDataReader(chunk.Cohort);
                            }
                            break;
                        }

                    case "CONDITION_OCCURRENCE":
                        {
                            if (chunk.ConditionOccurrences.Count > 0)
                            {
                                yield return new cdm6.ConditionOccurrenceDataReader(chunk.ConditionOccurrences, _offsetManager);
                            }
                            break;
                        }

                    case "COST":
                        {
                            if (chunk.Cost.Count > 0)
                            {
                                yield return new cdm6.CostDataReader(chunk.Cost, _offsetManager);
                            }
                            break;
                        }

                    case "NOTE":
                        {
                            if (chunk.Note.Count > 0)
                            {
                                yield return new cdm6.NoteDataReader(chunk.Note, _offsetManager);
                            }
                            break;
                        }

                    case "METADATA_TMP":
                        {
                            if (chunk.Metadata.Count > 0)
                            {
                                yield return new cdm6.MetadataDataReader(chunk.Metadata.Values.ToList());
                            }
                            break;
                        }

                    case "FACT_RELATIONSHIP":
                        {
                            if (chunk.FactRelationships.Count > 0)
                            {
                                yield return new cdm6.FactRelationshipDataReader(chunk.FactRelationships);
                            }

                            break;
                        }

                    case "DEATH":
                        break;

                    default:
                        throw new Exception("CreateDataReader, unsupported table name: " + table);
                }
            }
            else
            {
                switch (table)
                {
                    case "PERSON":
                        yield return new PersonDataReader(chunk.Persons);
                        break;

                    case "OBSERVATION_PERIOD":
                        {
                            if (_cdm == CdmVersions.V53)
                                yield return new ObservationPeriodDataReader53(chunk.ObservationPeriods, _offsetManager);
                            else
                                yield return new ObservationPeriodDataReader52(chunk.ObservationPeriods, _offsetManager);

                            break;
                        }

                    case "PAYER_PLAN_PERIOD":
                        {
                            if (_cdm == CdmVersions.V53)
                                yield return new PayerPlanPeriodDataReader53(chunk.PayerPlanPeriods, _offsetManager);
                            else
                                yield return new PayerPlanPeriodDataReader(chunk.PayerPlanPeriods, _offsetManager);

                            break;
                        }

                    case "DEATH":
                        {
                            yield return new DeathDataReader52(chunk.Deaths);
                            break;
                        }

                    case "DRUG_EXPOSURE":
                        {
                            if (_cdm == CdmVersions.V53)
                                yield return new DrugExposureDataReader53(chunk.DrugExposures, _offsetManager);
                            else
                                yield return new DrugExposureDataReader52(chunk.DrugExposures, _offsetManager);
                            break;
                        }


                    case "OBSERVATION":
                        {
                            if (_cdm == CdmVersions.V53)
                                yield return new ObservationDataReader53(chunk.Observations, _offsetManager);
                            else
                                yield return new ObservationDataReader(chunk.Observations, _offsetManager);
                            break;
                        }

                    case "VISIT_OCCURRENCE":
                        {
                            yield return new VisitOccurrenceDataReader52(chunk.VisitOccurrences, _offsetManager);
                            break;
                        }

                    case "VISIT_DETAIL":
                        yield return new VisitDetailDataReader53(chunk.VisitDetails, _offsetManager);
                        break;

                    case "PROCEDURE_OCCURRENCE":
                        {
                            if (_cdm == CdmVersions.V53)
                                yield return new ProcedureOccurrenceDataReader53(chunk.ProcedureOccurrences, _offsetManager);
                            else
                                yield return new ProcedureOccurrenceDataReader52(chunk.ProcedureOccurrences, _offsetManager);
                            break;
                        }

                    case "DRUG_ERA":
                        yield return new DrugEraDataReader(chunk.DrugEra, _offsetManager);
                        break;

                    case "CONDITION_ERA":
                        yield return new ConditionEraDataReader(chunk.ConditionEra, _offsetManager);
                        break;


                    case "DEVICE_EXPOSURE":
                        {
                            if (_cdm == CdmVersions.V53)
                                yield return new DeviceExposureDataReader53(chunk.DeviceExposure, _offsetManager);
                            else
                                yield return new DeviceExposureDataReader52(chunk.DeviceExposure, _offsetManager);
                            break;
                        }


                    case "MEASUREMENT":
                        if (_cdm == CdmVersions.V53)
                            yield return new MeasurementDataReader53(chunk.Measurements, _offsetManager);
                        else
                            yield return new MeasurementDataReader(chunk.Measurements, _offsetManager);
                        break;

                    case "COHORT":
                        yield return new CohortDataReader(chunk.Cohort);
                        break;

                    case "CONDITION_OCCURRENCE":
                        {
                            if (_cdm == CdmVersions.V53)
                                yield return new ConditionOccurrenceDataReader53(chunk.ConditionOccurrences, _offsetManager);
                            else
                                yield return new ConditionOccurrenceDataReader52(chunk.ConditionOccurrences, _offsetManager);
                            break;
                        }

                    case "COST":
                        {
                            yield return new CostDataReader52(chunk.Cost, _offsetManager);
                            break;
                        }

                    case "NOTE":

                        if (_cdm == CdmVersions.V53)
                            yield return new NoteDataReader53(chunk.Note, _offsetManager);
                        else
                            yield return new NoteDataReader52(chunk.Note, _offsetManager);
                        break;

                    case "FACT_RELATIONSHIP":
                        {
                            yield return new FactRelationshipDataReader(chunk.FactRelationships);
                            break;
                        }
                    default:
                        throw new Exception("CreateDataReader, unsupported table name: " + table);
                }
            }
        }

        public virtual void Write(ChunkData chunk, string table)
        {
            //Logger.Write(chunk.ChunkId, LogMessageTypes.Debug, "START - " + table);
            foreach (var reader in CreateDataReader(chunk, table))
            {
                Write(chunk.ChunkId, chunk.SubChunkId, reader, table);
            }
            //Logger.Write(chunk.ChunkId, LogMessageTypes.Debug, "END - " + table);
        }

        private void SaveSync(ChunkData chunk)
        {
            try
            {
                //var tasks = new List<Task>();
                Write(chunk, "PERSON");
                Write(chunk, "OBSERVATION_PERIOD");
                Write(chunk, "PAYER_PLAN_PERIOD");
                Write(chunk, "DEATH");
                Write(chunk, "DRUG_EXPOSURE");
                Write(chunk, "OBSERVATION");
                Write(chunk, "VISIT_OCCURRENCE");
                Write(chunk, "PROCEDURE_OCCURRENCE");

                Write(chunk, "DRUG_ERA");
                Write(chunk, "CONDITION_ERA");
                Write(chunk, "DEVICE_EXPOSURE");
                Write(chunk, "MEASUREMENT");
                Write(chunk, "COHORT");

                Write(chunk, "CONDITION_OCCURRENCE");

                Write(chunk, "COST");
                Write(chunk, "NOTE");

                if (_cdm == CdmVersions.V53 || _cdm == CdmVersions.V6)
                {
                    Write(chunk, "VISIT_DETAIL");
                    Write(chunk.ChunkId, chunk.SubChunkId, new MetadataDataReader(chunk.Metadata.Values.ToList()), "METADATA_TMP");
                }

                Write(chunk, "FACT_RELATIONSHIP");

                //Task.WaitAll(tasks.ToArray());

                Commit();
            }
            catch (Exception e)
            {
                //Logger.WriteError(chunk.ChunkId, e);
                Rollback();
                //Logger.Write(chunk.ChunkId, LogMessageTypes.Debug, "Rollback - Complete");
                throw;
            }
        }

        public virtual void SaveEntityLookup(CdmVersions cdmVersions, List<Location> location, List<CareSite> careSite, List<Provider> provider)
        {
            try
            {
                if (cdmVersions == CdmVersions.V6)
                {
                    if (location != null && location.Count > 0)
                        Write(null, null, new cdm6.LocationDataReader(location), "LOCATION");

                    if (careSite != null && careSite.Count > 0)
                        Write(null, null, new cdm6.CareSiteDataReader(careSite), "CARE_SITE");

                    if (provider != null && provider.Count > 0)
                        Write(null, null, new cdm6.ProviderDataReader(provider), "PROVIDER");
                }
                else
                {
                    if (location != null && location.Count > 0)
                        Write(null, null, new LocationDataReader(location), "LOCATION");

                    if (careSite != null && careSite.Count > 0)
                        Write(null, null, new CareSiteDataReader(careSite), "CARE_SITE");

                    if (provider != null && provider.Count > 0)
                    {
                        Write(null, null, new ProviderDataReader(provider), "PROVIDER");
                    }
                }


                Commit();
            }
            catch (Exception e)
            {
                //Logger.WriteError(e);
                Rollback();
                throw;
            }
        }

        public virtual void Write(int? chunkId, int? subChunkId, IDataReader reader, string tableName)
        {
            throw new NotImplementedException();
        }

        public virtual void Commit()
        {

        }

        public virtual void Rollback()
        {

        }

        public virtual void CopyVocabulary()
        {
            throw new NotImplementedException();
        }

        public virtual void Dispose()
        {

        }
    }
}
