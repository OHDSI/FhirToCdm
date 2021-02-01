using Hl7.Fhir.Model;
using org.ohdsi.cdm.framework.common.Lookups;
using org.ohdsi.cdm.framework.common.Omop;
using System;
using System.Collections.Generic;
using System.Linq;
using omop = org.ohdsi.cdm.framework.common.Omop;

namespace FHIRtoCDM
{
    public class FhirToCdmMappings
    {
        private Vocabulary _vocabulary;

        public FhirToCdmMappings(Vocabulary vocabulary)
        {
            _vocabulary = vocabulary;
        }

        public IEnumerable<Tuple<omop.Person, List<omop.Location>>> CreatePersonAndLocations(Bundle fhir)
        {
            //provider_id Patient.generalPractitioner Patient
            //care_site_id BodySite.patient BodySite
            //birth_datetime Patient.birthDate us-core - patient
            //location_id Patient.address Patient

            var locations = new HashSet<omop.Location>();
            //var pat = fhir.Entry.FirstOrDefault(e => e.Resource.TypeName == "Patient");
            foreach (var pat in fhir.Entry.Where(e => e.Resource.TypeName == "Patient"))
            {
                var patient = (Patient)pat.Resource;
                var person = new omop.Person
                {
                    GenderSourceValue = patient.Gender.ToString(),
                    PersonSourceValue = patient.Id,
                    YearOfBirth = DateTime.Parse(patient.BirthDate).Year,
                    MonthOfBirth = DateTime.Parse(patient.BirthDate).Month,
                    DayOfBirth = DateTime.Parse(patient.BirthDate).Day,
                    EthnicityConceptId = 0
                };

                foreach (var ex in patient.Extension)
                {
                    if (ex.Url.ToLower().Contains("death"))
                    {

                    }
                }

                switch (person.GenderSourceValue)
                {
                    case "Male":
                        person.GenderConceptId = 8507;
                        break;

                    case "Female":
                        person.GenderConceptId = 8532;
                        break;

                    default:
                        person.GenderConceptId = 0;
                        break;
                }

                if (patient.GeneralPractitioner.Count > 0)
                {

                }

                foreach (var address in patient.Address)
                {
                    var location = new omop.Location
                    {
                        City = address.City,
                        State = address.State,
                        Zip = address.PostalCode,
                        Country = address.Country
                    };

                    //var line = address.Line.ToList();

                    //if (line.Count > 0)
                    //    location.Address1 = line[0];

                    //if (line.Count > 1)
                    //    location.Address2 = line[1];

                    locations.Add(location);
                }

                foreach (var item in patient.Extension)
                {
                    if (item.Url == "http://hl7.org/fhir/StructureDefinition/us-core-race")
                    {
                        if (item.Value != null)
                        {
                            person.RaceSourceValue = ((Hl7.Fhir.Model.Coding)item.Value).Display;
                        }
                        else if (item.Extension.Count > 0)
                        {
                            person.RaceSourceValue = ((Hl7.Fhir.Model.Coding)item.Extension[0].Value).Display;
                        }
                        break;
                    }
                    else if (item.Url == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity")
                    {
                        if (item.Value != null)
                        {
                            person.EthnicitySourceValue = ((Hl7.Fhir.Model.Coding)item.Value).Display;
                        }
                        else if (item.Extension.Count > 0)
                        {
                            person.RaceSourceValue = ((Hl7.Fhir.Model.Coding)item.Extension[0].Value).Display;
                        }
                        break;
                    }
                }

                if (!string.IsNullOrEmpty(person.EthnicitySourceValue))
                {
                    switch (person.EthnicitySourceValue.ToUpper())
                    {
                        case "CENTRAL_AMERICAN":
                        case "DOMINICAN":
                        case "MEXICAN":
                        case "PUERTO_RICAN":
                        case "SOUTH_AMERICAN":
                            person.EthnicityConceptId = 38003563;
                            break;

                        default:
                            person.EthnicityConceptId = 0;
                            break;
                    }
                }

                if (!string.IsNullOrEmpty(person.RaceSourceValue))
                {
                    switch (person.RaceSourceValue.ToUpper())
                    {
                        case "ASIAN":
                            person.RaceConceptId = 8515;
                            break;

                        case "BLACK":
                            person.RaceConceptId = 8516;
                            break;

                        case "OTHER":
                            person.RaceConceptId = 8522;
                            break;

                        case "WHITE":
                            person.RaceConceptId = 8527;
                            break;

                        case "HISPANIC":
                            person.RaceConceptId = 0;
                            person.EthnicityConceptId = 38003563;
                            break;

                        default:
                            person.RaceConceptId = 0;
                            break;
                    }
                }

                yield return new Tuple<omop.Person, List<omop.Location>>(person, locations.ToList());
            }
        }

        public IEnumerable<KeyValuePair<string, omop.VisitOccurrence>> CreateVisitOccurence(Bundle fhir, Dictionary<string, long> personIds)
        {
            //care_site_id Encounter.location.location.identifier us-core - encounter, us - core - location
            //admitting_source_concept_id Encounter.hospitalization.admitSource or Encounter.hospitalization.origin(location).type    us - core - encounter, us - core - location
            //discharge_to_concept_id Encounter.location.location.type us-core - encounter,us - core - location
            //preceding_visit_occurence Encounter.partOf us-core - encounter

            foreach (var item in fhir.Entry.Where(e => e.Resource.TypeName == "Encounter"))
            {
                var encounter = (Encounter)item.Resource;

                var conceptId = 9202;

                if (encounter.Class.Code.ToUpper() == "IMP")
                    conceptId = 9201;
                else if (encounter.Class.Code.ToUpper() == "EMER")
                    conceptId = 9203;

                if (encounter.Diagnosis.Count > 0)
                {

                }

                if (encounter.ReasonCode.Count > 0)
                {
                    foreach (var reasonCode in encounter.ReasonCode)
                    {
                        foreach (var code in reasonCode.Coding)
                        {
                            if (code.Code == "308646001")
                            {

                            }
                        }

                    }
                }

                if (encounter.Extension.Count > 0)
                {

                }

                var vo = new omop.VisitOccurrence(new omop.Entity())
                {
                    PersonId = GetPersonId(encounter.Subject, personIds),
                    SourceValue = encounter.Class.Code,
                    StartDate = DateTime.Parse(encounter.Period.Start),
                    EndDate = DateTime.Parse(encounter.Period.End),
                    TypeConceptId = 32817,
                    ConceptId = conceptId
                };

                yield return new KeyValuePair<string, VisitOccurrence>(encounter.Id, vo);
            }

        }

        public IEnumerable<omop.ConditionOccurrence> CreateConditionOccurrence(Bundle fhir, Dictionary<string, long> personIds, Dictionary<string, VisitOccurrence> visits)
        {
            //provider_id Condition.asserter us-core - condition
            //visit_occurrence_id Condition.encounter us-core - condition
            //condition_status_concept_id Condition.clinicalStatus us-core - condition
            //stop_reason Condition.Extension(Proposed Name: abatement - reason : CodeableConcept) us - core - condition

            foreach (var item in fhir.Entry.Where(e => e.Resource.TypeName == "Condition"))
            {
                var condition = (Condition)item.Resource;

                foreach (var code in condition.Code.Coding)
                {
                    var date = DateTime.Parse(((Hl7.Fhir.Model.FhirDateTime)condition.Onset).Value);

                    var co = new omop.ConditionOccurrence(new omop.Entity())
                    {
                        PersonId = GetPersonId(condition.Subject, personIds),
                        TypeConceptId = 32817,
                        StartDate = date,
                        SourceValue = code.Code
                    };

                    if (condition.Abatement != null)
                    {
                        co.EndDate = DateTime.Parse(((Hl7.Fhir.Model.FhirDateTime)condition.Abatement).Value);
                    }

                    var result = LookupCode(code);
                    if (result.Any())
                        SetConceptId(co, result[0]);

                    if (co.Domain == "Drug")
                    {
                        co.TypeConceptId = 32817;
                    }
                    else if (co.Domain == "Observation")
                    {
                        co.TypeConceptId = 32817;
                    }

                    var vo = GetVisitOccurrence(condition.Encounter, visits);
                    co.VisitOccurrenceId = vo.Id;
                    co.VisitDetailId = vo.Id;

                    if (!co.EndDate.HasValue)
                        co.EndDate = vo.EndDate;

                    yield return co;
                }
            }

        }

        public IEnumerable<omop.DrugExposure> CreateDrugExposure(Bundle fhir, Dictionary<string, long> personIds, Dictionary<string, VisitOccurrence> visits)
        {
            //stop_reason MedicationStatement.statusReason us-core - medicationstatement
            //refills MedicationStatement.basedOn(MedicationRequest).dispenseRequest.numberOfRepeatsAllowed   us - core - medicationstatement, us - core - medicationrequest
            //quantity MedicationStatement.basedOn(MedicationRequest).dispenseRequest.quantity us - core - medicationstatement, us - core - medicationrequest
            //days_supply MedicationStatement.basedOn(MedicationRequest).dispenseRequest.expectedSupplyDuration   us - core - medicationstatement, us - core - medicationrequest
            //lot_number MedicationStatement.medication.batch.lotNumber us-core - medicationstatement, us - core - medication
            //route_concept_id MedicationStatement.basedOn(MedicationRequest).dosageInstruction.route  us - core - medicationstatement, us - core - medicationrequest
            //provider_id MedicationStatement.basedOn(MedicationRequest).requester    us - core - medicationstatement, us - core - medicationrequest
            //verbatim_end_date MedicationStatement.basedOn(MedicationRequest).validityPeriod   us - core - medicationstatement, us - core - medicationrequest


            foreach (var item in fhir.Entry.Where(e => e.Resource.TypeName == "MedicationRequest"))
            {
                var medication = (MedicationRequest)item.Resource;
                var cc = medication.Medication as Hl7.Fhir.Model.CodeableConcept;
                if (cc == null)
                    continue;

                foreach (var code in cc.Coding)
                {
                    var de = new omop.DrugExposure(new omop.Entity())
                    {
                        PersonId = GetPersonId(medication.Subject, personIds),
                        TypeConceptId = 32817
                    };

                    if (medication.BasedOn.Count > 0)
                    {

                    }

                    if (medication.StatusReason != null)
                    {

                    }

                    if (medication.DosageInstruction != null && medication.DosageInstruction.Count > 0)
                    {
                        de.Sig = medication.DosageInstruction[0].Text;
                    }

                    if (medication.DispenseRequest != null)
                    {

                    }

                    var result = LookupCode(code);
                    if (result.Any())
                        SetConceptId(de, result[0]);

                    var vo = GetVisitOccurrence(medication.Encounter, visits);
                    de.VisitOccurrenceId = vo.Id;
                    de.VisitDetailId = vo.Id;
                    de.StartDate = vo.StartDate;
                    de.EndDate = vo.EndDate;

                    yield return de;
                }
            }


            foreach (var item in fhir.Entry.Where(e => e.Resource.TypeName == "Immunization"))
            {
                var immunization = (Immunization)item.Resource;

                foreach (var code in ((Hl7.Fhir.Model.CodeableConcept)immunization.VaccineCode).Coding)
                {
                    var de = new omop.DrugExposure(new omop.Entity())
                    {
                        PersonId = GetPersonId(immunization.Patient, personIds),
                        TypeConceptId = 32817
                    };

                    var result = LookupCode(code);
                    if (result.Any())
                        SetConceptId(de, result[0]);

                    var vo = GetVisitOccurrence(immunization.Encounter, visits);
                    de.VisitOccurrenceId = vo.Id;
                    de.VisitDetailId = vo.Id;
                    de.StartDate = vo.StartDate;
                    de.EndDate = vo.EndDate;

                    yield return de;
                }
            }
        }

        public IEnumerable<omop.ProcedureOccurrence> CreateProcedureOccurrence(Bundle fhir, Dictionary<string, long> personIds, Dictionary<string, VisitOccurrence> visits)
        {
            //quantity Procedure.Extension(Proposed Name: num - of - procedures : CodeableConcept)    us - core - procedure
            //provider_id Procedure.performer.actor

            foreach (var item in fhir.Entry.Where(e => e.Resource.TypeName == "Procedure"))
            {
                var procedure = (Procedure)item.Resource;

                foreach (var code in ((Hl7.Fhir.Model.CodeableConcept)procedure.Code).Coding)
                {
                    var po = new omop.ProcedureOccurrence(new omop.Entity())
                    {
                        PersonId = GetPersonId(procedure.Subject, personIds),
                        TypeConceptId = 32817
                    };

                    var result = LookupCode(code);
                    if (result.Any())
                        SetConceptId(po, result[0]);

                    if (procedure.Extension.Count > 0)
                    {

                    }

                    if (po.Domain == "Measurement")
                    {
                        po.TypeConceptId = 32817;
                    }

                    var vo = GetVisitOccurrence(procedure.Encounter, visits);
                    po.VisitOccurrenceId = vo.Id;
                    po.VisitDetailId = vo.Id;
                    po.StartDate = DateTime.Parse(((Hl7.Fhir.Model.Period)procedure.Performed).Start);
                    po.EndDate = DateTime.Parse(((Hl7.Fhir.Model.Period)procedure.Performed).End);

                    yield return po;
                }
            }
        }

        public IEnumerable<omop.Observation> CreateObservation(Bundle fhir, Dictionary<string, long> personIds, Dictionary<string, VisitOccurrence> visits)
        {
            foreach (var item in fhir.Entry.Where(e => e.Resource.TypeName == "AllergyIntolerance"))
            {
                var allergy = (AllergyIntolerance)item.Resource;

                foreach (var code in ((Hl7.Fhir.Model.CodeableConcept)allergy.Code).Coding)
                {
                    var o = new omop.Observation(new omop.Entity())
                    {
                        PersonId = GetPersonId(allergy.Patient, personIds),
                        TypeConceptId = 32817
                    };

                    var result = LookupCode(code);
                    if (result.Any())
                        SetConceptId(o, result[0]);

                    o.StartDate = DateTime.Parse(allergy.RecordedDate);

                    yield return o;
                }
            }
        }

        public IEnumerable<omop.Measurement> CreateMeasurement(Bundle fhir, Dictionary<string, long> personIds, Dictionary<string, VisitOccurrence> visits)
        {
            //provider_id Observation.performer(Practitioner)    us - core - observationresults
            //value_as_concept_id Observation.valueCodeableConcept us-core - observationresults

            foreach (var item in fhir.Entry.Where(e => e.Resource.TypeName == "Observation"))
            {
                var observation = (Hl7.Fhir.Model.Observation)item.Resource;

                foreach (var code in ((Hl7.Fhir.Model.CodeableConcept)observation.Code).Coding)
                {
                    var m = new omop.Measurement(new omop.Entity())
                    {
                        PersonId = GetPersonId(observation.Subject, personIds),
                        TypeConceptId = 32817
                    };

                    var result = LookupCode(code);
                    if (result.Any())
                        SetConceptId(m, result[0]);

                    if (observation.ReferenceRange != null && observation.ReferenceRange.Count > 0)
                    {
                        if (observation.ReferenceRange[0].Low != null)
                            m.RangeLow = observation.ReferenceRange[0].Low.Value;

                        if (observation.ReferenceRange[0].High != null)
                            m.RangeHigh = observation.ReferenceRange[0].High.Value;
                    }

                    var vo = GetVisitOccurrence(observation.Encounter, visits);
                    m.VisitOccurrenceId = vo.Id;
                    m.VisitDetailId = vo.Id;
                    m.StartDate = DateTime.Parse(((Hl7.Fhir.Model.FhirDateTime)observation.Effective).Value);

                    if (observation.Value != null)
                    {
                        var quantity = observation.Value as Quantity;
                        if (quantity != null)
                        {
                            m.UnitSourceValue = quantity.Unit;
                            var unit = LookupCode(m.UnitSourceValue, "Unit");
                            if (unit.Any() && unit[0].ConceptId.HasValue)
                            {
                                m.UnitConceptId = unit[0].ConceptId.Value;
                            }

                            m.ValueAsNumber = quantity.Value;
                            m.ValueSourceValue = m.ValueAsNumber.ToString();
                        }
                        else
                        {
                            var cc = observation.Value as CodeableConcept;

                            if (cc != null && cc.Coding.Count > 0)
                            {
                                m.ValueSourceValue = cc.Coding[0].Display;

                                var conceptId = LookupCode(cc.Coding[0]);
                                if (conceptId.Any() && conceptId[0].ConceptId.HasValue)
                                    m.ValueAsConceptId = conceptId[0].ConceptId.Value;
                            }
                            else
                            {
                                m.ValueSourceValue = observation.Value.ToString();
                            }
                        }
                    }

                    yield return m;
                }
            }
        }

        private long GetPersonId(ResourceReference e, Dictionary<string, long> personIds)
        {
            return personIds[e.Reference.Replace("urn:uuid:", "")];
        }

        private VisitOccurrence GetVisitOccurrence(ResourceReference e, Dictionary<string, VisitOccurrence> visits)
        {
            return visits[e.Reference.Replace("urn:uuid:", "")];
        }

        private void SetConceptId(IEntity e, LookupValue value)
        {
            if (value.ConceptId.HasValue)
            {
                e.ConceptId = value.ConceptId.Value;
                e.Domain = value.Domain;

                if (value.Ingredients != null)
                {
                    e.Ingredients = new List<int>(value.Ingredients.Count);
                    e.Ingredients.AddRange(value.Ingredients);
                }
            }

            e.SourceConceptId = value.SourceConceptId;
        }

        private List<LookupValue> LookupCode(string code, string vocabularyName)
        {
            return _vocabulary.Lookup(code, vocabularyName, DateTime.MinValue);
        }

        private List<LookupValue> LookupCode(Coding code)
        {
            var vocabularyName = "";
            switch (code.System)
            {
                case "http://snomed.info/sct":
                    vocabularyName = "Snomed";
                    break;

                case "http://www.nlm.nih.gov/research/umls/rxnorm":
                    vocabularyName = "Rxnorm";
                    break;

                case "http://hl7.org/fhir/sid/cvx":
                    vocabularyName = "Cvx";
                    break;

                case "http://loinc.org":
                    vocabularyName = "Loinc";
                    break;

                default:
                    throw new Exception("unknown vocabulary " + code.System);
            }

            return LookupCode(code.Code, vocabularyName);
        }
    }
}
