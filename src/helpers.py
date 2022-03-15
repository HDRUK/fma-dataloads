import numpy as np

from datetime import datetime


def datasets_to_archive(custodian_datasets, gateway_datasets):
    """
    Determine which datasets to archive within the Gateway.
    """
    datasets_to_archive_ids = np.array(
        list(set(np.array(list(map(lambda x: x["pid"], gateway_datasets)))) - set(np.array(list(map(lambda x: x["identifier"], custodian_datasets)))))
    )

    if len(datasets_to_archive_ids) > 0:
        return np.array(
            list(
                filter(
                    lambda x: x["pid"] in datasets_to_archive_ids,
                    gateway_datasets,
                )
            )
        )

    return []


def extract_new_datasets(custodian_datasets, gateway_datasets):
    """
    Determine which datasets are new to the Gateway.
    """
    new_datasets_ids = np.array(
        list(set(np.array(list(map(lambda x: x["identifier"], custodian_datasets)))) - set(np.array(list(map(lambda x: x["pid"], gateway_datasets)))))
    )

    if len(new_datasets_ids) > 0:
        return np.array(list(filter(lambda x: x["identifier"] in new_datasets_ids, custodian_datasets)))

    return []


def extract_overlapping_datasets(custodian_datasets, gateway_datasets):
    """
    Extract a new array of common datasets that overlap between two lists.
    """
    overlapping_datasets_ids = np.array(
        list(
            set(np.array(list(map(lambda x: x["identifier"], custodian_datasets)))).intersection(
                set(np.array(list(map(lambda x: x["pid"], gateway_datasets))))
            )
        )
    )

    custodian_versions = _extract_datasets_by_id(custodian_datasets, overlapping_datasets_ids)

    gateway_versions = _extract_datasets_by_id(gateway_datasets, overlapping_datasets_ids)

    return custodian_versions, gateway_versions


def generate_question_answers(dataset):
    """
    Generate the Gateway questionAnswers field given a datasetv2 object.
    """
    question_answers = {}

    # Summary
    if _keys_exists(dataset, "summary", "title"):
        question_answers["properties/summary/title"] = dataset["summary"]["title"]
    if _keys_exists(dataset, "summary", "abstract"):
        question_answers["properties/summary/abstract"] = dataset["summary"]["abstract"]
    if _keys_exists(dataset, "summary", "contactPoint"):
        question_answers["properties/summary/contactPoint"] = dataset["summary"]["contactPoint"]
    if _keys_exists(dataset, "summary", "keywords"):
        question_answers["properties/summary/keywords"] = dataset["summary"]["keywords"]
    if _keys_exists(dataset, "summary", "alternateIdentifiers"):
        question_answers["properties/summary/alternateIdentifiers"] = dataset["summary"]["alternateIdentifiers"]
    if _keys_exists(dataset, "summary", "doiName"):
        question_answers["properties/summary/doiName"] = dataset["summary"]["doiName"]

    # Documentation
    if _keys_exists(dataset, "documentation", "description"):
        question_answers["properties/documentation/description"] = dataset["documentation"]["description"]
    if _keys_exists(dataset, "documentation", "associatedMedia"):
        question_answers["properties/documentation/associatedMedia"] = dataset["documentation"]["associatedMedia"]
    if _keys_exists(dataset, "documentation", "isPartOf"):
        question_answers["properties/documentation/isPartOf"] = dataset["documentation"]["isPartOf"]

    # Coverage
    if _keys_exists(dataset, "coverage", "spatial"):
        question_answers["properties/coverage/spatial"] = dataset["coverage"]["spatial"]
    if _keys_exists(dataset, "coverage", "typicalAgeRange"):
        question_answers["properties/coverage/typicalAgeRange"] = dataset["coverage"]["typicalAgeRange"]
    if _keys_exists(dataset, "coverage", "physicalSampleAvailability"):
        question_answers["properties/coverage/physicalSampleAvailability"] = dataset["coverage"]["physicalSampleAvailability"]
    if _keys_exists(dataset, "coverage", "followup"):
        question_answers["properties/coverage/followup"] = dataset["coverage"]["followup"]
    if _keys_exists(dataset, "coverage", "pathway"):
        question_answers["properties/coverage/pathway"] = dataset["coverage"]["pathway"]

    # Provenance - origin
    if _keys_exists(dataset, "provenance", "origin", "purpose"):
        question_answers["properties/provenance/origin/purpose"] = dataset["provenance"]["origin"]["purpose"]
    if _keys_exists(dataset, "provenance", "origin", "source"):
        question_answers["properties/provenance/origin/source"] = dataset["provenance"]["origin"]["source"]
    if _keys_exists(dataset, "provenance", "origin", "collectionSituation"):
        question_answers["properties/provenance/origin/collectionSituation"] = dataset["provenance"]["origin"]["collectionSituation"]

    # Provenance - temporal
    if _keys_exists(dataset, "provenance", "temporal", "accrualPeriodicity"):
        question_answers["properties/provenance/temporal/accrualPeriodicity"] = dataset["provenance"]["temporal"]["accrualPeriodicity"]
    if _keys_exists(dataset, "provenance", "temporal", "distributionReleaseDate"):
        question_answers["properties/provenance/temporal/distributionReleaseDate"] = dataset["provenance"]["temporal"]["distributionReleaseDate"]
    if _keys_exists(dataset, "provenance", "temporal", "startDate"):
        question_answers["properties/provenance/temporal/startDate"] = dataset["provenance"]["temporal"]["startDate"]
    if _keys_exists(dataset, "provenance", "temporal", "endDate"):
        question_answers["properties/provenance/temporal/endDate"] = dataset["provenance"]["temporal"]["endDate"]
    if _keys_exists(dataset, "provenance", "temporal", "timeLag"):
        question_answers["properties/provenance/temporal/timeLag"] = dataset["provenance"]["temporal"]["timeLag"]

    # Accessibility - usage
    if _keys_exists(dataset, "accessibility", "usage", "dataUseLimitation"):
        question_answers["properties/accessibility/usage/dataUseLimitation"] = dataset["accessibility"]["usage"]["dataUseLimitation"]
    if _keys_exists(dataset, "accessibility", "usage", "dataUseRequirements"):
        question_answers["properties/accessibility/usage/dataUseRequirements"] = dataset["accessibility"]["usage"]["dataUseRequirements"]
    if _keys_exists(dataset, "accessibility", "usage", "resourceCreator"):
        question_answers["properties/accessibility/usage/resourceCreator"] = dataset["accessibility"]["usage"]["resourceCreator"]
    if _keys_exists(dataset, "accessibility", "usage", "investigations"):
        question_answers["properties/accessibility/usage/investigations"] = dataset["accessibility"]["usage"]["investigations"]
    if _keys_exists(dataset, "accessibility", "usage", "isReferencedBy"):
        question_answers["properties/accessibility/usage/isReferencedBy"] = dataset["accessibility"]["usage"]["isReferencedBy"]

    # Accessibility - access
    if _keys_exists(dataset, "accessibility", "access", "accessRights"):
        question_answers["properties/accessibility/access/accessRights"] = dataset["accessibility"]["access"]["accessRights"]
    if _keys_exists(dataset, "accessibility", "access", "accessService"):
        question_answers["properties/accessibility/access/accessService"] = dataset["accessibility"]["access"]["accessService"]
    if _keys_exists(dataset, "accessibility", "access", "accessRequestCost"):
        question_answers["properties/accessibility/access/accessRequestCost"] = dataset["accessibility"]["access"]["accessRequestCost"]
    if _keys_exists(dataset, "accessibility", "access", "deliveryLeadTime"):
        question_answers["properties/accessibility/access/deliveryLeadTime"] = dataset["accessibility"]["access"]["deliveryLeadTime"]
    if _keys_exists(dataset, "accessibility", "access", "jurisdiction"):
        question_answers["properties/accessibility/access/jurisdiction"] = dataset["accessibility"]["access"]["jurisdiction"]
    if _keys_exists(dataset, "accessibility", "access", "dataProcessor"):
        question_answers["properties/accessibility/access/dataProcessor"] = dataset["accessibility"]["access"]["dataProcessor"]
    if _keys_exists(dataset, "accessibility", "access", "dataController"):
        question_answers["properties/accessibility/access/dataController"] = dataset["accessibility"]["access"]["dataController"]

    # Accessibility - formats and standards
    if _keys_exists(dataset, "accessibility", "formatAndStandards", "vocabularyEncodingScheme"):
        question_answers["properties/accessibility/formatAndStandards/vocabularyEncodingScheme"] = dataset["accessibility"]["formatAndStandards"][
            "vocabularyEncodingScheme"
        ]
    if _keys_exists(dataset, "accessibility", "formatAndStandards", "conformsTo"):
        question_answers["properties/accessibility/formatAndStandards/conformsTo"] = dataset["accessibility"]["formatAndStandards"]["conformsTo"]
    if _keys_exists(dataset, "accessibility", "formatAndStandards", "language"):
        question_answers["properties/accessibility/formatAndStandards/language"] = dataset["accessibility"]["formatAndStandards"]["language"]
    if _keys_exists(dataset, "accessibility", "formatAndStandards", "format"):
        question_answers["properties/accessibility/formatAndStandards/format"] = dataset["accessibility"]["formatAndStandards"]["format"]

    # Enrichment and linkage
    if _keys_exists(dataset, "enrichmentAndLinkages", "qualifiedRelation"):
        question_answers["properties/enrichmentAndLinkage/qualifiedRelation"] = dataset["enrichmentAndLinkage"]["qualifiedRelation"]
    if _keys_exists(dataset, "enrichmentAndLinkage", "derivation"):
        question_answers["properties/enrichmentAndLinkage/derivation"] = dataset["enrichmentAndLinkage"]["derivation"]
    if _keys_exists(dataset, "enrichmentAndLinkage", "tools"):
        question_answers["properties/enrichmentAndLinkage/tools"] = dataset["enrichmentAndLinkage"]["tools"]

    # Observations
    if _keys_exists(dataset, "observations") and len(dataset["observations"]) > 0:
        id = 0
        for i in dataset["observations"]:
            if _keys_exists(i, "observedNode"):
                question_answers["properties/observation/observedNode" + str(id)] = i["observedNode"]
            if _keys_exists(i, "measuredValue"):
                question_answers["properties/observation/measuredValue" + str(id)] = i["measuredValue"]
            if _keys_exists(i, "disambiguatingDescription"):
                question_answers["properties/observation/disambiguatingDescription" + str(id)] = i["disambiguatingDescription"]
            if _keys_exists(i, "observationDate"):
                question_answers["properties/observation/observationDate" + str(id)] = i["observationDate"]
            if _keys_exists(i, "measuredProperty"):
                question_answers["properties/observation/measuredProperty" + str(id)] = i["measuredProperty"]
            id += 1

    return question_answers


def create_sync_array(datasets=[], sync_status="ok", publisher={}):
    """
    Given a list of datasets, create a list of sync objects with a given status for addition to the Gateway sync collection.
    """
    return list(
        map(
            lambda x: {
                "publisherName": publisher["publisherDetails"]["name"],
                "pid": x["pid"],
                "version": x["datasetVersion"],
                "status": sync_status,
                "lastSync": datetime.now(),
            },
            datasets,
        )
    )


def _keys_exists(element, *keys):
    """
    INTERNAL: helper function two determine if a key exists in a dict.
    """
    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True


def _extract_datasets_by_id(datasets, ids):
    """
    INTERNAL: given a list of IDs, extract the relevant datasets from the datasets list as a separate list.
    """
    if "pid" in datasets[0].keys():
        return np.array(
            list(
                filter(
                    lambda x: x["pid"] in ids,
                    datasets,
                )
            )
        )
    else:
        return np.array(
            list(
                filter(
                    lambda x: x["identifier"] in ids,
                    datasets,
                )
            )
        )
