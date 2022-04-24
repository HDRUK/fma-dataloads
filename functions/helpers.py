"""
Helper functions for comparing lists and transforming data.
"""

import uuid
import json
import string
import numpy as np

from typing import Tuple
from datetime import datetime
from collections.abc import Mapping

from .exceptions import CriticalError


def datasets_to_archive(
    custodian_datasets: list = None, gateway_datasets: list = None
) -> np.array:
    """
    Determine which datasets to archive within the Gateway.
    """
    datasets_to_archive_ids = np.array(
        list(
            set(np.array(list(map(lambda x: x["pid"], gateway_datasets))))
            - set(np.array(list(map(lambda x: x["identifier"], custodian_datasets))))
        )
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


def extract_new_datasets(
    custodian_datasets: list = None, gateway_datasets: list = None
) -> np.array:
    """
    Determine which datasets are new to the Gateway.
    """
    new_datasets_ids = np.array(
        list(
            set(np.array(list(map(lambda x: x["identifier"], custodian_datasets))))
            - set(np.array(list(map(lambda x: x["pid"], gateway_datasets))))
        )
    )

    if len(new_datasets_ids) > 0:
        return np.array(
            list(
                filter(
                    lambda x: x["identifier"] in new_datasets_ids,
                    custodian_datasets,
                )
            )
        )

    return []


def extract_overlapping_datasets(
    custodian_datasets: list = None, gateway_datasets: list = None
) -> Tuple[np.array, np.array]:
    """
    Extract a new array of common datasets that overlap between two lists.
    """
    overlapping_datasets_ids = np.array(
        list(
            set(
                np.array(list(map(lambda x: x["identifier"], custodian_datasets)))
            ).intersection(
                set(np.array(list(map(lambda x: x["pid"], gateway_datasets))))
            )
        )
    )

    custodian_versions = _extract_datasets_by_id(
        custodian_datasets, overlapping_datasets_ids
    )

    gateway_versions = _extract_datasets_by_id(
        gateway_datasets, overlapping_datasets_ids
    )

    return custodian_versions, gateway_versions


def transform_dataset(dataset: dict = None, previous_version: dict = None) -> dict:
    """
    Given a datasetv2 format object, transform to the required Gateway format with a given activeflag.
    """
    try:
        dataset = _merge_dictionaries(dataset)

        formatted_dataset = {
            "datasetv2": dataset,
            "name": dataset["summary"]["title"],
            "datasetVersion": dataset["version"],
            "type": "dataset",
            "pid": dataset["identifier"],
            "datasetfields": {
                "publisher": dataset["summary"]["publisher"]["memberOf"]
                + ">"
                + dataset["summary"]["publisher"]["name"],
                "geographicCoverage": dataset["coverage"]["spatial"],
                "physicalSampleAvailability": dataset["coverage"][
                    "physicalSampleAvailability"
                ],
                "abstract": dataset["summary"]["abstract"],
                "releaseDate": dataset["provenance"]["temporal"][
                    "distributionReleaseDate"
                ],
                "accessRequestDuration": "",
                "datasetStartDate": dataset["provenance"]["temporal"]["startDate"],
                "datasetEndDate": dataset["provenance"]["temporal"]["endDate"],
                "ageBand": dataset["coverage"]["typicalAgeRange"]
                if _keys_exist(dataset, "coverage", "typicalAgeRange")
                else "",
                "contactPoint": dataset["summary"]["contactPoint"],
                "periodicity": dataset["provenance"]["temporal"]["accrualPeriodicity"],
                "metadataquality": {},
                "technicaldetails": [],
                "phenotypes": [],
            },
            "datasetid": str(uuid.uuid4()),
            "questionAnswers": json.dumps(_generate_question_answers(dataset)),
            "activeflag": "inReview",
            "is5Safes": True,
            "structuralMetadata": [],
            "timestamps": {
                "created": datetime.now(),
                "updated": datetime.now(),
                "submitted": datetime.now(),
            },
            "tags": {
                "features": dataset["summary"]["keywords"],
            },
            "source": "federation",
            "createdAt": datetime.now(),
            "updatedAt": datetime.now(),
        }

        if previous_version:
            formatted_dataset["datasetfields"]["metadataquality"] = previous_version[
                "datasetfields"
            ]["metadataquality"]

        if previous_version and previous_version["activeflag"] == "active":
            formatted_dataset["activeflag"] = "active"
            formatted_dataset["timestamps"]["published"] = datetime.now()

        if len(dataset["structuralMetadata"]) > 0:
            formatted_dataset["structuralMetadata"] = _format_structural_metadata(
                formatted_dataset["datasetv2"]["structuralMetadata"]
            )
            formatted_dataset["datasetfields"][
                "technicaldetails"
            ] = _format_technical_details(
                formatted_dataset["datasetv2"]["structuralMetadata"]
            )

        return formatted_dataset

    except KeyError as error:
        raise CriticalError(f"Key error when tranforming dataset: {error}") from error
    except Exception as error:
        raise CriticalError(
            f"Unknown error when tranforming dataset: {error}"
        ) from error


def create_sync_array(
    datasets: np.array = None, sync_status: str = "ok", publisher: dict = None
) -> list:
    """
    Given a list of datasets, create a list of sync objects with a given status for addition to the Gateway sync collection.
    """
    pid_key = "pid"
    version_key = "datasetVersion"

    if "pid" not in datasets[0].keys():
        pid_key = "identifier"

    if "datasetVersion" not in datasets[0].keys():
        version_key = "version"

    return list(
        map(
            lambda x: {
                "publisherName": publisher["publisherDetails"]["name"],
                "pid": x[pid_key],
                "name": x["name"] if "name" in x.keys() else x["summary"]["title"],
                "version": x[version_key],
                "status": sync_status,
                "lastSync": datetime.now(),
            },
            datasets,
        )
    )


def _generate_question_answers(dataset: dict = None) -> dict:
    """
    INTERNAL: generate the Gateway questionAnswers field given a datasetv2 object.
    """
    question_answers = {}

    # Summary
    if _keys_exist(dataset, "summary", "title"):
        question_answers["properties/summary/title"] = dataset["summary"]["title"]
    if _keys_exist(dataset, "summary", "abstract"):
        question_answers["properties/summary/abstract"] = dataset["summary"]["abstract"]
    if _keys_exist(dataset, "summary", "contactPoint"):
        question_answers["properties/summary/contactPoint"] = dataset["summary"][
            "contactPoint"
        ]
    if _keys_exist(dataset, "summary", "keywords"):
        question_answers["properties/summary/keywords"] = dataset["summary"]["keywords"]
    if _keys_exist(dataset, "summary", "alternateIdentifiers"):
        question_answers["properties/summary/alternateIdentifiers"] = dataset[
            "summary"
        ]["alternateIdentifiers"]
    if _keys_exist(dataset, "summary", "doiName"):
        question_answers["properties/summary/doiName"] = dataset["summary"]["doiName"]

    # Documentation
    if _keys_exist(dataset, "documentation", "description"):
        question_answers["properties/documentation/description"] = dataset[
            "documentation"
        ]["description"]
    if _keys_exist(dataset, "documentation", "associatedMedia"):
        question_answers["properties/documentation/associatedMedia"] = dataset[
            "documentation"
        ]["associatedMedia"]
    if _keys_exist(dataset, "documentation", "isPartOf"):
        question_answers["properties/documentation/isPartOf"] = dataset[
            "documentation"
        ]["isPartOf"]

    # Coverage
    if _keys_exist(dataset, "coverage", "spatial"):
        question_answers["properties/coverage/spatial"] = dataset["coverage"]["spatial"]
    if _keys_exist(dataset, "coverage", "typicalAgeRange"):
        question_answers["properties/coverage/typicalAgeRange"] = dataset["coverage"][
            "typicalAgeRange"
        ]
    if _keys_exist(dataset, "coverage", "physicalSampleAvailability"):
        question_answers["properties/coverage/physicalSampleAvailability"] = dataset[
            "coverage"
        ]["physicalSampleAvailability"]
    if _keys_exist(dataset, "coverage", "followup"):
        question_answers["properties/coverage/followup"] = dataset["coverage"][
            "followup"
        ]
    if _keys_exist(dataset, "coverage", "pathway"):
        question_answers["properties/coverage/pathway"] = dataset["coverage"]["pathway"]

    # Provenance - origin
    if _keys_exist(dataset, "provenance", "origin", "purpose"):
        question_answers["properties/provenance/origin/purpose"] = dataset[
            "provenance"
        ]["origin"]["purpose"]
    if _keys_exist(dataset, "provenance", "origin", "source"):
        question_answers["properties/provenance/origin/source"] = dataset["provenance"][
            "origin"
        ]["source"]
    if _keys_exist(dataset, "provenance", "origin", "collectionSituation"):
        question_answers["properties/provenance/origin/collectionSituation"] = dataset[
            "provenance"
        ]["origin"]["collectionSituation"]

    # Provenance - temporal
    if _keys_exist(dataset, "provenance", "temporal", "accrualPeriodicity"):
        question_answers["properties/provenance/temporal/accrualPeriodicity"] = dataset[
            "provenance"
        ]["temporal"]["accrualPeriodicity"]
    if _keys_exist(dataset, "provenance", "temporal", "distributionReleaseDate"):
        question_answers[
            "properties/provenance/temporal/distributionReleaseDate"
        ] = dataset["provenance"]["temporal"]["distributionReleaseDate"]
    if _keys_exist(dataset, "provenance", "temporal", "startDate"):
        question_answers["properties/provenance/temporal/startDate"] = dataset[
            "provenance"
        ]["temporal"]["startDate"]
    if _keys_exist(dataset, "provenance", "temporal", "endDate"):
        question_answers["properties/provenance/temporal/endDate"] = dataset[
            "provenance"
        ]["temporal"]["endDate"]
    if _keys_exist(dataset, "provenance", "temporal", "timeLag"):
        question_answers["properties/provenance/temporal/timeLag"] = dataset[
            "provenance"
        ]["temporal"]["timeLag"]

    # Accessibility - usage
    if _keys_exist(dataset, "accessibility", "usage", "dataUseLimitation"):
        question_answers["properties/accessibility/usage/dataUseLimitation"] = dataset[
            "accessibility"
        ]["usage"]["dataUseLimitation"]
    if _keys_exist(dataset, "accessibility", "usage", "dataUseRequirements"):
        question_answers[
            "properties/accessibility/usage/dataUseRequirements"
        ] = dataset["accessibility"]["usage"]["dataUseRequirements"]
    if _keys_exist(dataset, "accessibility", "usage", "resourceCreator"):
        question_answers["properties/accessibility/usage/resourceCreator"] = dataset[
            "accessibility"
        ]["usage"]["resourceCreator"]
    if _keys_exist(dataset, "accessibility", "usage", "investigations"):
        question_answers["properties/accessibility/usage/investigations"] = dataset[
            "accessibility"
        ]["usage"]["investigations"]
    if _keys_exist(dataset, "accessibility", "usage", "isReferencedBy"):
        question_answers["properties/accessibility/usage/isReferencedBy"] = dataset[
            "accessibility"
        ]["usage"]["isReferencedBy"]

    # Accessibility - access
    if _keys_exist(dataset, "accessibility", "access", "accessRights"):
        question_answers["properties/accessibility/access/accessRights"] = dataset[
            "accessibility"
        ]["access"]["accessRights"]
    if _keys_exist(dataset, "accessibility", "access", "accessService"):
        question_answers["properties/accessibility/access/accessService"] = dataset[
            "accessibility"
        ]["access"]["accessService"]
    if _keys_exist(dataset, "accessibility", "access", "accessRequestCost"):
        question_answers["properties/accessibility/access/accessRequestCost"] = dataset[
            "accessibility"
        ]["access"]["accessRequestCost"]
    if _keys_exist(dataset, "accessibility", "access", "deliveryLeadTime"):
        question_answers["properties/accessibility/access/deliveryLeadTime"] = dataset[
            "accessibility"
        ]["access"]["deliveryLeadTime"]
    if _keys_exist(dataset, "accessibility", "access", "jurisdiction"):
        question_answers["properties/accessibility/access/jurisdiction"] = dataset[
            "accessibility"
        ]["access"]["jurisdiction"]
    if _keys_exist(dataset, "accessibility", "access", "dataProcessor"):
        question_answers["properties/accessibility/access/dataProcessor"] = dataset[
            "accessibility"
        ]["access"]["dataProcessor"]
    if _keys_exist(dataset, "accessibility", "access", "dataController"):
        question_answers["properties/accessibility/access/dataController"] = dataset[
            "accessibility"
        ]["access"]["dataController"]

    # Accessibility - formats and standards
    if _keys_exist(
        dataset,
        "accessibility",
        "formatAndStandards",
        "vocabularyEncodingScheme",
    ):
        question_answers[
            "properties/accessibility/formatAndStandards/vocabularyEncodingScheme"
        ] = dataset["accessibility"]["formatAndStandards"]["vocabularyEncodingScheme"]
    if _keys_exist(dataset, "accessibility", "formatAndStandards", "conformsTo"):
        question_answers[
            "properties/accessibility/formatAndStandards/conformsTo"
        ] = dataset["accessibility"]["formatAndStandards"]["conformsTo"]
    if _keys_exist(dataset, "accessibility", "formatAndStandards", "language"):
        question_answers[
            "properties/accessibility/formatAndStandards/language"
        ] = dataset["accessibility"]["formatAndStandards"]["language"]
    if _keys_exist(dataset, "accessibility", "formatAndStandards", "format"):
        question_answers[
            "properties/accessibility/formatAndStandards/format"
        ] = dataset["accessibility"]["formatAndStandards"]["format"]

    # Enrichment and linkage
    if _keys_exist(dataset, "enrichmentAndLinkages", "qualifiedRelation"):
        question_answers["properties/enrichmentAndLinkage/qualifiedRelation"] = dataset[
            "enrichmentAndLinkage"
        ]["qualifiedRelation"]
    if _keys_exist(dataset, "enrichmentAndLinkage", "derivation"):
        question_answers["properties/enrichmentAndLinkage/derivation"] = dataset[
            "enrichmentAndLinkage"
        ]["derivation"]
    if _keys_exist(dataset, "enrichmentAndLinkage", "tools"):
        question_answers["properties/enrichmentAndLinkage/tools"] = dataset[
            "enrichmentAndLinkage"
        ]["tools"]

    # Observations
    if _keys_exist(dataset, "observations") and len(dataset["observations"]) > 0:
        observation_id = ""
        for i in dataset["observations"]:
            if _keys_exist(i, "observedNode"):
                question_answers[
                    "properties/observation/observedNode" + str(observation_id)
                ] = i["observedNode"]
            if _keys_exist(i, "measuredValue"):
                question_answers[
                    "properties/observation/measuredValue" + str(observation_id)
                ] = i["measuredValue"]
            if _keys_exist(i, "disambiguatingDescription"):
                question_answers[
                    "properties/observation/disambiguatingDescription"
                    + str(observation_id)
                ] = i["disambiguatingDescription"]
            if _keys_exist(i, "observationDate"):
                question_answers[
                    "properties/observation/observationDate" + str(observation_id)
                ] = i["observationDate"]
            if _keys_exist(i, "measuredProperty"):
                question_answers[
                    "properties/observation/measuredProperty" + str(observation_id)
                ] = i["measuredProperty"]
            observation_id = "_" + str(
                np.random.choice(list(string.ascii_uppercase + string.digits), 5)
            )

    return question_answers


def _keys_exist(element: dict = None, *keys) -> bool:
    """
    INTERNAL: helper function to determine if a key exists in a dict.
    """
    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True


def _extract_datasets_by_id(datasets: list = None, ids: np.array = None) -> np.array:
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


def _flatten(dictionary: dict = None, parent_key: str = "", sep: str = "/") -> dict:
    """
    INTERNAL: flatten a dict to a single dimension.
    """
    items = []
    for k, v in dictionary.items():
        new_key = parent_key + sep + k if parent_key else k
        if v and isinstance(v, Mapping):
            items.extend(_flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _unflatten(dictionary: dict = None, sep: str = "/") -> dict:
    """
    INTERNAL: unflatten a dict to multiple dimensions.
    """
    result = {}
    for key, value in dictionary.items():
        parts = key.split(sep)
        data = result
        for part in parts[:-1]:
            if part not in data:
                data[part] = {}
            data = data[part]
        data[parts[-1]] = value
    return result


def _merge_dictionaries(dictionary: dict = None) -> dict:
    """
    INTERNAL: merge two dictionaries, creating an empty field where missing from data.

    This function is neccessary because the Gateway EXPECTS fields which are NOT required
    by the validation schema.
    """
    # This is the minimum viable datasetv2 that is required for the Gateway to function
    minimum_viable_datasetv2 = {
        "identifier": "",
        "version": "",
        "issued": "",
        "modified": "",
        "revisions": [],
        "summary": {
            "title": "",
            "abstract": "",
            "publisher": {
                "identifier": "",
                "name": "",
                "logo": "",
                "description": "",
                "contactPoint": [],
                "memberOf": "",
                "accessRights": [],
                "deliveryLeadTime": "",
                "accessService": "",
                "accessRequestCost": "",
                "dataUseLimitation": [],
                "dataUseRequirements": [],
            },
            "contactPoint": "",
            "keywords": [],
            "alternateIdentifiers": [],
            "doiName": "",
        },
        "documentation": {
            "description": "",
            "associatedMedia": [],
            "isPartOf": [],
        },
        "coverage": {
            "spatial": [],
            "typicalAgeRange": "",
            "physicalSampleAvailability": [],
            "followup": "",
            "pathway": "",
        },
        "provenance": {
            "origin": {"purpose": [], "source": [], "collectionSituation": []},
            "temporal": {
                "accrualPeriodicity": "",
                "distributionReleaseDate": "",
                "startDate": "",
                "endDate": "",
                "timeLag": "",
            },
        },
        "accessibility": {
            "usage": {
                "dataUseLimitation": [],
                "dataUseRequirements": [],
                "resourceCreator": [],
                "investigations": [],
                "isReferencedBy": [],
            },
            "access": {
                "accessRights": [],
                "accessService": "",
                "accessRequestCost": [],
                "deliveryLeadTime": "",
                "jurisdiction": [],
                "dataProcessor": "",
                "dataController": "",
            },
            "formatAndStandards": {
                "vocabularyEncodingScheme": [],
                "conformsTo": [],
                "language": [],
                "format": [],
            },
        },
        "enrichmentAndLinkage": {
            "qualifiedRelation": [],
            "derivation": [],
            "tools": [],
        },
        "observations": [],
    }

    merged_dictionary = dictionary.copy()
    for key, value in _flatten(minimum_viable_datasetv2).items():
        if key not in _flatten(dictionary) or _flatten(dictionary)[key] == "":
            merged_dictionary[key] = value

    return _unflatten(merged_dictionary)


def _format_structural_metadata(metadata: list = None) -> list:
    """
    INTERNAL: convert SM from schema format to format required for structuralMetadata.
    """
    formatted_metadata = []

    for i in metadata:
        for j in i["elements"]:
            formatted_metadata.append(
                {
                    "tableName": i["name"],
                    "tableDescription": i["description"],
                    "columnName": j["name"],
                    "columnDescription": j["description"],
                    "dataType": j["dataType"],
                    "sensitive": j["sensitive"],
                }
            )

    return formatted_metadata


def _format_technical_details(metadata: list = None) -> list:
    """
    INTERNAL: convert SM from schema format to format for datasetfields.technicalDetails.
    """
    formatted_metadata = []

    for i in metadata:
        array_item = {
            "label": i["name"],
            "description": i["description"],
            "domainType": "DataClass",
            "elements": [],
        }
        for j in i["elements"]:
            array_item["elements"].append(
                {
                    "label": j["name"],
                    "description": j["description"],
                    "domainType": "DataElement",
                    "dataType": {"label": j["dataType"], "domainType": "PrimitiveType"},
                }
            )
        formatted_metadata.append(array_item)

    return formatted_metadata
