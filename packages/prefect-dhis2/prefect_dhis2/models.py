"""DHIS2 response and metadata models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Dhis2ApiResponse(BaseModel):
    """Wrapper for an API response summary."""

    endpoint: str
    record_count: int
    status_code: int = 200


# ---------------------------------------------------------------------------
# Metadata models -- used when POSTing to /api/metadata
# ---------------------------------------------------------------------------


class Dhis2Ref(BaseModel):
    """A DHIS2 object reference (just an id)."""

    id: str = Field(description="DHIS2 UID")


class Dhis2CategoryOption(BaseModel):
    """A DHIS2 category option for the metadata API."""

    id: str
    name: str
    shortName: str


class Dhis2Category(BaseModel):
    """A DHIS2 category for the metadata API."""

    id: str
    name: str
    shortName: str
    dataDimensionType: str = "DISAGGREGATION"
    categoryOptions: list[Dhis2Ref]


class Dhis2CategoryCombo(BaseModel):
    """A DHIS2 category combination for the metadata API."""

    id: str
    name: str
    dataDimensionType: str = "DISAGGREGATION"
    categories: list[Dhis2Ref]


class Dhis2DataElement(BaseModel):
    """A DHIS2 data element for the metadata API."""

    id: str = Field(description="Fixed UID")
    name: str = Field(description="Display name")
    shortName: str = Field(description="Short name")
    domainType: str = Field(default="AGGREGATE", description="AGGREGATE or TRACKER")
    valueType: str = Field(default="NUMBER", description="Value type")
    aggregationType: str = Field(default="SUM", description="Aggregation type")
    categoryCombo: Dhis2Ref | None = None


class Dhis2DataSetElement(BaseModel):
    """A data element assignment within a data set."""

    dataElement: Dhis2Ref = Field(description="Data element reference")


class Dhis2DataSet(BaseModel):
    """A DHIS2 data set for the metadata API."""

    id: str = Field(description="Fixed UID")
    name: str = Field(description="Display name")
    shortName: str = Field(description="Short name")
    periodType: str = Field(default="Yearly", description="Period type")
    dataSetElements: list[Dhis2DataSetElement] = Field(default_factory=list, description="Data elements in the set")
    organisationUnits: list[Dhis2Ref] = Field(default_factory=list, description="Assigned org units")


class Dhis2MetadataPayload(BaseModel):
    """Payload for POST /api/metadata."""

    categoryOptions: list[Dhis2CategoryOption] = Field(default_factory=list)
    categories: list[Dhis2Category] = Field(default_factory=list)
    categoryCombos: list[Dhis2CategoryCombo] = Field(default_factory=list)
    dataElements: list[Dhis2DataElement] = Field(default_factory=list)
    dataSets: list[Dhis2DataSet] = Field(default_factory=list)


class Dhis2DataValueSet(BaseModel):
    """Payload for POST /api/dataValueSets."""

    dataValues: list[DataValue] = Field(default_factory=list)


class MetadataResult(BaseModel):
    """Result from ensure_dhis2_metadata: org units and COC mapping."""

    org_units: list[OrgUnitGeo]
    coc_mapping: CocMapping


# ---------------------------------------------------------------------------
# Common data models
# ---------------------------------------------------------------------------


class OrgUnitGeo(BaseModel):
    """A DHIS2 organisation unit with geometry."""

    id: str
    name: str
    geometry: dict[str, Any]


class CocMapping(BaseModel):
    """categoryOptionCombo UIDs resolved from DHIS2."""

    male: str
    female: str


class DataValue(BaseModel):
    """A single DHIS2 data value with category option combo."""

    dataElement: str = Field(description="Data element UID")
    period: str = Field(description="Period (e.g. '2020' for Yearly)")
    orgUnit: str = Field(description="Organisation unit UID")
    categoryOptionCombo: str = Field(description="Category option combo UID")
    value: str = Field(description="Value as string")
