from datetime import date
from decimal import Decimal
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator, model_validator

ReportingCurrency = Literal["AUD", "USD", "CAD", "GBP", "EUR", "ZAR"]


class PriceAssumption(BaseModel):
    """One commodity price assumption used in the base case."""
    commodity: str = Field(..., description="e.g., 'Au', 'Cu', 'Li2O', 'U3O8'")
    price: Decimal
    unit: str = Field(..., description="e.g., 'USD/oz', 'USD/lb', 'USD/t'")


_TIER_BY_TYPE: dict[str, str] = {
    "DFS": "definitive", "Updated DFS": "definitive",
    "Revised DFS": "definitive", "FFS": "definitive",
    "PFS": "indicative", "Updated PFS": "indicative",
    "Scoping": "conceptual", "PEA": "conceptual",
}


class StudyExtraction(BaseModel):
    """Strict Pydantic schema for LLM study extraction (DFS / PFS / Scoping)."""

    # ─── Identification ──────────────────────────────────────────────
    project_name: str = Field(..., min_length=2, max_length=120,
                              description="The deposit/project name only (e.g., 'Hemi', not 'Hemi Project')")
    study_type: Literal[
        "DFS", "Updated DFS", "Revised DFS", "FFS",       # definitive tier
        "PFS", "Updated PFS",                              # indicative tier
        "Scoping", "PEA",                                  # conceptual tier
    ]
    effective_date: Optional[date] = Field(None, description="The 'as at' date of the study, NOT the announcement date")
    primary_commodity: str = Field(..., description="Primary commodity code: Au, Cu, Li2O, U3O8, Ni, Zn, Fe, TREO, Co")

    # ─── Currency and headline economics ─────────────────────────────
    reporting_currency: ReportingCurrency = Field(..., description="Currency of the headline NPV")
    discount_rate_pct: Decimal = Field(..., ge=Decimal("0"), le=Decimal("25"),
                                       description="Discount rate used for NPV, e.g., 8.0 for NPV8")

    post_tax_npv_millions: Optional[Decimal] = Field(None, description="Post-tax NPV in millions of reporting_currency")
    pre_tax_npv_millions: Optional[Decimal] = Field(None, description="Pre-tax NPV in millions of reporting_currency")
    irr_pct: Optional[Decimal] = Field(None, ge=Decimal("0"), le=Decimal("200"))
    payback_years: Optional[Decimal] = Field(None, ge=Decimal("0"), le=Decimal("30"))

    # ─── Capex / opex ────────────────────────────────────────────────
    initial_capex_millions: Optional[Decimal] = Field(None, description="Initial pre-production capex in reporting_currency millions")
    sustaining_capex_millions: Optional[Decimal] = Field(None, description="Sustaining capex over LOM")
    opex_per_unit: Optional[Decimal] = None
    opex_unit: Optional[str] = Field(None, description="e.g., 'USD/t', 'USD/oz'")
    aisc_per_unit: Optional[Decimal] = Field(None, description="All-in sustaining cost per oz/lb/t")
    aisc_unit: Optional[str] = None

    # ─── Production ──────────────────────────────────────────────────
    mine_life_years: Optional[Decimal] = Field(None, ge=Decimal("0"), le=Decimal("60"))
    annual_production: Optional[Decimal] = None
    annual_production_unit: Optional[str] = None
    recovery_pct: Optional[Decimal] = Field(None, ge=Decimal("0"), le=Decimal("100"))

    # ─── Assumptions ─────────────────────────────────────────────────
    # ─── Tax ─────────────────────────────────────────────────────
    tax_rate_pct: Optional[Decimal] = Field(None, ge=Decimal("0"), le=Decimal("100"),
                                            description="Effective tax rate (corporate + royalty), e.g., 30.0")

    price_assumptions: list[PriceAssumption] = Field(default_factory=list)
    fx_assumption: Optional[Decimal] = Field(None, description="FX rate if reported, e.g., 0.66 for AUD/USD")
    fx_pair: Optional[str] = Field(None, description="e.g., 'AUD/USD'")

    # ─── Provenance ──────────────────────────────────────────────────
    extraction_warnings: list[str] = Field(default_factory=list,
                                           description="Issues like mixed currencies without explicit FX, multiple price scenarios")

    # ─── Validators ──────────────────────────────────────────────────

    @field_validator("project_name")
    @classmethod
    def project_name_not_placeholder(cls, v: str) -> str:
        forbidden = {"the project", "project", "tbd", "n/a", "unknown", "[project name]"}
        if v.strip().lower() in forbidden:
            raise ValueError(f"project_name is a placeholder: {v}")
        return v.strip()

    @model_validator(mode="after")
    def _sanity_warnings(self):
        """Append (never raise) extraction-sanity warnings. Partial-tolerant (I4)."""
        post, pre = self.post_tax_npv_millions, self.pre_tax_npv_millions
        if post is not None and pre is not None:
            if post == pre:
                self.extraction_warnings.append("npv_post_equals_pre_suspected_duplicate")
            elif post > pre:
                self.extraction_warnings.append(f"npv_post_gt_pre:{post}>{pre}")
        if self.aisc_per_unit is not None and self.aisc_unit:
            # malformed compound units like "US$1243AUD/oz": two currency tokens.
            u = self.aisc_unit
            cur_tokens = sum(t in u for t in ("USD", "US$", "AUD", "A$", "CAD", "C$"))
            if cur_tokens >= 2:
                self.extraction_warnings.append(f"aisc_unit_malformed:{u}")
        return self

    def has_minimum_data(self) -> bool:
        """At least NPV (pre or post tax) AND initial capex must be present."""
        has_npv = self.post_tax_npv_millions is not None or self.pre_tax_npv_millions is not None
        return has_npv and self.initial_capex_millions is not None

    def confidence_tier(self) -> str:
        """Map study_type to confidence tier. Used by persistence layer."""
        return _TIER_BY_TYPE[self.study_type]


# Backward-compat alias. Remove after one release cycle once all code migrates.
DFSExtraction = StudyExtraction
