from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from fastapi import FastAPI, Request
from pydantic import BaseModel, ConfigDict, Field

from cap.core import (
    CAPABILITY_CARD_SCHEMA_URL,
    CapabilityAuthentication,
    CapabilityCard,
    CapabilityCausalEngine,
    CapabilityDetailedCapabilities,
    CapabilityDisclosurePolicy,
    CapabilityExtensionNamespace,
    CapabilityGraphMetadata,
    CapabilityProvider,
    CapabilityStructuralMechanisms,
    CapabilitySupportedVerbs,
)
from cap.core.canonical import (
    ASSUMPTION_ACYCLICITY,
    ASSUMPTION_LINEARITY,
    ASSUMPTION_MECHANISM_INVARIANCE_UNDER_INTERVENTION,
    IDENTIFICATION_STATUS_NOT_APPLICABLE,
    IDENTIFICATION_STATUS_NOT_FORMALLY_IDENTIFIED,
    REASONING_MODE_OBSERVATIONAL_PREDICTION,
    REASONING_MODE_SCM_SIMULATION,
    REASONING_MODE_STRUCTURAL_SEMANTICS,
)
from cap.core.contracts import (
    GraphMarkovBlanketRequest,
    GraphPathsRequest,
    GraphPath,
    GraphPathEdge,
    GraphPathNode,
    GraphNeighborsRequest,
    InterveneDoRequest,
    MetaCapabilitiesRequest,
    MetaMethodsRequest,
    ObservePredictRequest,
    TraverseChildrenRequest,
    TraverseParentsRequest,
)
from cap.server import (
    CAPHandlerSuccessSpec,
    CAPProvenanceContext,
    CAPProvenanceHint,
    CAPVerbRegistry,
    GRAPH_MARKOV_BLANKET_CONTRACT,
    GRAPH_NEIGHBORS_CONTRACT,
    GRAPH_PATHS_CONTRACT,
    INTERVENE_DO_CONTRACT,
    META_CAPABILITIES_CONTRACT,
    META_METHODS_CONTRACT,
    OBSERVE_PREDICT_CONTRACT,
    TRAVERSE_CHILDREN_CONTRACT,
    TRAVERSE_PARENTS_CONTRACT,
    build_fastapi_cap_dispatcher,
    register_cap_exception_handlers,
)

from example_cap_server import __version__
from example_cap_server import market_interpretation
from example_cap_server import runtime_config
from example_cap_server import toy_graph
from example_cap_server.integrations import get_cap_function_plan
from example_cap_server.market_pipeline.parser import parse_cap_request


@dataclass(frozen=True)
class ExampleService:
    public_base_url: str

    def capability_card(self) -> CapabilityCard:
        supported_verbs = CapabilitySupportedVerbs(
            core=registry.verbs_for_surface("core"),
            convenience=registry.verbs_for_surface("convenience"),
        )
        extension_verbs = registry.extension_verbs_by_namespace
        extensions = {
            namespace: CapabilityExtensionNamespace(
                schema_url=(
                    f"{self.public_base_url.rstrip('/')}/schema/extensions/{namespace}/v1.json"
                ),
                verbs=verbs,
                notes=[
                    "This namespace is deliberately synthetic and exists only to teach the extension boundary."
                ],
            )
            for namespace, verbs in extension_verbs.items()
        }
        return CapabilityCard(
            schema_url=CAPABILITY_CARD_SCHEMA_URL,
            name="CAP Example Server",
            description=(
                "Neutral Level 2 CAP example over a synthetic in-memory graph. "
                "The numeric outputs are illustrative, not scientific."
            ),
            version=__version__,
            provider=CapabilityProvider(
                name="CausalAgentProtocol",
                url="https://causalagentprotocol.org",
            ),
            endpoint=f"{self.public_base_url.rstrip('/')}/cap",
            conformance_level=2,
            supported_verbs=supported_verbs,
            causal_engine=CapabilityCausalEngine(
                family="toy_scm",
                algorithm="synthetic_linear_demo",
                supports_time_lag=False,
                supports_latent_variables=False,
                supports_nonlinear=False,
                supports_instantaneous=False,
                structural_mechanisms=CapabilityStructuralMechanisms(
                    available=True,
                    families=["linear"],
                    nodes_with_fitted_mechanisms=len(toy_graph.NODE_BASELINES),
                    residuals_computable=False,
                    mechanism_override_supported=False,
                    counterfactual_ready=False,
                ),
            ),
            detailed_capabilities=CapabilityDetailedCapabilities(
                graph_discovery=False,
                graph_traversal=True,
                temporal_multi_lag=False,
                effect_estimation=True,
                intervention_simulation=True,
                counterfactual_scm=False,
                latent_confounding_modeled=False,
                partial_identification=False,
                uncertainty_quantified=False,
            ),
            assumptions=[
                ASSUMPTION_ACYCLICITY,
                ASSUMPTION_LINEARITY,
                ASSUMPTION_MECHANISM_INVARIANCE_UNDER_INTERVENTION,
            ],
            reasoning_modes_supported=[
                REASONING_MODE_OBSERVATIONAL_PREDICTION,
                REASONING_MODE_SCM_SIMULATION,
                REASONING_MODE_STRUCTURAL_SEMANTICS,
            ],
            graph=CapabilityGraphMetadata(
                domains=["product", "finance", "commercial", "go_to_market"],
                node_count=len(toy_graph.NODE_BASELINES),
                edge_count=len(toy_graph.EDGES),
                node_types=sorted(set(toy_graph.NODE_TYPES.values())),
                edge_types_supported=["directed_causal_link"],
                graph_representation="synthetic_dag",
                update_frequency="static_demo",
                temporal_resolution=None,
                coverage_description=toy_graph.GRAPH_DESCRIPTION,
            ),
            authentication=CapabilityAuthentication(type="none"),
            access_tiers=[],
            disclosure_policy=CapabilityDisclosurePolicy(
                hidden_fields=[],
                default_response_detail="full",
                notes=[
                    "This example favors explicitness over privacy because the graph is synthetic."
                ],
            ),
            extensions=extensions,
        )


class DatasetProfileRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.example.dataset_profile"


class DatasetProfileResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    graph_id: str
    graph_version: str
    synthetic: bool
    node_count: int
    edge_count: int


class DatasetProfileResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: DatasetProfileResult


class DatasetDensityRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.example.dataset_density"


class DatasetDensityResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    graph_id: str
    graph_version: str
    synthetic: bool
    node_count: int
    edge_count: int
    possible_directed_edges: int
    missing_directed_edges: int
    density: float
    sparsity: float
    average_degree: float
    average_in_degree: float
    average_out_degree: float
    max_degree: int
    min_degree: int
    max_in_degree: int
    min_in_degree: int
    max_out_degree: int
    min_out_degree: int
    source_node_count: int
    sink_node_count: int
    isolated_node_count: int


class DatasetDensityResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: DatasetDensityResult


class ConnectivityReportParams(BaseModel):
    source_node_id: str
    target_node_id: str
    max_paths: int = 20


class ConnectivityReportRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.example.connectivity_report"
    params: ConnectivityReportParams


class ConnectivityReportResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_node_id: str
    target_node_id: str
    source_known: bool
    target_known: bool
    connected: bool
    path_count: int
    max_paths: int
    truncated: bool
    shortest_path: list[str] | None
    shortest_path_length: int | None
    longest_path: list[str] | None
    longest_path_length: int | None
    all_paths: list[list[str]]
    missing_nodes: list[str]


class ConnectivityReportResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: ConnectivityReportResult


class PathContributionReportParams(BaseModel):
    source_node_id: str
    target_node_id: str
    max_paths: int = 20


class PathContributionReportRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.example.path_contribution_report"
    params: PathContributionReportParams


class PathContributionEdge(BaseModel):
    model_config = ConfigDict(extra="forbid")

    from_node_id: str
    to_node_id: str
    weight: float


class PathContributionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rank: int
    node_ids: list[str]
    edge_count: int
    edges: list[PathContributionEdge]
    path_effect: float
    abs_path_effect: float
    share_of_total_effect: float | None
    share_of_total_absolute_effect: float | None


class PathContributionReportResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_node_id: str
    target_node_id: str
    source_known: bool
    target_known: bool
    connected: bool
    path_count: int
    max_paths: int
    truncated: bool
    total_effect: float
    total_absolute_effect: float
    top_contributing_path: list[str] | None
    top_contributing_path_effect: float | None
    paths: list[PathContributionItem]
    missing_nodes: list[str]


class PathContributionReportResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: PathContributionReportResult


class MarketImpactParams(BaseModel):
    target_node: str
    intervention_delta: float = 1.0
    min_effect_threshold: float = 0.0001


class MarketImpactRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.example.market_impact"
    params: MarketImpactParams


class MarketImpactNodeChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str
    delta: float
    abs_delta: float
    baseline: float
    relative_change: float | None


class MarketImpactResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_node: str
    target_known: bool
    intervention_delta: float
    min_effect_threshold: float
    node_count: int
    affected_node_count: int
    downstream_affected_node_count: int
    unaffected_node_count: int
    total_absolute_change: float
    average_absolute_change: float
    market_change_level: float
    max_affected_node: str | None
    max_affected_change: float | None
    max_affected_change_abs: float | None
    affected_nodes: list[MarketImpactNodeChange]
    missing_nodes: list[str]


class MarketImpactResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: MarketImpactResult


class NodeSystemicRiskParams(BaseModel):
    node_id: str
    stress_delta: float = 1.0
    min_effect_threshold: float = 0.0001


class NodeSystemicRiskRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.example.node_systemic_risk"
    params: NodeSystemicRiskParams


class NodeSystemicRiskResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str
    node_known: bool
    stress_delta: float
    min_effect_threshold: float
    node_count: int
    in_degree: int
    out_degree: int
    degree: int
    downstream_reachable_count: int
    downstream_reach_ratio: float
    affected_node_count: int
    downstream_affected_node_count: int
    unaffected_node_count: int
    total_absolute_change: float
    average_absolute_change: float
    market_change_level: float
    max_affected_node: str | None
    max_affected_change: float | None
    max_affected_change_abs: float | None
    impact_intensity: float
    propagation_breadth: float
    concentration_risk: float
    structural_centrality: float
    systemic_risk_score: float
    systemic_risk_level: str
    affected_nodes: list[MarketImpactNodeChange]
    missing_nodes: list[str]


class NodeSystemicRiskResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: NodeSystemicRiskResult


class MultiInterventionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_node: str
    intervention_delta: float = 1.0


class MultiInterventionImpactParams(BaseModel):
    interventions: list[MultiInterventionInput] = Field(min_length=1)
    min_effect_threshold: float = 0.0001


class MultiInterventionImpactRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.example.multi_intervention_impact"
    params: MultiInterventionImpactParams


class MultiInterventionInterventionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_node: str
    target_known: bool
    intervention_delta: float


class MultiInterventionSummaryItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_node: str
    target_known: bool
    intervention_delta: float
    affected_node_count: int
    market_change_level: float
    total_absolute_change: float
    max_affected_node: str | None
    max_affected_change_abs: float | None


class MultiInterventionNodeChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str
    delta: float
    abs_delta: float
    baseline: float
    relative_change: float | None


class MultiInterventionImpactResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intervention_count: int
    evaluated_intervention_count: int
    interventions: list[MultiInterventionInterventionItem]
    intervention_summaries: list[MultiInterventionSummaryItem]
    min_effect_threshold: float
    node_count: int
    affected_node_count: int
    unaffected_node_count: int
    total_absolute_change: float
    average_absolute_change: float
    market_change_level: float
    max_affected_node: str | None
    max_affected_change: float | None
    max_affected_change_abs: float | None
    affected_nodes: list[MultiInterventionNodeChange]
    missing_nodes: list[str]


class MultiInterventionImpactResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: MultiInterventionImpactResult


class InterventionRankingParams(BaseModel):
    outcome_node: str
    intervention_delta: float = 1.0
    candidate_nodes: list[str] | None = None
    top_k: int = 5
    min_effect_threshold: float = 0.0001


class InterventionRankingRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.example.intervention_ranking"
    params: InterventionRankingParams


class InterventionRankingItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rank: int
    candidate_node: str
    effect_on_outcome: float
    abs_effect_on_outcome: float
    affected_node_count: int
    downstream_affected_node_count: int
    market_change_level: float
    total_absolute_change: float
    max_affected_node: str | None
    max_affected_change_abs: float | None


class InterventionRankingResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome_node: str
    outcome_known: bool
    intervention_delta: float
    min_effect_threshold: float
    top_k: int
    candidate_count: int
    evaluated_candidate_count: int
    ranked_candidate_count: int
    missing_nodes: list[str]
    missing_candidate_nodes: list[str]
    rankings: list[InterventionRankingItem]


class InterventionRankingResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: InterventionRankingResult


class GenericExampleExtensionRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str
    params: dict = Field(default_factory=dict)


class GenericExampleExtensionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: dict


class NodeCriticalityRankingRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.node_criticality_ranking"


class EdgeCriticalityRankingRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.edge_criticality_ranking"


class GoalSeekInterventionRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.goal_seek_intervention"


class BudgetedInterventionOptimizerRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.budgeted_intervention_optimizer"


class ParetoInterventionFrontierRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.pareto_intervention_frontier"


class ScenarioCompareRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.scenario_compare"


class ShockCascadeSimulationRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.shock_cascade_simulation"


class ResilienceReportRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.resilience_report"


class TargetVulnerabilityReportRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.target_vulnerability_report"


class BottleneckReportRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.bottleneck_report"


class InfluenceMatrixRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.influence_matrix"


class InterventionBattleRequest(GenericExampleExtensionRequest):
    verb: str = "extensions.example.intervention_battle"


class VerbCatalogParams(BaseModel):
    detail: str = "full"
    include_examples: bool = True


class VerbCatalogRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.example.verb_catalog"
    params: VerbCatalogParams = Field(default_factory=VerbCatalogParams)


class VerbCatalogResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: dict


class MarketInterpretParams(BaseModel):
    request: dict


class MarketInterpretRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.market.interpret_request"
    params: MarketInterpretParams


class MarketInterpretResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: dict


class MarketParseParams(BaseModel):
    request: dict


class MarketParseRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.market.parse_request"
    params: MarketParseParams


class MarketParseResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: dict


class MarketBatchParams(BaseModel):
    requests: list[dict]
    stop_on_error: bool = False


class MarketBatchRequest(BaseModel):
    cap_version: str = "0.2.2"
    request_id: str | None = None
    context: dict | None = None
    options: dict = Field(default_factory=dict)
    verb: str = "extensions.market.batch_execute"
    params: MarketBatchParams


class MarketBatchResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cap_version: str
    request_id: str
    verb: str
    status: str
    result: dict


registry = CAPVerbRegistry()


def get_service(request: Request) -> ExampleService:
    return cast(ExampleService, request.app.state.cap_service)


def _coerce_float(value: object, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value: object, default: int, minimum: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, parsed)


def _parse_optional_str_list(value: object, param_name: str) -> tuple[list[str] | None, dict | None]:
    if value is None:
        return None, None
    if isinstance(value, str):
        return [value], None
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)], None
    return None, {
        "status": "invalid_request",
        "message": f"`params.{param_name}` must be a string array when provided.",
    }


@registry.core(META_CAPABILITIES_CONTRACT)
def meta_capabilities(payload: MetaCapabilitiesRequest, request: Request) -> dict:
    service = get_service(request)
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "meta-capabilities",
        "verb": payload.verb,
        "status": "success",
        "result": service.capability_card().model_dump(exclude_none=True, by_alias=True),
    }


@registry.core(META_METHODS_CONTRACT)
def meta_methods(payload: MetaMethodsRequest, request: Request) -> dict:
    del request
    params = payload.params
    methods = registry.list_methods(
        verbs=params.verbs if params and params.verbs else None,
        detail=params.detail if params else "compact",
        include_examples=params.include_examples if params else False,
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "meta-methods",
        "verb": payload.verb,
        "status": "success",
        "result": {"methods": [item.model_dump(exclude_none=True) for item in methods]},
    }


@registry.core(OBSERVE_PREDICT_CONTRACT)
def observe_predict(payload: ObservePredictRequest, request: Request) -> CAPHandlerSuccessSpec:
    del request
    target_node = payload.params.target_node
    return CAPHandlerSuccessSpec(
        result={
            "target_node": target_node,
            "prediction": toy_graph.compute_prediction(target_node),
            "drivers": toy_graph.strongest_drivers(target_node),
        },
        provenance_hint=CAPProvenanceHint(algorithm="synthetic_linear_demo"),
    )


@registry.core(INTERVENE_DO_CONTRACT)
def intervene_do(payload: InterveneDoRequest, request: Request) -> CAPHandlerSuccessSpec:
    del request
    params = payload.params
    effect = round(
        toy_graph.total_path_effect(params.treatment_node, params.outcome_node)
        * params.treatment_value,
        4,
    )
    return CAPHandlerSuccessSpec(
        result={
            "outcome_node": params.outcome_node,
            "effect": effect,
            "reasoning_mode": REASONING_MODE_SCM_SIMULATION,
            "identification_status": IDENTIFICATION_STATUS_NOT_FORMALLY_IDENTIFIED,
            "assumptions": [
                ASSUMPTION_ACYCLICITY,
                ASSUMPTION_LINEARITY,
                ASSUMPTION_MECHANISM_INVARIANCE_UNDER_INTERVENTION,
            ],
        },
        provenance_hint=CAPProvenanceHint(
            algorithm="synthetic_linear_demo",
            mechanism_family_used="linear",
        ),
    )


@registry.core(GRAPH_NEIGHBORS_CONTRACT)
def graph_neighbors(payload: GraphNeighborsRequest, request: Request) -> CAPHandlerSuccessSpec:
    del request
    params = payload.params
    ids = toy_graph.neighbors(params.node_id, params.scope)
    limited = ids[: params.max_neighbors]
    role = "parent" if params.scope == "parents" else "child"
    return CAPHandlerSuccessSpec(
        result={
            "node_id": params.node_id,
            "scope": params.scope,
            "neighbors": [{"node_id": node_id, "roles": [role]} for node_id in limited],
            "total_candidate_count": len(ids),
            "truncated": len(limited) < len(ids),
            "edge_semantics": "immediate_structural_neighbor",
            "reasoning_mode": REASONING_MODE_STRUCTURAL_SEMANTICS,
            "identification_status": IDENTIFICATION_STATUS_NOT_APPLICABLE,
            "assumptions": [ASSUMPTION_ACYCLICITY],
        },
        provenance_hint=CAPProvenanceHint(algorithm="synthetic_linear_demo"),
    )


@registry.core(GRAPH_MARKOV_BLANKET_CONTRACT)
def graph_markov_blanket(
    payload: GraphMarkovBlanketRequest,
    request: Request,
) -> CAPHandlerSuccessSpec:
    del request
    params = payload.params
    parent_ids = toy_graph.neighbors(params.node_id, "parents")
    child_ids = toy_graph.neighbors(params.node_id, "children")
    spouse_ids: set[str] = set()
    for child_id in child_ids:
        spouse_ids.update(toy_graph.neighbors(child_id, "parents"))
    spouse_ids.discard(params.node_id)

    flattened = (
        [{"node_id": node_id, "roles": ["parent"]} for node_id in parent_ids]
        + [{"node_id": node_id, "roles": ["child"]} for node_id in child_ids]
        + [{"node_id": node_id, "roles": ["spouse"]} for node_id in sorted(spouse_ids)]
    )
    limited = flattened[: params.max_neighbors]
    return CAPHandlerSuccessSpec(
        result={
            "node_id": params.node_id,
            "neighbors": limited,
            "total_candidate_count": len(flattened),
            "truncated": len(flattened) > len(limited),
            "edge_semantics": "markov_blanket_membership",
            "reasoning_mode": REASONING_MODE_STRUCTURAL_SEMANTICS,
            "identification_status": IDENTIFICATION_STATUS_NOT_APPLICABLE,
            "assumptions": [ASSUMPTION_ACYCLICITY],
        },
        provenance_hint=CAPProvenanceHint(algorithm="synthetic_linear_demo"),
    )


@registry.core(GRAPH_PATHS_CONTRACT)
def graph_paths(payload: GraphPathsRequest, request: Request) -> CAPHandlerSuccessSpec:
    del request
    params = payload.params
    raw_paths = toy_graph.find_paths(
        params.source_node_id,
        params.target_node_id,
        params.max_paths,
    )
    paths = []
    for path in raw_paths:
        nodes = [
            GraphPathNode(
                node_id=node_id,
                node_name=node_id.replace("_", " ").title(),
                node_type=toy_graph.NODE_TYPES[node_id],
                domain=toy_graph.NODE_DOMAINS[node_id],
            )
            for node_id in path
        ]
        edges = [
            GraphPathEdge(
                from_node_id=path[index],
                to_node_id=path[index + 1],
                edge_type="directed_causal_link",
            )
            for index in range(len(path) - 1)
        ]
        paths.append(GraphPath(distance=max(0, len(path) - 1), nodes=nodes, edges=edges))

    return CAPHandlerSuccessSpec(
        result={
            "source_node_id": params.source_node_id,
            "target_node_id": params.target_node_id,
            "connected": len(paths) > 0,
            "path_count": len(paths),
            "paths": [item.model_dump(exclude_none=True) for item in paths],
            "reasoning_mode": REASONING_MODE_STRUCTURAL_SEMANTICS,
            "identification_status": IDENTIFICATION_STATUS_NOT_APPLICABLE,
            "assumptions": [ASSUMPTION_ACYCLICITY],
        },
        provenance_hint=CAPProvenanceHint(algorithm="synthetic_linear_demo"),
    )


@registry.core(TRAVERSE_PARENTS_CONTRACT, surface="convenience")
def traverse_parents(payload: TraverseParentsRequest, request: Request) -> CAPHandlerSuccessSpec:
    del request
    nodes = toy_graph.neighbors(payload.params.node_id, "parents")[: payload.params.top_k]
    return CAPHandlerSuccessSpec(
        result={
            "node_id": payload.params.node_id,
            "direction": "parents",
            "nodes": nodes,
            "reasoning_mode": REASONING_MODE_STRUCTURAL_SEMANTICS,
            "identification_status": IDENTIFICATION_STATUS_NOT_APPLICABLE,
            "assumptions": [ASSUMPTION_ACYCLICITY],
        },
        provenance_hint=CAPProvenanceHint(algorithm="synthetic_linear_demo"),
    )


@registry.core(TRAVERSE_CHILDREN_CONTRACT, surface="convenience")
def traverse_children(
    payload: TraverseChildrenRequest,
    request: Request,
) -> CAPHandlerSuccessSpec:
    del request
    nodes = toy_graph.neighbors(payload.params.node_id, "children")[: payload.params.top_k]
    return CAPHandlerSuccessSpec(
        result={
            "node_id": payload.params.node_id,
            "direction": "children",
            "nodes": nodes,
            "reasoning_mode": REASONING_MODE_STRUCTURAL_SEMANTICS,
            "identification_status": IDENTIFICATION_STATUS_NOT_APPLICABLE,
            "assumptions": [ASSUMPTION_ACYCLICITY],
        },
        provenance_hint=CAPProvenanceHint(algorithm="synthetic_linear_demo"),
    )


@registry.extension(
    namespace="example",
    name="dataset_profile",
    request_model=DatasetProfileRequest,
    response_model=DatasetProfileResponse,
    description="Return non-normative metadata about the synthetic demo dataset used by cap-example.",
)
def dataset_profile(payload: DatasetProfileRequest, request: Request) -> dict:
    del request
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "dataset-profile",
        "verb": payload.verb,
        "status": "success",
        "result": {
            "graph_id": toy_graph.GRAPH_ID,
            "graph_version": toy_graph.GRAPH_VERSION,
            "synthetic": True,
            "node_count": len(toy_graph.NODE_BASELINES),
            "edge_count": len(toy_graph.EDGES),
        },
    }


@registry.extension(
    namespace="example",
    name="dataset_density",
    request_model=DatasetDensityRequest,
    response_model=DatasetDensityResponse,
    description=(
        "Return summary graph statistics for the synthetic demo dataset, "
        "including density, sparsity, and degree distribution summaries."
    ),
)
def dataset_density(payload: DatasetDensityRequest, request: Request) -> dict:
    del request
    metrics = toy_graph.dataset_density_metrics()
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "dataset-density",
        "verb": payload.verb,
        "status": "success",
        "result": {
            "graph_id": toy_graph.GRAPH_ID,
            "graph_version": toy_graph.GRAPH_VERSION,
            "synthetic": True,
            **metrics,
        },
    }


@registry.extension(
    namespace="example",
    name="connectivity_report",
    request_model=ConnectivityReportRequest,
    response_model=ConnectivityReportResponse,
    description=(
        "Return connectivity summary between two nodes, including connected status, "
        "shortest path, longest path, and all discovered paths."
    ),
)
def connectivity_report(payload: ConnectivityReportRequest, request: Request) -> dict:
    del request
    params = payload.params
    result = toy_graph.connectivity_report(
        params.source_node_id,
        params.target_node_id,
        max_paths=params.max_paths,
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "connectivity-report",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="path_contribution_report",
    request_model=PathContributionReportRequest,
    response_model=PathContributionReportResponse,
    description=(
        "Return per-path contribution decomposition between source and target, "
        "including path-level effect, normalized shares, and dominant path."
    ),
)
def path_contribution_report(payload: PathContributionReportRequest, request: Request) -> dict:
    del request
    params = payload.params
    result = toy_graph.path_contribution_report(
        params.source_node_id,
        params.target_node_id,
        max_paths=params.max_paths,
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "path-contribution-report",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="market_impact",
    request_model=MarketImpactRequest,
    response_model=MarketImpactResponse,
    description=(
        "Estimate intervention propagation level over the toy market graph, including "
        "affected node count, aggregate impact level, and maximum changed node."
    ),
)
def market_impact(payload: MarketImpactRequest, request: Request) -> dict:
    del request
    params = payload.params
    result = toy_graph.market_impact_report(
        params.target_node,
        params.intervention_delta,
        min_effect_threshold=params.min_effect_threshold,
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "market-impact",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="node_systemic_risk",
    request_model=NodeSystemicRiskRequest,
    response_model=NodeSystemicRiskResponse,
    description=(
        "Estimate systemic risk concentration for a node under stress, combining "
        "propagation breadth, impact intensity, structural centrality, and concentration."
    ),
)
def node_systemic_risk(payload: NodeSystemicRiskRequest, request: Request) -> dict:
    del request
    params = payload.params
    result = toy_graph.node_systemic_risk_report(
        params.node_id,
        params.stress_delta,
        min_effect_threshold=params.min_effect_threshold,
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "node-systemic-risk",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="multi_intervention_impact",
    request_model=MultiInterventionImpactRequest,
    response_model=MultiInterventionImpactResponse,
    description=(
        "Estimate aggregate market impact when multiple intervention nodes are changed "
        "at once, including combined affected nodes and per-intervention summaries."
    ),
)
def multi_intervention_impact(payload: MultiInterventionImpactRequest, request: Request) -> dict:
    del request
    params = payload.params
    interventions = [
        {
            "target_node": item.target_node,
            "intervention_delta": item.intervention_delta,
        }
        for item in params.interventions
    ]
    result = toy_graph.multi_intervention_impact_report(
        interventions,
        min_effect_threshold=params.min_effect_threshold,
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "multi-intervention-impact",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="intervention_ranking",
    request_model=InterventionRankingRequest,
    response_model=InterventionRankingResponse,
    description=(
        "Rank intervention candidates for a target outcome node by expected effect size "
        "and aggregate market impact."
    ),
)
def intervention_ranking(payload: InterventionRankingRequest, request: Request) -> dict:
    del request
    params = payload.params
    result = toy_graph.intervention_ranking_report(
        params.outcome_node,
        params.intervention_delta,
        candidate_nodes=params.candidate_nodes,
        top_k=params.top_k,
        min_effect_threshold=params.min_effect_threshold,
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "intervention-ranking",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="node_criticality_ranking",
    request_model=NodeCriticalityRankingRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Rank nodes by systemic criticality under stress propagation, including "
        "risk score, breadth, and structural indicators."
    ),
)
def node_criticality_ranking(
    payload: NodeCriticalityRankingRequest,
    request: Request,
) -> dict:
    del request
    params = payload.params
    candidate_nodes, error = _parse_optional_str_list(params.get("candidate_nodes"), "candidate_nodes")
    if error:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "node-criticality-ranking",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": error,
        }
    result = toy_graph.node_criticality_ranking_report(
        candidate_nodes=candidate_nodes,
        stress_delta=_coerce_float(params.get("stress_delta"), 1.0),
        top_k=_coerce_int(params.get("top_k"), default=5, minimum=1),
        min_effect_threshold=max(0.0, _coerce_float(params.get("min_effect_threshold"), 0.0001)),
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "node-criticality-ranking",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="edge_criticality_ranking",
    request_model=EdgeCriticalityRankingRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Rank edges by influence loss under edge removal, highlighting structural bottlenecks."
    ),
)
def edge_criticality_ranking(payload: EdgeCriticalityRankingRequest, request: Request) -> dict:
    del request
    params = payload.params
    result = toy_graph.edge_criticality_ranking_report(
        top_k=_coerce_int(params.get("top_k"), default=5, minimum=1),
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "edge-criticality-ranking",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="goal_seek_intervention",
    request_model=GoalSeekInterventionRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Propose intervention plans that can achieve a requested outcome delta with "
        "estimated impact tradeoffs."
    ),
)
def goal_seek_intervention(payload: GoalSeekInterventionRequest, request: Request) -> dict:
    del request
    params = payload.params
    outcome_node = params.get("outcome_node")
    if not isinstance(outcome_node, str) or not outcome_node:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "goal-seek-intervention",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": {"message": "`params.outcome_node` is required and must be a string."},
        }
    candidate_nodes, error = _parse_optional_str_list(params.get("candidate_nodes"), "candidate_nodes")
    if error:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "goal-seek-intervention",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": error,
        }
    result = toy_graph.goal_seek_intervention_report(
        outcome_node,
        _coerce_float(params.get("target_outcome_change"), 1.0),
        candidate_nodes=candidate_nodes,
        max_plans=_coerce_int(params.get("max_plans"), default=5, minimum=1),
        min_effect_threshold=max(0.0, _coerce_float(params.get("min_effect_threshold"), 0.0001)),
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "goal-seek-intervention",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="budgeted_intervention_optimizer",
    request_model=BudgetedInterventionOptimizerRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Optimize intervention allocation under a fixed budget to improve or dampen "
        "a target outcome."
    ),
)
def budgeted_intervention_optimizer(
    payload: BudgetedInterventionOptimizerRequest,
    request: Request,
) -> dict:
    del request
    params = payload.params
    outcome_node = params.get("outcome_node")
    if not isinstance(outcome_node, str) or not outcome_node:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "budgeted-intervention-optimizer",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": {"message": "`params.outcome_node` is required and must be a string."},
        }
    candidate_nodes, error = _parse_optional_str_list(params.get("candidate_nodes"), "candidate_nodes")
    if error:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "budgeted-intervention-optimizer",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": error,
        }
    result = toy_graph.budgeted_intervention_optimizer_report(
        outcome_node,
        _coerce_float(params.get("budget"), 1.0),
        objective=str(params.get("objective", "increase")),
        candidate_nodes=candidate_nodes,
        max_allocations=_coerce_int(params.get("max_allocations"), default=3, minimum=1),
        min_effect_threshold=max(0.0, _coerce_float(params.get("min_effect_threshold"), 0.0001)),
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "budgeted-intervention-optimizer",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="pareto_intervention_frontier",
    request_model=ParetoInterventionFrontierRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Return non-dominated intervention candidates on impact-vs-disruption frontier."
    ),
)
def pareto_intervention_frontier(
    payload: ParetoInterventionFrontierRequest,
    request: Request,
) -> dict:
    del request
    params = payload.params
    outcome_node = params.get("outcome_node")
    if not isinstance(outcome_node, str) or not outcome_node:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "pareto-intervention-frontier",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": {"message": "`params.outcome_node` is required and must be a string."},
        }
    candidate_nodes, error = _parse_optional_str_list(params.get("candidate_nodes"), "candidate_nodes")
    if error:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "pareto-intervention-frontier",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": error,
        }
    result = toy_graph.pareto_intervention_frontier_report(
        outcome_node,
        _coerce_float(params.get("intervention_delta"), 1.0),
        objective=str(params.get("objective", "increase")),
        candidate_nodes=candidate_nodes,
        min_effect_threshold=max(0.0, _coerce_float(params.get("min_effect_threshold"), 0.0001)),
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "pareto-intervention-frontier",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="scenario_compare",
    request_model=ScenarioCompareRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Compare multiple intervention scenarios on outcome lift and aggregate disruption."
    ),
)
def scenario_compare(payload: ScenarioCompareRequest, request: Request) -> dict:
    del request
    params = payload.params
    raw_scenarios = params.get("scenarios")
    if not isinstance(raw_scenarios, list) or not raw_scenarios:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "scenario-compare",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": {
                "message": "`params.scenarios` is required and must be a non-empty array."
            },
        }
    scenarios = [item for item in raw_scenarios if isinstance(item, dict)]
    if not scenarios:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "scenario-compare",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": {"message": "`params.scenarios` must contain scenario objects."},
        }
    outcome_node = params.get("outcome_node") if isinstance(params.get("outcome_node"), str) else None
    result = toy_graph.scenario_compare_report(
        scenarios,
        outcome_node=outcome_node,
        min_effect_threshold=max(0.0, _coerce_float(params.get("min_effect_threshold"), 0.0001)),
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "scenario-compare",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="shock_cascade_simulation",
    request_model=ShockCascadeSimulationRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Simulate multi-step shock propagation from a target node with optional damping/noise."
    ),
)
def shock_cascade_simulation(
    payload: ShockCascadeSimulationRequest,
    request: Request,
) -> dict:
    del request
    params = payload.params
    target_node = params.get("target_node")
    if not isinstance(target_node, str) or not target_node:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "shock-cascade-simulation",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": {"message": "`params.target_node` is required and must be a string."},
        }
    random_seed = (
        _coerce_int(params.get("random_seed"), default=0, minimum=0)
        if params.get("random_seed") is not None
        else None
    )
    result = toy_graph.shock_cascade_simulation_report(
        target_node,
        _coerce_float(params.get("shock_delta"), 1.0),
        steps=_coerce_int(params.get("steps"), default=3, minimum=0),
        damping=_coerce_float(params.get("damping"), 0.6),
        min_effect_threshold=max(0.0, _coerce_float(params.get("min_effect_threshold"), 0.0001)),
        noise_scale=max(0.0, _coerce_float(params.get("noise_scale"), 0.0)),
        random_seed=random_seed,
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "shock-cascade-simulation",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="resilience_report",
    request_model=ResilienceReportRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Estimate graph resilience under single-node and single-edge failure stress tests."
    ),
)
def resilience_report(payload: ResilienceReportRequest, request: Request) -> dict:
    del request
    params = payload.params
    result = toy_graph.resilience_report(
        top_k=_coerce_int(params.get("top_k"), default=5, minimum=1),
        min_effect_threshold=max(0.0, _coerce_float(params.get("min_effect_threshold"), 0.0001)),
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "resilience-report",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="target_vulnerability_report",
    request_model=TargetVulnerabilityReportRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Rank upstream source nodes by vulnerability contribution to a target node."
    ),
)
def target_vulnerability_report(
    payload: TargetVulnerabilityReportRequest,
    request: Request,
) -> dict:
    del request
    params = payload.params
    target_node = params.get("target_node")
    if not isinstance(target_node, str) or not target_node:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "target-vulnerability-report",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": {"message": "`params.target_node` is required and must be a string."},
        }
    candidate_sources, error = _parse_optional_str_list(
        params.get("candidate_sources"),
        "candidate_sources",
    )
    if error:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "target-vulnerability-report",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": error,
        }
    result = toy_graph.target_vulnerability_report(
        target_node,
        shock_delta=_coerce_float(params.get("shock_delta"), 1.0),
        candidate_sources=candidate_sources,
        top_k=_coerce_int(params.get("top_k"), default=5, minimum=1),
        min_effect_threshold=max(0.0, _coerce_float(params.get("min_effect_threshold"), 0.0001)),
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "target-vulnerability-report",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="bottleneck_report",
    request_model=BottleneckReportRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Identify structural bottleneck nodes/edges with highest path participation."
    ),
)
def bottleneck_report(payload: BottleneckReportRequest, request: Request) -> dict:
    del request
    params = payload.params
    result = toy_graph.bottleneck_report(
        top_k=_coerce_int(params.get("top_k"), default=5, minimum=1),
        max_paths_per_pair=_coerce_int(params.get("max_paths_per_pair"), default=20, minimum=1),
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "bottleneck-report",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="influence_matrix",
    request_model=InfluenceMatrixRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Return a full source-target influence matrix with in/out strength summaries."
    ),
)
def influence_matrix(payload: InfluenceMatrixRequest, request: Request) -> dict:
    del request
    params = payload.params
    node_ids, error = _parse_optional_str_list(params.get("node_ids"), "node_ids")
    if error:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "influence-matrix",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": error,
        }
    result = toy_graph.influence_matrix_report(node_ids=node_ids)
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "influence-matrix",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="intervention_battle",
    request_model=InterventionBattleRequest,
    response_model=GenericExampleExtensionResponse,
    description=(
        "Compare two intervention plans head-to-head on outcome impact and market disruption."
    ),
)
def intervention_battle(payload: InterventionBattleRequest, request: Request) -> dict:
    del request
    params = payload.params
    outcome_node = params.get("outcome_node")
    if not isinstance(outcome_node, str) or not outcome_node:
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "intervention-battle",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": {"message": "`params.outcome_node` is required and must be a string."},
        }
    plan_a = params.get("plan_a")
    plan_b = params.get("plan_b")
    if not isinstance(plan_a, dict) or not isinstance(plan_b, dict):
        return {
            "cap_version": payload.cap_version,
            "request_id": payload.request_id or "intervention-battle",
            "verb": payload.verb,
            "status": "invalid_request",
            "result": {"message": "`params.plan_a` and `params.plan_b` are required objects."},
        }
    result = toy_graph.intervention_battle_report(
        outcome_node,
        plan_a,
        plan_b,
        min_effect_threshold=max(0.0, _coerce_float(params.get("min_effect_threshold"), 0.0001)),
        disruption_penalty=max(0.0, _coerce_float(params.get("disruption_penalty"), 1.0)),
    )
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "intervention-battle",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


@registry.extension(
    namespace="example",
    name="verb_catalog",
    request_model=VerbCatalogRequest,
    response_model=VerbCatalogResponse,
    description=(
        "Return a usage-oriented catalog for all mounted CAP verbs, including "
        "examples and extension groupings."
    ),
)
def verb_catalog(payload: VerbCatalogRequest, request: Request) -> dict:
    del request
    detail = "full" if payload.params.detail == "full" else "compact"
    methods = registry.list_methods(
        detail=detail,
        include_examples=payload.params.include_examples,
    )
    method_rows = []
    for item in methods:
        row = item.model_dump(exclude_none=True)
        if payload.params.include_examples:
            existing = row.get("examples")
            if not isinstance(existing, list) or not existing:
                row["examples"] = [_example_request_for_verb(row.get("verb", ""))]
        method_rows.append(row)
    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "verb-catalog",
        "verb": payload.verb,
        "status": "success",
        "result": {
            "summary": {
                "core_count": len(registry.verbs_for_surface("core")),
                "convenience_count": len(registry.verbs_for_surface("convenience")),
                "extension_count": sum(
                    len(verbs) for verbs in registry.extension_verbs_by_namespace.values()
                ),
            },
            "supported_verbs": {
                "core": registry.verbs_for_surface("core"),
                "convenience": registry.verbs_for_surface("convenience"),
                "extensions": registry.extension_verbs_by_namespace,
            },
            "methods": method_rows,
        },
    }


def _example_request_for_verb(verb: str) -> dict:
    base = {
        "cap_version": "0.2.2",
        "request_id": f"example-{verb.replace('.', '-')}",
        "verb": verb,
    }
    params_by_verb = {
        "meta.methods": {"detail": "compact"},
        "observe.predict": {"target_node": "revenue"},
        "intervene.do": {
            "treatment_node": "marketing_spend",
            "treatment_value": 1.0,
            "outcome_node": "revenue",
        },
        "graph.neighbors": {"node_id": "demand", "scope": "parents", "max_neighbors": 5},
        "graph.markov_blanket": {"node_id": "demand", "max_neighbors": 20},
        "graph.paths": {
            "source_node_id": "marketing_spend",
            "target_node_id": "revenue",
            "max_paths": 5,
            "max_depth": 10,
            "directed": True,
        },
        "traverse.parents": {"node_id": "demand", "top_k": 5},
        "traverse.children": {"node_id": "marketing_spend", "top_k": 5},
        "extensions.example.connectivity_report": {
            "source_node_id": "product_quality",
            "target_node_id": "revenue",
            "max_paths": 10,
        },
        "extensions.example.path_contribution_report": {
            "source_node_id": "product_quality",
            "target_node_id": "revenue",
            "max_paths": 10,
        },
        "extensions.example.market_impact": {
            "target_node": "marketing_spend",
            "intervention_delta": 1.0,
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.node_systemic_risk": {
            "node_id": "product_quality",
            "stress_delta": 1.0,
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.multi_intervention_impact": {
            "interventions": [
                {"target_node": "marketing_spend", "intervention_delta": 1.0},
                {"target_node": "product_quality", "intervention_delta": 1.0},
            ],
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.intervention_ranking": {
            "outcome_node": "revenue",
            "intervention_delta": 1.0,
            "candidate_nodes": ["marketing_spend", "product_quality", "demand"],
            "top_k": 3,
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.node_criticality_ranking": {
            "candidate_nodes": ["marketing_spend", "product_quality", "demand", "retention"],
            "stress_delta": 1.0,
            "top_k": 3,
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.edge_criticality_ranking": {
            "top_k": 3,
        },
        "extensions.example.goal_seek_intervention": {
            "outcome_node": "revenue",
            "target_outcome_change": 3.0,
            "candidate_nodes": ["marketing_spend", "product_quality", "demand"],
            "max_plans": 3,
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.budgeted_intervention_optimizer": {
            "outcome_node": "revenue",
            "budget": 2.0,
            "objective": "increase",
            "candidate_nodes": ["marketing_spend", "product_quality", "demand"],
            "max_allocations": 2,
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.pareto_intervention_frontier": {
            "outcome_node": "revenue",
            "intervention_delta": 1.0,
            "objective": "increase",
            "candidate_nodes": ["marketing_spend", "product_quality", "demand", "retention"],
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.scenario_compare": {
            "outcome_node": "revenue",
            "scenarios": [
                {
                    "name": "growth_push",
                    "interventions": [
                        {"target_node": "marketing_spend", "intervention_delta": 1.5},
                        {"target_node": "product_quality", "intervention_delta": 0.5},
                    ],
                },
                {
                    "name": "quality_focus",
                    "interventions": [
                        {"target_node": "product_quality", "intervention_delta": 1.5},
                    ],
                },
            ],
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.shock_cascade_simulation": {
            "target_node": "product_quality",
            "shock_delta": 1.0,
            "steps": 3,
            "damping": 0.6,
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.resilience_report": {
            "top_k": 3,
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.target_vulnerability_report": {
            "target_node": "revenue",
            "shock_delta": 1.0,
            "candidate_sources": ["marketing_spend", "product_quality", "demand", "retention"],
            "top_k": 3,
            "min_effect_threshold": 0.0001,
        },
        "extensions.example.bottleneck_report": {
            "top_k": 3,
            "max_paths_per_pair": 20,
        },
        "extensions.example.influence_matrix": {
            "node_ids": ["marketing_spend", "product_quality", "demand", "retention", "revenue"],
        },
        "extensions.example.intervention_battle": {
            "outcome_node": "revenue",
            "plan_a": {
                "name": "acquisition_heavy",
                "interventions": [
                    {"target_node": "marketing_spend", "intervention_delta": 1.5},
                ],
            },
            "plan_b": {
                "name": "product_heavy",
                "interventions": [
                    {"target_node": "product_quality", "intervention_delta": 1.0},
                ],
            },
            "min_effect_threshold": 0.0001,
            "disruption_penalty": 1.0,
        },
        "extensions.example.dataset_density": {},
        "extensions.example.verb_catalog": {"detail": "compact", "include_examples": True},
        "extensions.market.parse_request": {
            "request": {
                "cap_version": "0.2.2",
                "verb": "graph.neighbors",
                "params": {"node_id": "demand", "scope": "parents", "max_neighbors": 5},
            }
        },
        "extensions.market.batch_execute": {
            "requests": [
                {
                    "cap_version": "0.2.2",
                    "verb": "observe.predict",
                    "params": {"target_node": "revenue"},
                },
                {
                    "cap_version": "0.2.2",
                    "verb": "graph.neighbors",
                    "params": {"node_id": "demand", "scope": "parents", "max_neighbors": 5},
                },
            ]
        },
        "extensions.market.interpret_request": {
            "request": {
                "cap_version": "0.2.2",
                "verb": "graph.neighbors",
                "params": {"node_id": "demand", "scope": "parents", "max_neighbors": 5},
            }
        },
    }
    params = params_by_verb.get(verb)
    if params is not None:
        base["params"] = params
    return base


@registry.extension(
    namespace="market",
    name="parse_request",
    request_model=MarketParseRequest,
    response_model=MarketParseResponse,
    description=(
        "Parse an embedded CAP request and return extracted nodes plus mapped "
        "graph-computer function plan."
    ),
)
def market_parse_request(payload: MarketParseRequest, request: Request) -> dict:
    del request
    effective_options = runtime_config.merge_market_options(payload.options)
    try:
        parsed = parse_cap_request(payload.params.request)
        result = {
            "parsed_request": {
                "verb": parsed.verb,
                "node_ids": parsed.node_ids,
                "params": parsed.params,
            },
            "function_plan": get_cap_function_plan(parsed.verb),
            "effective_options": effective_options,
        }
        status = "success"
    except ValueError as error:
        result = {
            "parse_error": str(error),
            "effective_options": effective_options,
        }
        status = "invalid_request"

    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "market-parse-request",
        "verb": payload.verb,
        "status": status,
        "result": result,
    }


@registry.extension(
    namespace="market",
    name="batch_execute",
    request_model=MarketBatchRequest,
    response_model=MarketBatchResponse,
    description=(
        "Execute multiple embedded CAP requests through the market pipeline and "
        "return per-request stage outcomes."
    ),
)
async def market_batch_execute(payload: MarketBatchRequest, request: Request) -> dict:
    del request
    effective_options = runtime_config.merge_market_options(payload.options)
    items: list[dict] = []
    success_count = 0

    for index, embedded in enumerate(payload.params.requests):
        run_result = await market_interpretation.interpret_cap_request(
            embedded,
            options=effective_options,
        )
        stages = run_result.get("stages", {}) if isinstance(run_result, dict) else {}
        stage_statuses = {
            stage_name: stage_payload.get("status", "unknown")
            for stage_name, stage_payload in stages.items()
            if isinstance(stage_payload, dict)
        }
        stage_ok = (
            stage_statuses.get("parse") == "success"
            and stage_statuses.get("graph_operations") == "success"
            and stage_statuses.get("calculation") == "success"
            and stage_statuses.get("postprocess") == "success"
            and stage_statuses.get("analysis") == "success"
        )
        if stage_ok:
            success_count += 1
        items.append(
            {
                "index": index,
                "request": embedded,
                "stage_statuses": stage_statuses,
                "ok": stage_ok,
                "result": run_result,
            }
        )
        if payload.params.stop_on_error and not stage_ok:
            break

    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "market-batch-execute",
        "verb": payload.verb,
        "status": "success",
        "result": {
            "total_requests": len(payload.params.requests),
            "executed_requests": len(items),
            "success_count": success_count,
            "failure_count": len(items) - success_count,
            "stopped_early": (
                payload.params.stop_on_error and len(items) < len(payload.params.requests)
            ),
            "items": items,
        },
    }


@registry.extension(
    namespace="market",
    name="interpret_request",
    request_model=MarketInterpretRequest,
    response_model=MarketInterpretResponse,
    description=(
        "Parse an embedded CAP request and return staged graph/calc/postprocess/analysis "
        "results for market nodes."
    ),
)
async def market_interpret_request(payload: MarketInterpretRequest, request: Request) -> dict:
    del request
    effective_options = runtime_config.merge_market_options(payload.options)
    result = await market_interpretation.interpret_cap_request(
        payload.params.request,
        options=effective_options,
    )
    debug_cfg = (
        effective_options.get("debug", {})
        if isinstance(effective_options, dict)
        else {}
    )
    if isinstance(result, dict) and isinstance(debug_cfg, dict):
        if bool(debug_cfg.get("include_runtime_config", False)):
            result["runtime_config"] = runtime_config.runtime_config_debug_view()

    return {
        "cap_version": payload.cap_version,
        "request_id": payload.request_id or "market-interpret-request",
        "verb": payload.verb,
        "status": "success",
        "result": result,
    }


async def provenance_context_provider(
    payload: object,
    request: Request,
) -> CAPProvenanceContext:
    del payload, request
    return CAPProvenanceContext(
        graph_version=toy_graph.GRAPH_VERSION,
        graph_timestamp=None,
        server_name="cap-example",
        server_version=__version__,
    )


dispatch = build_fastapi_cap_dispatcher(
    registry=registry,
    provenance_context_provider=provenance_context_provider,
)

app = FastAPI(
    title="CAP Example Server",
    version=__version__,
    description="Neutral CAP example over a synthetic in-memory graph.",
)
register_cap_exception_handlers(app)
app.state.cap_service = ExampleService(public_base_url="http://127.0.0.1:8000")


@app.get("/")
def root() -> dict:
    return {
        "name": "cap-example",
        "version": __version__,
        "docs": "/docs",
    }


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "app_name": "cap-example",
        "version": __version__,
    }


@app.get("/debug/runtime-config")
def debug_runtime_config() -> dict:
    return runtime_config.runtime_config_debug_view()


@app.get("/.well-known/cap.json")
def well_known_cap(request: Request) -> dict:
    service = ExampleService(public_base_url=str(request.base_url).rstrip("/"))
    return service.capability_card().model_dump(exclude_none=True, by_alias=True)


@app.post("/cap")
async def cap_endpoint(payload: dict, request: Request) -> dict:
    request.app.state.cap_service = ExampleService(public_base_url=str(request.base_url).rstrip("/"))
    return await dispatch(payload, request)
