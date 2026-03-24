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
from example_cap_server import toy_graph


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


registry = CAPVerbRegistry()


def get_service(request: Request) -> ExampleService:
    return cast(ExampleService, request.app.state.cap_service)


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
    ids = toy_graph.markov_blanket(params.node_id)
    limited = ids[: params.max_neighbors]
    return CAPHandlerSuccessSpec(
        result={
            "node_id": params.node_id,
            "neighbors": [{"node_id": node_id, "roles": ["parent"]} for node_id in limited],
            "total_candidate_count": len(ids),
            "truncated": len(limited) < len(ids),
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


@app.get("/.well-known/cap.json")
def well_known_cap(request: Request) -> dict:
    service = ExampleService(public_base_url=str(request.base_url).rstrip("/"))
    return service.capability_card().model_dump(exclude_none=True, by_alias=True)


@app.post("/cap")
async def cap_endpoint(payload: dict, request: Request) -> dict:
    request.app.state.cap_service = ExampleService(public_base_url=str(request.base_url).rstrip("/"))
    return await dispatch(payload, request)
