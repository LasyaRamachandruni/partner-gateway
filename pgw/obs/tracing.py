"""Distributed tracing with OpenTelemetry.

One trace follows a partner's command end to end:

    partner HTTP request ─► gateway span ─► gRPC SendCommand (metadata) ─► vehicle service span
         ─► car executes ─► result event (Pub/Sub attributes) ─► gateway consumer span ─► webhook (header)

Context crosses each hop as a W3C `traceparent`: in HTTP headers, gRPC metadata,
message attributes and webhook headers. The helpers below inject it into and extract
it from a plain dict, so each transport needs only a line or two.

`setup()` installs a tracer provider. Tests pass an in-memory exporter to inspect
spans. Production would send them over OTLP to a collector (Cloud Trace, Jaeger, Tempo).
"""

from __future__ import annotations

from opentelemetry import context, propagate, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter

_provider: TracerProvider | None = None


def setup(service: str = "partner-gateway", exporter: SpanExporter | None = None) -> TracerProvider:
    """Install a tracer provider once per process; later calls only add exporters."""
    global _provider
    if _provider is None:
        _provider = TracerProvider(resource=Resource.create({"service.name": service}))
        trace.set_tracer_provider(_provider)
    if exporter is not None:
        _provider.add_span_processor(SimpleSpanProcessor(exporter))
    return _provider


def tracer(name: str):
    return trace.get_tracer(name)


def inject(carrier: dict | None = None) -> dict:
    carrier = {} if carrier is None else carrier
    propagate.inject(carrier)
    return carrier


def extract(carrier: dict | None) -> context.Context:
    return propagate.extract(carrier or {})


def current_trace_id() -> str | None:
    ctx = trace.get_current_span().get_span_context()
    return format(ctx.trace_id, "032x") if ctx.is_valid else None
