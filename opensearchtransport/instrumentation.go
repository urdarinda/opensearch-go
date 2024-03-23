// SPDX-License-Identifier: Apache-2.0
//
// The OpenSearch Contributors require contributions made to
// this file be licensed under the Apache-2.0 license or a
// compatible open source license.
//
// Modifications Copyright OpenSearch Contributors. See
// GitHub history for details.

package opensearchtransport

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const schemaUrl = "https://opentelemetry.io/schemas/1.21.0"
const tracerName = "opensearch-api"

// Constants for Semantic Convention
// see https://opentelemetry.io/docs/specs/semconv/database/elasticsearch/ for details.
const attrDbSystem = "db.system"
const attrDbStatement = "db.statement"
const attrDbOperation = "db.operation"
const attrHttpRequestMethod = "http.request.method"
const attrUrlFull = "url.full"
const attrServerAddress = "server.address"
const attrServerPort = "server.port"

// Instrumentation defines the interface the client uses to propagate information about the requests.
// Each method is called with the current context or request for propagation.
type Instrumentation interface {
	// Start creates the span before building the request, returned context will be propagated to the request by the client.
	Start(ctx context.Context, operation string) context.Context

	// Close will be called once the client has returned.
	Close(ctx context.Context)

	// RecordError propagates an error.
	RecordError(ctx context.Context, err error)

	// RecordRequestBody records the current request payload.
	RecordRequestBody(ctx context.Context, endpoint string, query io.Reader) io.ReadCloser

	// BeforeRequest provides the request called before sending to the server.
	BeforeRequest(req *http.Request)

	// AfterRequest provides the request
	// Called after the request has been enhanced with the information from the transport and sent to the server.
	AfterRequest(req *http.Request)

	// AfterResponse provides the response.
	AfterResponse(ctx context.Context, res *http.Response)
}

type OpensearchOpenTelemetry struct {
	tracer     trace.Tracer
	recordBody bool
}

// NewOtelInstrumentation returns a new instrument for Open Telemetry traces
// If no provider is passed, the instrumentation will fall back to the global otel provider.
// captureSearchBody sets the query capture behavior for search endpoints.
// version should be set to the version provided by the caller.
func NewOtelInstrumentation(provider trace.TracerProvider, captureSearchBody bool, version string) *OpensearchOpenTelemetry {
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	return &OpensearchOpenTelemetry{
		tracer: provider.Tracer(
			tracerName,
			trace.WithInstrumentationVersion(version),
			trace.WithSchemaURL(schemaUrl),
		),
		recordBody: captureSearchBody,
	}
}

// Start begins a new span in the given context with the provided operation.
// Span will always have a kind set to trace.SpanKindClient.
// The context span aware is returned for use within the client.
func (i OpensearchOpenTelemetry) Start(ctx context.Context, operation string) context.Context {
	newCtx, _ := i.tracer.Start(ctx, operation,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
				attribute.String(attrDbSystem, "opensearch"),
				attribute.String(attrDbOperation, operation),
		),
	)
	return newCtx
}

// Close call for the end of the span, preferably defered by the client once started.
func (i OpensearchOpenTelemetry) Close(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.End()
	}
}

// shouldRecordRequestBody filters for search endpoints.
func (i OpensearchOpenTelemetry) shouldRecordRequestBody(endpoint string) bool {
	// allow list of endpoints that will propagate query to OpenTelemetry.
	// see https://opentelemetry.io/docs/specs/semconv/database/elasticsearch/#call-level-attributes
	var searchEndpoints = map[string]struct{}{
		"search":                 {},
		"msearch":                {},
		"terms_enum":             {},
		"search_template":        {},
		"msearch_template":       {},
		"render_search_template": {},
	}

	if i.recordBody {
		if _, ok := searchEndpoints[endpoint]; ok {
			return true
		}
	}
	return false
}

// RecordRequestBody add the db.statement attributes only for search endpoints.
// Returns a new reader if the query has been recorded, nil otherwise.
func (i OpensearchOpenTelemetry) RecordRequestBody(ctx context.Context, endpoint string, query io.Reader) io.ReadCloser {
	if !i.shouldRecordRequestBody(endpoint) {
		return nil
	}

	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		buf := bytes.Buffer{}
		buf.ReadFrom(query)
		span.SetAttributes(attribute.String(attrDbStatement, buf.String()))
		getBody := func() (io.ReadCloser, error) {
			reader := buf
			return io.NopCloser(&reader), nil
		}
		reader, _ := getBody()
		return reader
	}

	return nil
}

// RecordError sets any provided error as an OTel error in the active span.
func (i OpensearchOpenTelemetry) RecordError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetStatus(codes.Error, "an error happened while executing a request")
		span.RecordError(err)
	}
}

// BeforeRequest noop for interface.
func (i OpensearchOpenTelemetry) BeforeRequest(req *http.Request) {}

// AfterRequest enrich the span with the available data from the request.
func (i OpensearchOpenTelemetry) AfterRequest(req *http.Request) {
	span := trace.SpanFromContext(req.Context())
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String(attrHttpRequestMethod, req.Method),
			attribute.String(attrUrlFull, req.URL.String()),
			attribute.String(attrServerAddress, req.URL.Hostname()),
		)
		if value, err := strconv.ParseInt(req.URL.Port(), 10, 32); err == nil {
			span.SetAttributes(attribute.Int64(attrServerPort, value))
		}
	}
}

// AfterResponse noop for interface.
func (i OpensearchOpenTelemetry) AfterResponse(ctx context.Context, res *http.Response) {}