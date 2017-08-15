package httptrace

import (
	"net/http"
	"strconv"

	"github.com/DataDog/dd-trace-go/tracer"
	"github.com/DataDog/dd-trace-go/tracer/ext"
)

type TraceHandler struct {
	tracer  *tracer.Tracer
	handler http.Handler
	service string
}

func NewTraceHandler(h http.Handler, service string, t *tracer.Tracer) *TraceHandler {
	if t == nil {
		t = tracer.DefaultTracer
	}
	t.SetServiceInfo(service, "net/http", ext.AppTypeWeb)
	return &TraceHandler{t, h, service}
}

func (h *TraceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// bail out if tracing isn't enabled.
	if !h.tracer.Enabled() {
		h.ServeHTTP(w, r)
		return
	}

	// trace the request
	tracedRequest, span := h.trace(r)
	defer span.Finish()

	// trace the response
	tracedWriter := newTracedResponseWriter(span, w)

	// run the request
	h.handler.ServeHTTP(tracedWriter, tracedRequest)
}

// span will create a span for the given request.
func (h *TraceHandler) trace(req *http.Request) (*http.Request, *tracer.Span) {
	resource := req.Method + " " + req.URL.Path

	span := h.tracer.NewRootSpan("http.request", h.service, resource)
	span.Type = ext.HTTPType
	span.SetMeta(ext.HTTPMethod, req.Method)
	span.SetMeta(ext.HTTPURL, req.URL.Path)

	// patch the span onto the request context.
	treq := SetRequestSpan(req, span)
	return treq, span
}

// tracedResponseWriter is a small wrapper around an http response writer that will
// intercept and store the status of a request.
type tracedResponseWriter struct {
	span   *tracer.Span
	w      http.ResponseWriter
	status int
}

func newTracedResponseWriter(span *tracer.Span, w http.ResponseWriter) *tracedResponseWriter {
	return &tracedResponseWriter{
		span: span,
		w:    w,
	}
}

func (t *tracedResponseWriter) Header() http.Header {
	return t.w.Header()
}

func (t *tracedResponseWriter) Write(b []byte) (int, error) {
	if t.status == 0 {
		t.WriteHeader(http.StatusOK)
	}
	return t.w.Write(b)
}

func (t *tracedResponseWriter) WriteHeader(status int) {
	t.w.WriteHeader(status)
	t.status = status
	t.span.SetMeta(ext.HTTPCode, strconv.Itoa(status))
	if status >= 500 && status < 600 {
		t.span.Error = 1
	}
}

// SetRequestSpan sets the span on the request's context.
func SetRequestSpan(r *http.Request, span *tracer.Span) *http.Request {
	if r == nil || span == nil {
		return r
	}

	ctx := tracer.ContextWithSpan(r.Context(), span)
	return r.WithContext(ctx)
}

// GetRequestSpan will return the span associated with the given request. It
// will return nil/false if it doesn't exist.
func GetRequestSpan(r *http.Request) (*tracer.Span, bool) {
	span, ok := tracer.SpanFromContext(r.Context())
	return span, ok
}
