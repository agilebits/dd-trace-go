package httptrace

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DataDog/dd-trace-go/tracer"
	"github.com/DataDog/dd-trace-go/tracer/tracertest"
	"github.com/stretchr/testify/assert"
)

func TestHttpTracerDisabled(t *testing.T) {
	assert := assert.New(t)

	testTracer, testTransport := tracertest.GetTestTracer()
	handler := NewTraceHandler(http.NewServeMux(), "service", testTracer)
	testTracer.SetEnabled(false) // the key line in this test.

	// make the request
	req := httptest.NewRequest("GET", "/disabled", nil)
	writer := httptest.NewRecorder()
	handler.ServeHTTP(writer, req)
	assert.Equal(writer.Code, 200)
	assert.Equal(writer.Body.String(), "disabled!")

	// assert nothing was traced.
	testTracer.ForceFlush()
	traces := testTransport.Traces()
	assert.Len(traces, 0)
}

func TestHttpTracer200(t *testing.T) {
	assert := assert.New(t)

	// setup
	tracer, transport, router := setup(t)

	// Send and verify a 200 request
	url := "/200"
	req := httptest.NewRequest("GET", url, nil)
	writer := httptest.NewRecorder()
	router.ServeHTTP(writer, req)
	assert.Equal(writer.Code, 200)
	assert.Equal(writer.Body.String(), "200!")

	// ensure properly traced
	tracer.ForceFlush()
	traces := transport.Traces()
	assert.Len(traces, 1)
	spans := traces[0]
	assert.Len(spans, 1)

	s := spans[0]
	assert.Equal(s.Name, "http.request")
	assert.Equal(s.Service, "my-service")
	assert.Equal(s.Resource, "GET "+url)
	assert.Equal(s.GetMeta("http.status_code"), "200")
	assert.Equal(s.GetMeta("http.method"), "GET")
	assert.Equal(s.GetMeta("http.url"), url)
	assert.Equal(s.Error, int32(0))
}

func TestHttpTracer500(t *testing.T) {
	assert := assert.New(t)

	// setup
	tracer, transport, router := setup(t)

	// SEnd and verify a 200 request
	url := "/500"
	req := httptest.NewRequest("GET", url, nil)
	writer := httptest.NewRecorder()
	router.ServeHTTP(writer, req)
	assert.Equal(writer.Code, 500)
	assert.Equal(writer.Body.String(), "500!\n")

	// ensure properly traced
	tracer.ForceFlush()
	traces := transport.Traces()
	assert.Len(traces, 1)
	spans := traces[0]
	assert.Len(spans, 1)

	s := spans[0]
	assert.Equal(s.Name, "http.request")
	assert.Equal(s.Service, "my-service")
	assert.Equal(s.Resource, "GET "+url)
	assert.Equal(s.GetMeta("http.status_code"), "500")
	assert.Equal(s.GetMeta("http.method"), "GET")
	assert.Equal(s.GetMeta("http.url"), url)
	assert.Equal(s.Error, int32(1))
}

// test handlers

func handler200(t *testing.T) http.HandlerFunc {
	assert := assert.New(t)
	return func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("200!"))
		assert.Nil(err)
		span := tracer.SpanFromContextDefault(r.Context())
		assert.Equal(span.Service, "my-service")
		assert.Equal(span.Duration, int64(0))
	}
}

func handler500(t *testing.T) http.HandlerFunc {
	assert := assert.New(t)
	return func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "500!", http.StatusInternalServerError)
		span := tracer.SpanFromContextDefault(r.Context())
		assert.Equal(span.Service, "my-service")
		assert.Equal(span.Duration, int64(0))
	}
}

func setup(t *testing.T) (*tracer.Tracer, *dummyTransport, *http.ServeMux) {
	tracer, transport, ht := getTestTracer("my-service")

	mux := http.NewServeMux()

	h200 := handler200(t)
	h500 := handler500(t)

	mux.Handle("/200", ht.Handler(h200))
	mux.Handle("/500", ht.Handler(h500))

	return tracer, transport, mux
}
