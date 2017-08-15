package httptrace_test

import (
	"fmt"
	"net/http"

	"github.com/DataDog/dd-trace-go/tracer/contrib/net/httptrace"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Hello world!")
}

func Example() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handler)

	httpTracer := httptrace.NewHttpTracer("web-service", nil)
	http.ListenAndServe(":8080", httpTracer.Trace(mux))
}
