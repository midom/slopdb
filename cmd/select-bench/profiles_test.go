package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBenchmarkProfilesRequireOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		mux := http.NewServeMux()
		if enabled {
			installBenchmarkProfiles(mux)
		}
		for _, path := range []string{"/debug/pprof/", "/debug/pprof/allocs", "/debug/pprof/mutex", "/debug/pprof/block"} {
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			want := http.StatusNotFound
			if enabled {
				want = http.StatusOK
			}
			if response.Code != want || response.Body.Len() == 0 {
				t.Fatalf("enabled=%v path=%s status=%d bytes=%d", enabled, path, response.Code, response.Body.Len())
			}
		}
	}
}
