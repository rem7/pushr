package middleware

import (
	"net/http"
)

func NewApiKeyMiddleware(apiKeys []string) *ApiKeyMiddleware {
	return &ApiKeyMiddleware{apiKeys}
}

type ApiKeyMiddleware struct {
	apiKeys []string
}

func (a *ApiKeyMiddleware) ServeHTTP(rw http.ResponseWriter, req *http.Request, next http.HandlerFunc) {

	apikey := req.Header.Get("X-Pushr-ApiKey")
	if apikey == "" {
		rw.WriteHeader(http.StatusBadRequest)
		http.Error(rw, "api key missing", http.StatusBadRequest)
		return
	}

	for _, key := range a.apiKeys {
		if key == apikey {
			next(rw, req)
			return
		}
	}
	rw.WriteHeader(http.StatusForbidden)
}
