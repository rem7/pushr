/*
 * Copyright (c) 2016 Yanko Bolanos
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */
package middleware

import (
	"net/http"
)

type Cors struct{}

func (c *Cors) ServeHTTP(rw http.ResponseWriter, req *http.Request, next http.HandlerFunc) {
	rw.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	rw.Header().Set("Access-Control-Allow-Methods", "*")
	rw.Header().Set("Access-Control-Allow-Headers", "*")
	rw.Header().Set("Access-Control-Max-Age", "3600")
	next(rw, req)
}

func NewApiKeyMiddleware(apiKeys []string) *ApiKeyMiddleware {
	return &ApiKeyMiddleware{apiKeys}
}

type ApiKeyMiddleware struct {
	apiKeys []string
}

func (a *ApiKeyMiddleware) ServeHTTP(rw http.ResponseWriter, req *http.Request, next http.HandlerFunc) {

	apikey := req.Header.Get("X-Pushr-ApiKey")
	if apikey == "" {
		apikey = req.URL.Query().Get("apikey")
	}

	if apikey == "" {
		rw.WriteHeader(http.StatusBadRequest)
		http.Error(rw, "api key missing", http.StatusBadRequest)
		return
	}
	valid := false
	for _, key := range a.apiKeys {
		if key == apikey {
			valid = true
		}
	}

	if valid {
		next(rw, req)
	} else {
		rw.WriteHeader(http.StatusForbidden)
	}

}
