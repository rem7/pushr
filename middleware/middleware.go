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

	for _, key := range a.apiKeys {
		if key == apikey {
			next(rw, req)
			return
		}
	}
	rw.WriteHeader(http.StatusForbidden)
}
