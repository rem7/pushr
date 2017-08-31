/*
 * Copyright (c) 2016 Yanko Bolanos
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */
package main

import (
	"log"
)

func LoadParserPlugin(pluginPath string) Parser {
	var parser Parser
	log.Fatalf("plugins not supported on mac")
	return parser
}
