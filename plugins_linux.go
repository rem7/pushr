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
	"fmt"
	"log"
	"os"
	"plugin"
)

func LoadParserPlugin(pluginPath string) Parser {

	log.Printf("loading plugin:%s", pluginPath)

	plug, err := plugin.Open(pluginPath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	symParser, err := plug.Lookup("Parser")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var parser Parser
	parser, ok := symParser.(Parser)
	if !ok {
		fmt.Println("unexpected type from module symbol")
		os.Exit(1)
	}

	return parser

}
