/*
 * Copyright (c) 2016 Yanko Bolanos
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */
package main

type Parser interface {
	Parse(line string) (map[string]string, error)
	Defaults() map[string]string
	GetTable() []Attribute
	Init(defaults, fieldMappings map[string]string, FieldsOrder []string, defaultTable []Attribute)
}
