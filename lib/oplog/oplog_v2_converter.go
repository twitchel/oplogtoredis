package oplog

import "regexp"

// Translated from https://github.com/meteor/meteor/blob/devel/packages/mongo/oplog_v2_converter.js

var arrayIndexOperatorKeyRegex = regexp.MustCompile("^u\\d+")

func isArrayOperator(possibleArrayOperator map[string]interface{}) bool {
	if (possibleArrayOperator == nil) || len(possibleArrayOperator) == 0 {
		return false
	}

	_, hasA := possibleArrayOperator["a"]
	if !hasA {
		return false
	}

	for _, key := range mapKeys(possibleArrayOperator) {
		if key != "a" && !arrayIndexOperatorKeyRegex.MatchString(key) {
			// we have found a field in here that's not valid inside
			// an array operator
			return false
		}
	}

	return true
}
