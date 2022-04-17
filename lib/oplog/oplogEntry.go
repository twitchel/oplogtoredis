package oplog

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/vlasky/oplogtoredis/lib/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	operationInsert  = "i"
	operationUpdate  = "u"
	operationRemove  = "d"
	operationCommand = "c"
)

var metricUnprocessableChangedFields = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "otr",
	Subsystem: "oplog",
	Name:      "unprocessable_changed_fields",
	Help:      "Oplog messages containing data that we couldn't parse to determine changed fields",
})

// Oplog entry after basic processing to extract the document ID that was
// affected
type oplogEntry struct {
	DocID      interface{}
	Timestamp  primitive.Timestamp
	Data       map[string]interface{}
	Operation  string
	Namespace  string
	Database   string
	Collection string

	TxIdx uint
}

// Returns whether this oplogEntry is for an insert
func (op *oplogEntry) IsInsert() bool {
	return op.Operation == operationInsert
}

// Returns whether this oplogEntry is for an update
func (op *oplogEntry) IsUpdate() bool {
	return op.Operation == operationUpdate
}

// Returns whether this oplogEntry is for a remove
func (op *oplogEntry) IsRemove() bool {
	return op.Operation == operationRemove
}

// Returns whether this is an oplog update format v2 update (new in MongoDB 5.0)
func (op *oplogEntry) UpdateIsV2Formatted() bool {
	dataVersion, ok := op.Data["$v"]
	if !ok {
		return false
	}

	// bson unmarshals integers into interface{} differently depending on platform,
	// so we handle any kind of number
	var dataVersionInt int
	switch t := dataVersion.(type) {
	case int:
		dataVersionInt = t
	case int8:
		dataVersionInt = int(t)
	case int16:
		dataVersionInt = int(t)
	case int32:
		dataVersionInt = int(t)
	case int64:
		dataVersionInt = int(t)
	case uint:
		dataVersionInt = int(t)
	case uint8:
		dataVersionInt = int(t)
	case uint16:
		dataVersionInt = int(t)
	case uint32:
		dataVersionInt = int(t)
	case uint64:
		dataVersionInt = int(t)
	case float32:
		dataVersionInt = int(t)
	case float64:
		dataVersionInt = int(t)
	default:
		return false
	}

	if dataVersionInt != 2 {
		return false
	}

	_, ok = op.Data["diff"]
	return ok
}

// If this oplogEntry is for an insert, returns whether that insert is a
// replacement (rather than a modification)
func (op *oplogEntry) UpdateIsReplace() bool {
	if _, ok := op.Data["$set"]; ok {
		return false
	} else if _, ok := op.Data["$unset"]; ok {
		return false
	} else if op.UpdateIsV2Formatted() {
		// the v2 update format is only used for modifications
		return false
	} else {
		return true
	}
}

// Given an operation, returned the fields affected by that operation
func (op *oplogEntry) ChangedFields() []string {
	if op.IsInsert() || (op.IsUpdate() && op.UpdateIsReplace()) {
		return mapKeys(op.Data)
	} else if op.IsUpdate() && op.UpdateIsV2Formatted() {
		// New-style update. Looks like:
		// { $v: 2, diff: { sa: "10", sb: "20", d: { c: true  } }
		diff, ok := op.Data["diff"]
		if !ok {
			metricUnprocessableChangedFields.Inc()
			log.Log.Errorw("Oplog data for non-replacement v2 update did not have a diff field",
				"op", op)
			return []string{}
		}

		diffMap, ok := diff.(map[string]interface{})
		if !ok {
			metricUnprocessableChangedFields.Inc()
			log.Log.Errorw("Oplog data for non-replacement v2 update had a diff that was not a map",
				"op", op)
			return []string{}
		}

		fields := []string{}
		for operationKey, operation := range diffMap {
			if operationKey == "i" || operationKey == "u" || operationKey == "d" {
				// indicates an insert, update, or delete of a whole subtree
				operationMap, operationMapOK := operation.(map[string]interface{})
				if !operationMapOK {
					metricUnprocessableChangedFields.Inc()
					log.Log.Errorw("Oplog data for non-replacement v2 update contained a i/u/d key with a non-map value",
						"op", op)
					continue
				}

				fields = append(fields, mapKeys(operationMap)...)
			} else if strings.HasPrefix(operationKey, "s") {
				// indicates a sub-field set
				fields = append(fields, strings.TrimPrefix(operationKey, "s"))
			} else if operationKey == "a" || strings.HasPrefix(operationKey, "o") {
				// ignore
				continue
			} else {
				metricUnprocessableChangedFields.Inc()
				log.Log.Errorw("Oplog data for non-replacement v2 update contained a field that was not an i/u/d or an s-prefixed field",
					"op", op)
				continue
			}

		}

		return fields
	} else if op.IsUpdate() {
		// Old-style update. Looks like:
		// { $v: 1, $set: { "a": 10, "b": 20 }, $unset: { "c": true } }

		fields := []string{}
		for operationKey, operation := range op.Data {
			if operationKey == "$v" {
				// $v indicates the update document format; it's not a changed key
				continue
			}

			operationMap, operationMapOK := operation.(map[string]interface{})
			if !operationMapOK {
				metricUnprocessableChangedFields.Inc()
				log.Log.Errorw("Oplog data for non-replacement v1 update contained a key with a non-map value",
					"op", op)
				continue
			}

			fields = append(fields, mapKeys(operationMap)...)
		}

		return fields
	}

	return []string{}
}

// Given a map, returns the keys of that map
func mapKeys(m map[string]interface{}) []string {
	fields := make([]string, len(m))

	i := 0
	for key := range m {
		fields[i] = key
		i++
	}

	return fields
}
