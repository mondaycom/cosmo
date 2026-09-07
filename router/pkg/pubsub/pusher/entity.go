package pusher

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/wundergraph/cosmo/router/pkg/config"
)

// entityMapper reduces a channel payload to an entity representation, keyed by the
// subscription root field the payload was delivered for.
//
// monday publishes change notifications ("project_name_change" carries name,
// pulse_id, board_id, ...), whose keys are not the fields of the federated type. The
// resolver, however, only needs the entity key: given
// {"__typename":"Board","id":"5002284778"} it resolves every requested field from the
// subgraph that owns Board. So a mapping rewrites
//
//	{"name":"...","pulse_id":2536911968,"board_id":5002284778,...}
//
// into
//
//	{"__typename":"Board","id":"5002284778"}
type entityMapper struct {
	byField map[string]config.PusherEntityMapping
}

func newEntityMapper(mappings []config.PusherEntityMapping) (*entityMapper, error) {
	if len(mappings) == 0 {
		return nil, nil
	}

	byField := make(map[string]config.PusherEntityMapping, len(mappings))
	for _, mapping := range mappings {
		if mapping.FieldName == "" {
			return nil, fmt.Errorf("pusher: an entity mapping is missing field_name")
		}
		if mapping.TypeName == "" {
			return nil, fmt.Errorf("pusher: entity mapping for field %q is missing type_name", mapping.FieldName)
		}
		if len(mapping.IDFrom) == 0 {
			return nil, fmt.Errorf("pusher: entity mapping for field %q is missing id_from", mapping.FieldName)
		}
		if mapping.KeyField == "" {
			mapping.KeyField = "id"
		}
		if _, exists := byField[mapping.FieldName]; exists {
			return nil, fmt.Errorf("pusher: duplicate entity mapping for field %q", mapping.FieldName)
		}
		byField[mapping.FieldName] = mapping
	}

	return &entityMapper{byField: byField}, nil
}

// mapEvent returns the representation for the given root field. It returns the
// payload unchanged when no mapping is configured for the field.
func (m *entityMapper) mapEvent(fieldName string, payload []byte) ([]byte, error) {
	if m == nil {
		return payload, nil
	}
	mapping, ok := m.byField[fieldName]
	if !ok {
		return payload, nil
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, fmt.Errorf("pusher: payload for field %q is not a JSON object, so it cannot be mapped to %s: %w",
			fieldName, mapping.TypeName, err)
	}

	for _, path := range mapping.IDFrom {
		value, found := lookupPath(decoded, path)
		if !found {
			continue
		}
		id, err := scalarToString(value)
		if err != nil {
			return nil, fmt.Errorf("pusher: %q in the payload for field %q cannot be used as %s.%s: %w",
				path, fieldName, mapping.TypeName, mapping.KeyField, err)
		}
		return json.Marshal(map[string]string{
			"__typename":     mapping.TypeName,
			mapping.KeyField: id,
		})
	}

	return nil, fmt.Errorf("pusher: the payload for field %q contains none of %s, so no %s key could be derived",
		fieldName, strings.Join(mapping.IDFrom, ", "), mapping.TypeName)
}

// lookupPath resolves a dot-separated path in a decoded JSON object. A null value
// counts as absent, so the next candidate key is tried.
func lookupPath(object map[string]any, path string) (any, bool) {
	current := object
	segments := strings.Split(path, ".")

	for i, segment := range segments {
		value, ok := current[segment]
		if !ok || value == nil {
			return nil, false
		}
		if i == len(segments)-1 {
			return value, true
		}
		nested, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		current = nested
	}

	return nil, false
}

// scalarToString renders a JSON scalar as the string an ID field expects. IDs arrive
// as JSON numbers in monday's payloads, and json.Unmarshal decodes those into
// float64, so an integer is formatted without an exponent or a fractional part.
func scalarToString(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	case json.Number:
		return typed.String(), nil
	case bool:
		return strconv.FormatBool(typed), nil
	default:
		return "", fmt.Errorf("unsupported type %T", value)
	}
}
