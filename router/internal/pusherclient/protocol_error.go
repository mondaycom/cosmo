package pusherclient

import (
	"encoding/json"
	"fmt"
)

// ProtocolError is a pusher:error frame.
//
// The code ranges are defined by the Pusher protocol:
//
//	4000-4099 the connection must not be retried with the same parameters
//	4100-4199 reconnect after a backoff
//	4200-4299 reconnect immediately
type ProtocolError struct {
	Code    int
	Message string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("pusher: protocol error %d: %s", e.Code, e.Message)
}

// ShouldReconnect reports whether reconnecting can succeed. Codes below 4100
// signal a client or configuration fault, such as an unknown app key, so
// retrying with the same options is pointless. Codes without a range (0) are
// treated as retryable because they carry no guidance.
func (e *ProtocolError) ShouldReconnect() bool {
	return e.Code < 4000 || e.Code >= 4100
}

func parseProtocolError(raw json.RawMessage) *ProtocolError {
	protoErr := &ProtocolError{}

	var payload struct {
		Code    *int   `json:"code"`
		Message string `json:"message"`
	}
	if err := unmarshalFrameData(raw, &payload); err != nil {
		protoErr.Message = string(raw)
		return protoErr
	}

	if payload.Code != nil {
		protoErr.Code = *payload.Code
	}
	protoErr.Message = payload.Message

	return protoErr
}
