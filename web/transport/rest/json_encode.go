package rest

import (
	"context"
	"encoding/json"
	"net/http"
)

// EncodeJSONResponse encode a response as json
func EncodeJSONResponse(_ context.Context, w http.ResponseWriter, response interface{}) error {
	return json.NewEncoder(w).Encode(response)
}
