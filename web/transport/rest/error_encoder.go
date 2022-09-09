package rest

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/owlint/maestro/errors"
)

// encode errors from business-logic
func EncodeError(_ context.Context, err error, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	switch err.(type) {
	case errors.ValidationError:
		w.WriteHeader(http.StatusNotAcceptable)
	case errors.NotFoundError:
		w.WriteHeader(http.StatusNotFound)
	case errors.RedisError:
		w.WriteHeader(http.StatusFailedDependency)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": err.Error(),
	})
}
