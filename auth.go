package s2

import (
	"net/http"
)

// AuthController is an interface defining authentication
type AuthController interface {
	SecretKey(r *http.Request, accessKey string, region *string) (*string, error)
	CustomAuth(r *http.Request) (bool, error)
}
