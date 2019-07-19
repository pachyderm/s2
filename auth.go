package s2

import (
    "net/http"
)

// AuthController is an interface defining authentication
type AuthController interface {
    SecretKey(r *http.Request, accessKey, region string) (secretKey string, err error)
}
