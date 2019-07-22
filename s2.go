package s2

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"

	"github.com/gofrs/uuid"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

var bucketNameValidator = regexp.MustCompile(`^/[a-zA-Z0-9\-_\.]{1,255}/`)
var authHeaderValidator = regexp.MustCompile(`^AWS4-HMAC-SHA256 Credential=([^/]+)/([^/]+)/([^/]+)/s3/aws4_request, SignedHeaders=([^,]+), Signature=(.+)$`)

func attachBucketRoutes(logger *logrus.Entry, router *mux.Router, handler *bucketHandler, multipartHandler *multipartHandler) {
	router.Methods("GET", "PUT").Queries("accelerate", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT").Queries("acl", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("analytics", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("cors", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("encryption", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("inventory", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("lifecycle", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT").Queries("logging", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("metrics", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT").Queries("notification", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT").Queries("object-lock", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("policy", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET").Queries("policyStatus", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("publicAccessBlock", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("PUT", "DELETE").Queries("replication", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT").Queries("requestPayment", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("tagging", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT").Queries("versioning", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET").Queries("versions", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("website", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("POST").HandlerFunc(NotImplementedEndpoint(logger))

	router.Methods("GET").Queries("uploads", "").HandlerFunc(multipartHandler.list)
	router.Methods("GET", "HEAD").Queries("location", "").HandlerFunc(handler.location)
	router.Methods("GET", "HEAD").HandlerFunc(handler.get)
	router.Methods("PUT").HandlerFunc(handler.put)
	router.Methods("DELETE").HandlerFunc(handler.del)
}

func attachObjectRoutes(logger *logrus.Entry, router *mux.Router, handler *objectHandler, multipartHandler *multipartHandler) {
	router.Methods("GET", "PUT").Queries("acl", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT").Queries("legal-hold", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT").Queries("retention", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET", "PUT", "DELETE").Queries("tagging", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("GET").Queries("torrent", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("POST").Queries("restore", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("POST").Queries("select", "").HandlerFunc(NotImplementedEndpoint(logger))
	router.Methods("PUT").Headers("x-amz-copy-source", "").HandlerFunc(NotImplementedEndpoint(logger))

	router.Methods("GET", "HEAD").Queries("uploadId", "").HandlerFunc(multipartHandler.listChunks)
	router.Methods("POST").Queries("uploads", "").HandlerFunc(multipartHandler.init)
	router.Methods("POST").Queries("uploadId", "").HandlerFunc(multipartHandler.complete)
	router.Methods("PUT").Queries("uploadId", "").HandlerFunc(multipartHandler.put)
	router.Methods("DELETE").Queries("uploadId", "").HandlerFunc(multipartHandler.del)
	router.Methods("GET", "HEAD").HandlerFunc(handler.get)
	router.Methods("PUT").HandlerFunc(handler.put)
	router.Methods("DELETE").HandlerFunc(handler.del)
}

// S2 is the root struct used in the s2 library
type S2 struct {
	Auth      AuthController
	Service   ServiceController
	Bucket    BucketController
	Object    ObjectController
	Multipart MultipartController
	logger    *logrus.Entry
}

// NewS2 creates a new S2 instance. One created, you set zero or more
// attributes to implement various S3 functionality, then create a router.
func NewS2(logger *logrus.Entry) *S2 {
	return &S2{
		Auth:      nil,
		Service:   unimplementedServiceController{},
		Bucket:    unimplementedBucketController{},
		Object:    unimplementedObjectController{},
		Multipart: unimplementedMultipartController{},
		logger:    logger,
	}
}

// requestIDMiddleware creates a middleware handler that adds a request ID to
// every request.
func (h *S2) requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		id, err := uuid.NewV4()
		if err != nil {
			baseErr := fmt.Errorf("could not generate request ID: %v", err)
			WriteError(h.logger, w, r, InternalError(r, baseErr))
			return
		}

		vars["requestID"] = id.String()
		next.ServeHTTP(w, r)
	})
}

func (h *S2) authV4(w http.ResponseWriter, r *http.Request) error {
	// parse auth-related headers
	match := authHeaderValidator.FindStringSubmatch(r.Header.Get("authorization"))

	if len(match) == 0 {
		return AuthorizationHeaderMalformedError(r)
	}

	accessKey := match[1]
	date := match[2]
	region := match[3]
	signedHeaderKeys := strings.Split(match[4], ";")
	sort.Strings(signedHeaderKeys)
	expectedSignature := match[5]
	h.logger.Debugf("accessKey: %s", accessKey)
	h.logger.Debugf("date: %s", date)
	h.logger.Debugf("region: %s", region)
	h.logger.Debugf("signedHeaderKeys: %v", signedHeaderKeys)
	h.logger.Debugf("expectedSignature: %s", expectedSignature)

	// get the expected secret key
	secretKey, err := h.Auth.SecretKey(r, accessKey, region)
	if err != nil {
		// Even though an error occurred, we'll continue to compute the
		// signature to prevent a timing attack.
		h.logger.Errorf("Failed to get secret key for region=%s, accessKey=%s: %v", region, accessKey, err)
		secretKey = ""
	}

	// step 1: construct the canonical request
	var signedHeaders strings.Builder
	for _, key := range signedHeaderKeys {
		signedHeaders.WriteString(key)
		signedHeaders.WriteString(":")
		if key == "host" {
			signedHeaders.WriteString(r.Host)
		} else {
			signedHeaders.WriteString(strings.TrimSpace(r.Header.Get(key)))
		}
		signedHeaders.WriteString("\n")
	}

	canonicalRequest := strings.Join([]string{
		r.Method,
		normURI(r.URL.Path),
		normQuery(r.URL.Query()),
		signedHeaders.String(),
		strings.Join(signedHeaderKeys, ";"),
		r.Header.Get("x-amz-content-sha256"),
	}, "\n")

	timestamp := r.Header.Get("x-amz-date")
	stringToSign := fmt.Sprintf(
		"AWS4-HMAC-SHA256\n%s\n%s/%s/s3/aws4_request\n%x",
		timestamp,
		date,
		region,
		sha256.Sum256([]byte(canonicalRequest)),
	)

	// step 2: construct the string to sign
	dateKey := hmacSHA256([]byte("AWS4"+secretKey), date)
	dateRegionKey := hmacSHA256(dateKey, region)
	dateRegionServiceKey := hmacSHA256(dateRegionKey, "s3")
	signingKey := hmacSHA256(dateRegionServiceKey, "aws4_request")
	h.logger.Debugf("dateKey: %x", dateKey)
	h.logger.Debugf("dateRegionKey: %x", dateRegionKey)
	h.logger.Debugf("dateRegionServiceKey: %x", dateRegionServiceKey)

	// step 3: construct & verify the signature
	signature := hmacSHA256(signingKey, stringToSign)

	h.logger.Debugf("canonicalRequest:\n%s", canonicalRequest)
	h.logger.Debugf("stringToSign:\n%s", stringToSign)
	h.logger.Debugf("signingKey: %x", signingKey)
	h.logger.Debugf("signature: %x", signature)

	if expectedSignature != fmt.Sprintf("%x", signature) {
		return SignatureDoesNotMatchError(r)
	}

	vars := mux.Vars(r)
	vars["authAccessKey"] = accessKey
	vars["authRegion"] = region
	return nil
}

func (h *S2) authMiddleware(next http.Handler) http.Handler {
	// Verifies auth using AWS' signature v4. See here for a guide:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
	// Much of the code is built off of smartystreets/go-aws-auth, which does
	// signing from the client-side:
	// https://github.com/smartystreets/go-aws-auth
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.logger.Debugf("headers: %v", r.Header)

		if strings.HasPrefix(r.Header.Get("authorization"), "AWS4-HMAC-SHA256 ") {
			if err := h.authV4(w, r); err != nil {
				WriteError(h.logger, w, r, err)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// Router creates a new mux router.
func (h *S2) Router() *mux.Router {
	serviceHandler := &serviceHandler{
		controller: h.Service,
		logger:     h.logger,
	}
	bucketHandler := &bucketHandler{
		controller: h.Bucket,
		logger:     h.logger,
	}
	objectHandler := &objectHandler{
		controller: h.Object,
		logger:     h.logger,
	}
	multipartHandler := &multipartHandler{
		controller: h.Multipart,
		logger:     h.logger,
	}

	router := mux.NewRouter()
	router.Use(h.requestIDMiddleware)

	if h.Auth != nil {
		router.Use(h.authMiddleware)
	}

	router.Path(`/`).Methods("GET", "HEAD").HandlerFunc(serviceHandler.get)

	// Bucket-related routes. Repo validation regex is the same that the aws
	// cli uses. There's two routers - one with a trailing a slash and one
	// without. Both route to the same handlers, i.e. a request to `/foo` is
	// the same as `/foo/`. This is used instead of mux's builtin "strict
	// slash" functionality, because that uses redirects which doesn't always
	// play nice with s3 clients.
	trailingSlashBucketRouter := router.Path(`/{bucket:[a-zA-Z0-9\-_\.]{1,255}}/`).Subrouter()
	attachBucketRoutes(h.logger, trailingSlashBucketRouter, bucketHandler, multipartHandler)
	bucketRouter := router.Path(`/{bucket:[a-zA-Z0-9\-_\.]{1,255}}`).Subrouter()
	attachBucketRoutes(h.logger, bucketRouter, bucketHandler, multipartHandler)

	// Object-related routes
	objectRouter := router.Path(`/{bucket:[a-zA-Z0-9\-_\.]{1,255}}/{key:.+}`).Subrouter()
	attachObjectRoutes(h.logger, objectRouter, objectHandler, multipartHandler)

	router.MethodNotAllowedHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.logger.Infof("method not allowed: %s %s", r.Method, r.URL.Path)
		WriteError(h.logger, w, r, MethodNotAllowedError(r))
	})

	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.logger.Infof("not found: %s", r.URL.Path)
		if bucketNameValidator.MatchString(r.URL.Path) {
			WriteError(h.logger, w, r, NoSuchKeyError(r))
		} else {
			WriteError(h.logger, w, r, InvalidBucketNameError(r))
		}
	})

	return router
}
