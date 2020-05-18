package s2

import (
	"bufio"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

var (
	// chunkValidator is a regexp for validating a chunk "header" in the
	// request body of a multi-chunk upload
	chunkValidator = regexp.MustCompile(`^([0-9a-fA-F]+);chunk-signature=([0-9a-fA-F]+)`)

	// InvalidChunk is an error returned when reading a multi-chunk object
	// upload that contains an invalid chunk header or body
	InvalidChunk = errors.New("invalid chunk")
)

// Reads a multi-chunk upload body
type chunkedReader struct {
	body      io.ReadCloser
	lastChunk []byte
	bufBody   *bufio.Reader

	signingKey    []byte
	lastSignature string
	timestamp     string
	date          string
	region        string
}

func newChunkedReader(body io.ReadCloser, signingKey []byte, seedSignature, timestamp, date, region string) *chunkedReader {
	return &chunkedReader{
		body:      body,
		lastChunk: nil,
		bufBody:   bufio.NewReader(body),

		signingKey:    signingKey,
		lastSignature: seedSignature,
		timestamp:     timestamp,
		date:          date,
		region:        region,
	}
}

func (c *chunkedReader) Read(p []byte) (n int, err error) {
	if c.lastChunk == nil {
		if err := c.readChunk(); err != nil {
			return 0, err
		}
	}

	n = copy(p, c.lastChunk)

	if n == len(c.lastChunk) {
		c.lastChunk = nil
	} else {
		c.lastChunk = c.lastChunk[n:]
	}

	return n, nil
}

func (c *chunkedReader) readChunk() error {
	// step 1: read the chunk header
	line, err := c.bufBody.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return err
		}
		return InvalidChunk
	}

	match := chunkValidator.FindStringSubmatch(line)
	if len(match) == 0 {
		return InvalidChunk
	}

	chunkLengthHexStr := match[1]
	chunkSignature := match[2]

	chunkLength, err := strconv.ParseUint(chunkLengthHexStr, 16, 32)
	if err != nil {
		return InvalidChunk
	}

	// step 2: read the chunk body
	chunk := make([]byte, chunkLength)
	_, err = io.ReadFull(c.bufBody, chunk)
	if err != nil {
		return InvalidChunk
	}

	// step 3: read the trailer
	trailer := make([]byte, 2)
	_, err = io.ReadFull(c.bufBody, trailer)
	if err != nil || trailer[0] != '\r' || trailer[1] != '\n' {
		return InvalidChunk
	}

	// step 4: construct the string to sign
	stringToSign := fmt.Sprintf(
		"AWS4-HMAC-SHA256-PAYLOAD\n%s\n%s/%s/s3/aws4_request\n%s\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n%x",
		c.timestamp,
		c.date,
		c.region,
		c.lastSignature,      // TODO: not the same
		sha256.Sum256(chunk), // TODO: not the same
	)

	// step 5: calculate & verify the signature
	signature := hmacSHA256(c.signingKey, stringToSign)
	if chunkSignature != fmt.Sprintf("%x", signature) {
		return InvalidChunk
	}

	c.lastChunk = chunk
	c.lastSignature = chunkSignature
	return nil
}

func (c *chunkedReader) Close() error {
	return c.body.Close()
}

// GetObjectResult is a response from a GetObject call
type GetObjectResult struct {
	// ETag is a hex encoding of the hash of the object contents, with or
	// without surrounding quotes.
	ETag string
	// Version is the version of the object, or an empty string if versioning
	// is not enabled or supported.
	Version string
	// DeleteMarker specifies whether there's a delete marker in place of the
	// object.
	DeleteMarker bool
	// ModTime specifies when the object was modified.
	ModTime time.Time
	// Content is the contents of the object.
	Content io.ReadSeeker
}

// PutObjectResult is a response from a PutObject call
type PutObjectResult struct {
	// ETag is a hex encoding of the hash of the object contents, with or
	// without surrounding quotes.
	ETag string
	// Version is the version of the object, or an empty string if versioning
	// is not enabled or supported.
	Version string
}

// DeleteObjectResult is a response from a DeleteObject call
type DeleteObjectResult struct {
	// Version is the version of the object, or an empty string if versioning
	// is not enabled or supported.
	Version string
	// DeleteMarker specifies whether there's a delete marker in place of the
	// object.
	DeleteMarker bool
}

// ObjectController is an interface that specifies object-level functionality.
type ObjectController interface {
	// GetObject gets an object
	GetObject(r *http.Request, bucket, key, version string) (*GetObjectResult, error)
	// PutObject sets an object
	PutObject(r *http.Request, bucket, key string, reader io.Reader) (*PutObjectResult, error)
	// DeleteObject deletes an object
	DeleteObject(r *http.Request, bucket, key, version string) (*DeleteObjectResult, error)
}

// unimplementedObjectController defines a controller that returns
// `NotImplementedError` for all functionality
type unimplementedObjectController struct{}

func (c unimplementedObjectController) GetObject(r *http.Request, bucket, key, version string) (*GetObjectResult, error) {
	return nil, NotImplementedError(r)
}

func (c unimplementedObjectController) PutObject(r *http.Request, bucket, key string, reader io.Reader) (*PutObjectResult, error) {
	return nil, NotImplementedError(r)
}

func (c unimplementedObjectController) DeleteObject(r *http.Request, bucket, key, version string) (*DeleteObjectResult, error) {
	return nil, NotImplementedError(r)
}

type objectHandler struct {
	controller ObjectController
	logger     *logrus.Entry
}

func (h *objectHandler) get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	versionId := r.FormValue("versionId")

	result, err := h.controller.GetObject(r, bucket, key, versionId)
	if err != nil {
		WriteError(h.logger, w, r, err)
		return
	}

	if result.ETag != "" {
		w.Header().Set("ETag", addETagQuotes(result.ETag))
	}
	if result.Version != "" {
		w.Header().Set("x-amz-version-id", result.Version)
	}

	if result.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
		WriteError(h.logger, w, r, NoSuchKeyError(r))
		return
	}

	http.ServeContent(w, r, key, result.ModTime, result.Content)
}

func (h *objectHandler) put(w http.ResponseWriter, r *http.Request) {
	transferEncoding := r.Header["Transfer-Encoding"]
	identity := false
	for _, headerValue := range transferEncoding {
		if headerValue == "identity" {
			identity = true
		}
	}
	if len(transferEncoding) == 0 || identity {
		if err := requireContentLength(r); err != nil {
			WriteError(h.logger, w, r, err)
			return
		}
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	chunked := r.Header.Get("X-Amz-Content-Sha256") == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

	var body io.ReadCloser
	if chunked {
		signingKey := []byte(vars["authSignatureKey"])
		seedSignature := vars["authSignature"]
		timestamp := vars["authSignatureTimestamp"]
		date := vars["authSignatureDate"]
		region := vars["authSignatureRegion"]
		body = newChunkedReader(r.Body, signingKey, seedSignature, timestamp, date, region)
	} else {
		body = r.Body
	}

	result, err := h.controller.PutObject(r, bucket, key, body)
	if err != nil {
		if err == InvalidChunk {
			WriteError(h.logger, w, r, SignatureDoesNotMatchError(r))
		} else {
			WriteError(h.logger, w, r, err)
		}
		return
	}

	if result.ETag != "" {
		w.Header().Set("ETag", addETagQuotes(result.ETag))
	}
	if result.Version != "" {
		w.Header().Set("x-amz-version-id", result.Version)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *objectHandler) del(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	versionId := r.FormValue("versionId")

	result, err := h.controller.DeleteObject(r, bucket, key, versionId)
	if err != nil {
		WriteError(h.logger, w, r, err)
		return
	}

	if result.Version != "" {
		w.Header().Set("x-amz-version-id", result.Version)
	}
	if result.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *objectHandler) post(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	payload := struct {
		XMLName xml.Name `xml:"Delete"`
		Quiet   bool     `xml:"Quiet"`
		Objects []struct {
			Key     string `xml:"Key"`
			Version string `xml:"VersionId"`
		} `xml:"Object"`
	}{}
	if err := readXMLBody(r, &payload); err != nil {
		WriteError(h.logger, w, r, err)
		return
	}

	marshallable := struct {
		XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult"`
		Deleted []struct {
			Key                 string `xml:"Key"`
			Version             string `xml:"Version,omitempty"`
			DeleteMarker        bool   `xml:"Code,omitempty"`
			DeleteMarkerVersion string `xml:"DeleteMarkerVersionId,omitempty"`
		} `xml:"Deleted"`
		Errors []struct {
			Key     string `xml:"Key"`
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}{
		Deleted: []struct {
			Key                 string `xml:"Key"`
			Version             string `xml:"Version,omitempty"`
			DeleteMarker        bool   `xml:"Code,omitempty"`
			DeleteMarkerVersion string `xml:"DeleteMarkerVersionId,omitempty"`
		}{},
		Errors: []struct {
			Key     string `xml:"Key"`
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		}{},
	}

	for _, object := range payload.Objects {
		result, err := h.controller.DeleteObject(r, bucket, object.Key, object.Version)
		if err != nil {
			s3Err := newGenericError(r, err)

			marshallable.Errors = append(marshallable.Errors, struct {
				Key     string `xml:"Key"`
				Code    string `xml:"Code"`
				Message string `xml:"Message"`
			}{
				Key:     object.Key,
				Code:    s3Err.Code,
				Message: s3Err.Message,
			})
		} else {
			deleteMarkerVersion := ""
			if result.DeleteMarker {
				deleteMarkerVersion = result.Version
			}

			if !payload.Quiet {
				marshallable.Deleted = append(marshallable.Deleted, struct {
					Key                 string `xml:"Key"`
					Version             string `xml:"Version,omitempty"`
					DeleteMarker        bool   `xml:"Code,omitempty"`
					DeleteMarkerVersion string `xml:"DeleteMarkerVersionId,omitempty"`
				}{
					Key:                 object.Key,
					Version:             object.Version,
					DeleteMarker:        result.DeleteMarker,
					DeleteMarkerVersion: deleteMarkerVersion,
				})
			}
		}
	}

	writeXML(h.logger, w, r, http.StatusOK, marshallable)
}
