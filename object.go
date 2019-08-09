package s2

import (
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// GetObjectResult is a response from a GetObject call
type GetObjectResult struct {
	ETag         string
	Version      string
	DeleteMarker bool
	ModTime      time.Time
	Content      io.ReadSeeker
}

type PutObjectResult struct {
	ETag    string
	Version string
}

type DeleteObjectResult struct {
	Version      string
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
	if versionId == "null" {
		versionId = ""
	}

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
	if err := requireContentLength(r); err != nil {
		WriteError(h.logger, w, r, err)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	result, err := h.controller.PutObject(r, bucket, key, r.Body)
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
	w.WriteHeader(http.StatusOK)
}

func (h *objectHandler) del(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	versionId := r.FormValue("versionId")
	if versionId == "null" {
		versionId = ""
	}

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
