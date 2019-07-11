package s2

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

type GetObjectResult struct {
	Name    string        `xml:"Name"`
	ETag    string        `xml:"ETag"`
	ModTime time.Time     `xml:"ModTime"`
	Content io.ReadSeeker `xml:"Content"`
}

type ObjectController interface {
	GetObject(r *http.Request, bucket, key string, result *GetObjectResult) error
	PutObject(r *http.Request, bucket, key string, reader io.Reader) error
	DeleteObject(r *http.Request, bucket, key string) error
}

type UnimplementedObjectController struct{}

func (c UnimplementedObjectController) GetObject(r *http.Request, bucket, key string, result *GetObjectResult) error {
	return NotImplementedError(r)
}

func (c UnimplementedObjectController) PutObject(r *http.Request, bucket, key string, reader io.Reader) error {
	return NotImplementedError(r)
}

func (c UnimplementedObjectController) DeleteObject(r *http.Request, bucket, key string) error {
	return NotImplementedError(r)
}

type objectHandler struct {
	controller ObjectController
	logger     *logrus.Entry
}

func (h *objectHandler) get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	result := &GetObjectResult{}

	if err := h.controller.GetObject(r, bucket, key, result); err != nil {
		writeError(h.logger, w, r, err)
		return
	}

	if result.ETag != "" {
		if !strings.HasPrefix(result.ETag, "\"") {
			result.ETag = fmt.Sprintf("\"%s\"", result.ETag)
		}

		w.Header().Set("ETag", result.ETag)
	}

	http.ServeContent(w, r, result.Name, result.ModTime, result.Content)
}

func (h *objectHandler) put(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	hashBytes, shouldCleanup, err := withBodyReader(r, func(reader io.Reader) error {
		return h.controller.PutObject(r, bucket, key, reader)
	})

	if shouldCleanup {
		// try to clean up the file
		if err := h.controller.DeleteObject(r, bucket, key); err != nil {
			h.logger.Errorf("could not clean up file after an error: %+v", err)
		}
	}

	if err != nil {
		writeError(h.logger, w, r, err)
	} else {
		w.Header().Set("ETag", fmt.Sprintf("\"%x\"", hashBytes))
		w.WriteHeader(http.StatusOK)
	}
}

func (h *objectHandler) del(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	if err := h.controller.DeleteObject(r, bucket, key); err != nil {
		writeError(h.logger, w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
