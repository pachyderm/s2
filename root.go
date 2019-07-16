package s2

import (
	"encoding/xml"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

type Bucket struct {
	Name         string    `xml:"Name"`
	CreationDate time.Time `xml:"CreationDate"`
}

type RootController interface {
	ListBuckets(r *http.Request) (owner *User, buckets []Bucket, err error)
}

type UnimplementedRootController struct{}

func (c UnimplementedRootController) ListBuckets(r *http.Request) (owner *User, buckets []Bucket, err error) {
	return nil, nil, NotImplementedError(r)
}

type rootHandler struct {
	controller RootController
	logger     *logrus.Entry
}

func (h *rootHandler) get(w http.ResponseWriter, r *http.Request) {
	owner, buckets, err := h.controller.ListBuckets(r)
	if err != nil {
		WriteError(h.logger, w, r, err)
		return
	}

	writeXML(h.logger, w, r, http.StatusOK, struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		Owner   *User    `xml:"Owner"`
		Buckets []Bucket `xml:"Buckets>Bucket"`
	}{
		Owner:   owner,
		Buckets: buckets,
	})
}
