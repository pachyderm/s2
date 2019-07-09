package s2

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/sirupsen/logrus"
)

// writeError serializes an error to a response as XML
func writeError(logger *logrus.Entry, r *http.Request, w http.ResponseWriter, err error) {
	switch e := err.(type) {
	case *Error:
		e.Write(logger, w)
	default:
		InternalError(r, e).Write(logger, w)
	}
}

func writeXMLPrelude(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(code)
	fmt.Fprint(w, xml.Header)
}

func writeXMLBody(logger *logrus.Entry, w http.ResponseWriter, v interface{}) {
	encoder := xml.NewEncoder(w)
	if err := encoder.Encode(v); err != nil {
		// just log a message since a response has already been partially
		// written
		logger.Errorf("could not encode xml response: %v", err)
	}
}

func writeXML(logger *logrus.Entry, w http.ResponseWriter, r *http.Request, code int, v interface{}) {
	writeXMLPrelude(w, code)
	writeXMLBody(logger, w, v)
}

func NotImplementedEndpoint(logger *logrus.Entry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		NotImplementedError(r).Write(logger, w)
	}
}

func withBodyReader(r *http.Request, f func(reader io.Reader) error) ([]byte, bool, error) {
	expectedHash, ok := r.Header["Content-Md5"]
	var expectedHashBytes []uint8
	var err error
	if ok && len(expectedHash) == 1 {
		expectedHashBytes, err = base64.StdEncoding.DecodeString(expectedHash[0])
		if err != nil || len(expectedHashBytes) != 16 {
			return nil, false, InvalidDigestError(r)
		}
	}

	hasher := md5.New()
	reader := io.TeeReader(r.Body, hasher)
	if err = f(reader); err != nil {
		return nil, false, err
	}

	actualHashBytes := hasher.Sum(nil)
	if expectedHashBytes != nil && !bytes.Equal(expectedHashBytes, actualHashBytes) {
		return nil, true, BadDigestError(r)
	}

	return actualHashBytes, false, nil
}

// intFormValue extracts an int value from a request's form values, ensuring
// it's within specified bounds. If the value is unspecified, `def` is
// returned. If the value is not an int, or not with the specified bounds, an
// error is returned.
func intFormValue(r *http.Request, name string, min int, max int, def int) (int, error) {
	s := r.FormValue(name)
	if s == "" {
		return def, nil
	}

	i, err := strconv.Atoi(s)
	if err != nil || i < min || i > max {
		return 0, InvalidArgument(r)
	}

	return i, nil
}
