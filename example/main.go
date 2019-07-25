package main

import (
	stdlog "log"
	"net/http"
	"time"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/controllers"
	"github.com/pachyderm/s2/example/models"
	"github.com/sirupsen/logrus"
)

func main() {
	db := models.NewStorage()

	logrus.SetLevel(logrus.TraceLevel)
	logger := logrus.WithFields(logrus.Fields{
		"source": "s2-example",
	})

	controller := controllers.NewController(db, logger)

	s3 := s2.NewS2(logger, 0)
	s3.Auth = controller
	s3.Service = controller
	s3.Bucket = controller
	s3.Object = controller
	s3.Multipart = controller

	router := s3.Router()

	server := &http.Server{
		Addr: ":8080",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.Infof("%s %s", r.Method, r.RequestURI)
			logger.Infof("headers: %+v", r.Header)
			router.ServeHTTP(w, r)
		}),
		ErrorLog:     stdlog.New(logger.Writer(), "", 0),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	server.ListenAndServe()
}
