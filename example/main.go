package main

import (
	stdlog "log"
	"net/http"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/controllers"
	"github.com/pachyderm/s2/example/models"
	"github.com/sirupsen/logrus"
)

func main() {
	db, err := gorm.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	models.Init(db)

	logrus.SetLevel(logrus.TraceLevel)
	logger := logrus.WithFields(logrus.Fields{
		"source": "s2-example",
	})

	s3 := s2.NewS2(logger, 0, 5*time.Second)
	controller := controllers.NewController(logger)
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

			vars := mux.Vars(r)
			tx := db.Begin()
			vars["tx"] = tx
			router.ServeHTTP(w, r)

			if w.StatusCode < 200 || w.StatusCode > 399 {
				tx.Rollback()
			} else {
				if err := tx.Commit().Error; err != nil {
					logger.WithError(err).Error("could not commit request transaction")
				}
			}
		}),
		ErrorLog:     stdlog.New(logger.Writer(), "", 0),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	server.ListenAndServe()
}
