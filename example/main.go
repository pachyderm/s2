package main

import (
	"net/http"

	"github.com/pachyderm/s3server"
	"github.com/pachyderm/s3server/example/controllers"
	"github.com/pachyderm/s3server/example/models"
	"github.com/sirupsen/logrus"
)

func main() {
	db := models.NewStorage()

	logger := logrus.WithFields(logrus.Fields{
		"source": "s3server-example",
	})

	s3 := s3server.NewS3()
	s3.Root = controllers.RootController{DB: db}
	s3.Bucket = controllers.BucketController{DB: db}
	s3.Object = controllers.ObjectController{DB: db}

	http.ListenAndServe(":8080", s3.Router(logger))
}
