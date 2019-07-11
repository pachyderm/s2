package controllers

import (
	"github.com/pachyderm/s2/example/models"
	"github.com/sirupsen/logrus"
)

type Controller struct {
	DB     models.Storage
	logger *logrus.Entry
}

func NewController(db models.Storage, logger *logrus.Entry) Controller {
	return Controller{
		DB:     db,
		logger: logger,
	}
}
