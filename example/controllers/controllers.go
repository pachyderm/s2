package controllers

import (
	"github.com/sirupsen/logrus"
)

type Controller struct {
	logger *logrus.Entry
}

func NewController(logger *logrus.Entry) Controller {
	return Controller{
		logger: logger,
	}
}
