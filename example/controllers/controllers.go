package controllers

import (
	"github.com/pachyderm/s2/example/models"
)

type Controller struct {
	DB models.Storage
}

func NewController(db models.Storage) Controller {
	return Controller{DB: db}
}
