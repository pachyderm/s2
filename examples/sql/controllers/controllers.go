package controllers

import (
	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
)

type Controller struct {
	logger *logrus.Entry
	db     *gorm.DB
}

func NewController(logger *logrus.Entry, db *gorm.DB) Controller {
	return Controller{
		logger: logger,
		db:     db,
	}
}

func (c Controller) commit(tx *gorm.DB) {
	if err := tx.Commit().Error; err != nil {
		c.logger.WithError(err).Error("could not commit request transaction")
	}
}
