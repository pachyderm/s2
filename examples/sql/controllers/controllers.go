package controllers

import (
	"sync"

	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
)

type Controller struct {
	logger *logrus.Entry
	db     *gorm.DB
	lock   *sync.Mutex
}

func NewController(logger *logrus.Entry, db *gorm.DB) *Controller {
	return &Controller{
		logger: logger,
		db:     db,
		lock:   &sync.Mutex{},
	}
}

func (c *Controller) trans() *gorm.DB {
	c.lock.Lock()
	return c.db.New().Begin()
}

func (c *Controller) rollback(tx *gorm.DB) {
	if err := tx.Rollback().Error; err != nil {
		c.logger.WithError(err).Error("could not rollback transaction")
	}

	c.lock.Unlock()
}

func (c *Controller) commit(tx *gorm.DB) {
	if err := tx.Commit().Error; err != nil {
		c.logger.WithError(err).Error("could not commit transaction")
	}

	c.lock.Unlock()
}
