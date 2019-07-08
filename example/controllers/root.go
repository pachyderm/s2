package controllers

import (
	"net/http"

	"github.com/pachyderm/s2"
	"github.com/pachyderm/s2/example/models"
)

type RootController struct {
	DB models.Storage
}

func (c RootController) List(r *http.Request, result *s2.ListAllMyBucketsResult) *s2.Error {
	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()
	result.Owner = models.GlobalUser

	for bucket := range c.DB.Buckets {
		result.Buckets = append(result.Buckets, s2.Bucket{
			Name:         bucket,
			CreationDate: models.Epoch,
		})
	}

	return nil
}
