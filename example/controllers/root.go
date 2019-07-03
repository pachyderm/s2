package controllers

import (
	"net/http"

	"github.com/pachyderm/s3server"
	"github.com/pachyderm/s3server/example/models"
)

type RootController struct {
	DB models.Storage
}

func (c RootController) List(r *http.Request, result *s3server.ListAllMyBucketsResult) *s3server.Error {
	c.DB.Lock.RLock()
	defer c.DB.Lock.RUnlock()
	result.Owner = models.GlobalUser

	for bucket := range c.DB.Buckets {
		result.Buckets = append(result.Buckets, s3server.Bucket{
			Name:         bucket,
			CreationDate: models.Epoch,
		})
	}

	return nil
}
