package controllers

import (
	"net/http"

	"github.com/pachyderm/s2/example/models"
)

func (c Controller) SecretKey(r *http.Request, accessKey, region string) (secretKey *string, err error) {
	c.logger.Tracef("SecretKey: accessKey=%+v, region=%+v", accessKey, region)

	if accessKey == models.AccessKey {
		return &models.SecretKey, nil
	}
	return nil, nil
}
