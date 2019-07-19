package controllers

import (
	"net/http"
)

func (c Controller) SecretKey(r *http.Request, accessKey, region string) (secretKey string, err error) {
	c.logger.Tracef("SecretKey: accessKey=%+v, region=%+v", accessKey, region)

	if accessKey == "homer" {
		return "donuts", nil
	} else {
		return "", nil
	}
}
