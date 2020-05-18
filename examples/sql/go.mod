module github.com/pachyderm/s2/examples/sql

go 1.12

require (
	github.com/gorilla/mux v1.7.3
	github.com/jinzhu/gorm v1.9.10
	github.com/pachyderm/s2 v0.0.0-20190725181334-d1f4a476d240
	github.com/sirupsen/logrus v1.5.0
)

replace github.com/pachyderm/s2 => ../../
