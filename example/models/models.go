package models

import (
    "crypto/md5"
    "fmt"
    "time"

    "github.com/jinzhu/gorm"
    "github.com/pachyderm/s2"
)

var (
    GlobalUser = s2.User{
        ID:          "s2-demo",
        DisplayName: "s2 demo",
    }

    Epoch = time.Unix(0, 0)

    StorageClass = "STANDARD"

    Location = "pachydermia"

    AccessKey = "homer"
    SecretKey = "donuts"
)

func Init(db *gorm.DB) error {
    if err := db.AutoMigrate(&Bucket{}, &Object{}, &Multipart{}).Error; err != nil {
        return err
    }
    if err := db.Model(&Object{}).AddForeignKey("bucket_id", "buckets(id)", "CASCADE", "CASCADE").Error; err != nil {
        return err
    }
    return db.Model(&Multipart{}).AddForeignKey("bucket_id", "buckets(id)", "CASCADE", "CASCADE").Error
}

type Bucket struct {
    ID   string `gorm:"primary_key"`
    Name string `gorm:"not null,unique_index"`
}

func CreateBucket(db *gorm.DB, name string) (*Bucket, error) {
    bucket := &Bucket{Name: name}
    err := db.Save(bucket).Error
    return bucket, err
}

func GetBucket(db *gorm.DB, name string) (*Bucket, error) {
    var bucket *Bucket
    err := db.Where("name = ?", name).First(bucket).Error
    return bucket, err
}

type Object struct {
    ID uint `gorm:"primary_key"`

    BucketID uint   `gorm:"not null"`
    ETag     string `gorm:"not null"`
    Key      string `gorm:"not null,index:idx_object_key"`
    Content  []byte `gorm:"not null"`
}

func GetObject(db *gorm.DB, bucketID uint, key string) (*Object, error) {
    var object *Object
    err := db.Where("bucket_id = ? AND key = ?", bucketID, key).First(object).Error
    return object, err
}

func ListObjects(db *gorm.DB, bucketID uint, marker string, limit int) ([]*Object, error) {
    var objects []*Object
    err := db.Limit(limit).Order("bucket_id, key").Where("bucket_id = ? AND key > ?", bucketID, marker).Find(&objects).Error
    return objects, err
}

func UpsertObject(db *gorm.DB, bucketID uint, key string, content []byte) (*Object, error) {
    var object *Object
    err := db.FirstOrCreate(object, Object{
        BucketID: bucketID,
        Key:      key,
    }).Error
    if err != nil {
        return nil, err
    }
    object.ETag = fmt.Sprintf("%x", md5.Sum(content))
    object.Content = content
    err = db.Save(object).Error
    return object, err
}

func DeleteObject(db *gorm.DB, bucketID uint, key string) error {
    return db.Delete(&Object{
        BucketID: bucketID,
        Key:      key,
    }).Error
}

type Multipart struct {
    ID string `gorm:"primary_key"`

    BucketID   uint   `gorm:"not null"`
    Key        string `gorm:"not null,index:idx_multipart_key"`
    UploadID   string `gorm:"not null,index:idx_multipart_upload_id"`
    PartNumber int    `gorm:"not null"`
    ETag       string `gorm:"not null"`
    Content    []byte `gorm:"not null"`
}

func UpsertMultipart(db *gorm.DB, bucketID uint, key string, uploadID string, partNumber int, content []byte) (*Multipart, error) {
    var multipart *Multipart
    err := db.FirstOrCreate(multipart, Multipart{
        BucketID:   bucketID,
        Key:        key,
        UploadID:   uploadID,
        PartNumber: partNumber,
    }).Error
    if err != nil {
        return nil, err
    }
    multipart.ETag = fmt.Sprintf("%x", md5.Sum(content))
    multipart.Content = content
    err = db.Save(multipart).Error
    return multipart, err
}

func ListMultiparts(db *gorm.DB, bucketID uint, keyMarker string, uploadIDMarker string, limit int) ([]*Multipart, error) {
    var parts []*Multipart
    err := db.Limit(limit).Order("bucket_id, key, upload_id, part_number").Where("bucket_id = ? AND key >= ? AND upload_id > ?", bucketID, keyMarker, uploadIDMarker).Find(&parts).Error
    return parts, err
}

func ListMultipartChunks(db *gorm.DB, bucketID uint, key string, uploadID string, partNumberMarker, limit int) ([]*Multipart, error) {
    var parts []*Multipart
    err := db.Limit(limit).Order("bucket_id, key, upload_id, part_number").Where("bucket_id = ? AND key = ? AND upload_id = ? AND part_number > ?", bucketID, key, uploadID, partNumberMarker).Find(&parts).Error
    return parts, err
}

func DeleteMultiparts(db *gorm.DB, bucketID uint, key, uploadID string) error {
    return db.Delete(&Multipart{
        BucketID: bucketID,
        Key:      key,
        UploadID: uploadID,
    }).Error
}
