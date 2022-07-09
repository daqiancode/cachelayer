package gormredis

import (
	"time"

	"github.com/daqiancode/cachelayer"
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

func NewGormRedis[T cachelayer.Table[I], I cachelayer.IDType](prefix, table, idField string, db *gorm.DB, red *redis.Client, ttl time.Duration) *cachelayer.RedisCache[T, I] {
	rc := cachelayer.NewRedisCache[T, I](prefix, table, idField, &Gorm[T, I]{db: db}, red, ttl)
	return rc
}
func NewGormRedisFull[T cachelayer.Table[I], I cachelayer.IDType](prefix, table, idField string, db *gorm.DB, red *redis.Client, ttl time.Duration) cachelayer.FullCache[T, I] {
	rc := cachelayer.NewFullRedisCache[T, I](prefix, table, idField, &Gorm[T, I]{db: db}, red, ttl)
	return rc
}

type Gorm[T cachelayer.Table[I], I cachelayer.IDType] struct {
	db *gorm.DB
}

func (s *Gorm[T, I]) Close() error {
	return nil
}
func (s *Gorm[T, I]) DB() *gorm.DB {
	return s.DB()
}
func (s *Gorm[T, I]) Create(r *T) error {
	if err := s.db.Create(r).Error; err != nil {
		return err
	}
	return nil
}
func (s *Gorm[T, I]) Save(r *T) error {
	_, exists, err := s.Get((*r).GetID())
	if err != nil {
		return err
	}
	if !exists {
		return s.Create(r)
	}
	return s.db.Save(r).Error
}
func (s *Gorm[T, I]) Update(id I, values interface{}) (int64, error) {
	old, exists, err := s.Get(id)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	rs := s.db.Model(&old).Updates(values)
	if rs.Error != nil {
		return 0, rs.Error
	}
	return rs.RowsAffected, nil
}
func (s *Gorm[T, I]) Delete(ids ...I) (int64, error) {
	rs := s.db.Delete(new(T), ids)
	if rs.Error != nil {
		return 0, rs.Error
	}
	return rs.RowsAffected, nil
}
func (s *Gorm[T, I]) Get(id I) (T, bool, error) {
	var r T
	if err := s.db.First(&r, id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return r, false, nil
		}
		return r, false, err
	}
	return r, true, nil
}
func (s *Gorm[T, I]) GetBy(index cachelayer.Index) (T, bool, error) {
	var r T
	if err := s.db.Where(map[string]interface{}(index)).First(&r).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return r, false, nil
		}
		return r, false, err
	}
	return r, true, nil
}
func (s *Gorm[T, I]) List(ids ...I) ([]T, error) {
	var r []T
	err := s.db.Find(&r, ids).Error
	return r, err
}
func (s *Gorm[T, I]) ListBy(index cachelayer.Index, orderBys cachelayer.OrderBys) ([]T, error) {
	var r []T
	if err := s.db.Where(map[string]interface{}(index)).Order(orderBys.String()).Find(&r).Error; err != nil {
		return nil, err
	}
	return r, nil
}

func (s *Gorm[T, I]) ListAll() ([]T, error) {
	var r []T
	if err := s.db.Find(&r).Error; err != nil {
		return nil, err
	}
	return r, nil
}
