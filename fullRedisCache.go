package cachelayer

import (
	"context"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

type FullDBCache[T Table[I], I IDType] interface {
	Create(r *T) error
	Save(r *T) error
	Update(id I, values interface{}) (int64, error)
	Delete(ids ...I) (int64, error)
	Get(id I) (T, bool, error)
	List(ids ...I) ([]T, error)
	ListAll() ([]T, error)
	Close() error
}

type FullRedisCache[T Table[I], I IDType] struct {
	*CacheBase[T, I]
	db  FullDBCache[T, I]
	red *RedisHashJson[T, I]
	ctx context.Context
}

func NewFullRedisCache[T Table[I], I IDType](prefix, table, idField string, db FullDBCache[T, I], red *redis.Client, ttl time.Duration) *FullRedisCache[T, I] {
	return &FullRedisCache[T, I]{
		CacheBase: &CacheBase[T, I]{prefix: prefix, table: table, idField: idField, ctx: context.Background()},
		db:        db,
		red:       NewRedisHashJson[T, I](red, ttl),
		ctx:       context.Background(),
	}
}

func (s *FullRedisCache[T, I]) CacheKey() string {
	r := s.prefix + "/" + s.table + "/full"
	return strings.ToLower(r)
}

func (s *FullRedisCache[T, I]) load() error {
	r, err := s.db.ListAll()
	if err != nil {
		return err
	}

	key := s.CacheKey()
	err = s.red.HSetJson(key, r)
	if err != nil {
		return err
	}
	return s.red.Expire(s.ctx, key, s.red.ttl).Err()
}

func (s *FullRedisCache[T, I]) Get(id I) (T, bool, error) {
	key := s.CacheKey()
	r, exists, err := s.red.HGetJson(key, id)
	if err != nil {
		return r, false, err
	}
	if exists {
		return r, true, nil
	}
	if err := s.load(); err != nil {
		return r, false, err
	}
	return s.red.HGetJson(key, id)
}

func (s *FullRedisCache[T, I]) List(id ...I) ([]T, error) {
	key := s.CacheKey()
	count, err := s.red.Exists(s.ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		if err := s.load(); err != nil {
			return nil, err
		}
	}
	return s.red.HMGetJson(key, id...)
}

func (s *FullRedisCache[T, I]) Create(r *T) error {
	if err := s.db.Create(r); err != nil {
		return err
	}
	return s.load()
}
func (s *FullRedisCache[T, I]) Save(r *T) error {
	_, exists, err := s.Get((*r).GetID())
	if err != nil {
		return err
	}
	if IsNullID((*r).GetID()) || !exists {
		if err := s.db.Create(r); err != nil {
			return err
		}
	} else {
		if err := s.db.Save(r); err != nil {
			return err
		}
	}
	s.load()
	return nil
}
func (s *FullRedisCache[T, I]) Update(id I, values interface{}) (int64, error) {
	if IsNullID(id) {
		return 0, nil
	}

	effectedRows, err := s.db.Update(id, values)
	if err != nil {
		return 0, err
	}
	s.load()

	return effectedRows, err
}
func (s *FullRedisCache[T, I]) Delete(ids ...I) (int64, error) {
	rowsAffected, err := s.db.Delete(ids...)
	if err != nil {
		return 0, err
	}
	s.load()
	return rowsAffected, err
}

func (s *FullRedisCache[T, I]) ListAll() ([]T, error) {
	key := s.CacheKey()
	count, err := s.red.Exists(s.ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		if err := s.load(); err != nil {
			return nil, err
		}
	}
	return s.red.HGetAllJson(key)
}

func (s *FullRedisCache[T, I]) ClearCache(objs ...T) error {
	return s.red.Del(s.ctx, s.CacheKey()).Err()
}
