package cachelayer

import (
	"context"
	"fmt"
	"time"

	"github.com/daqiancode/jsoniter"
	"github.com/go-redis/redis/v8"
)

type Serializer interface {
	Marshal(obj interface{}) (string, error)
	Unmarshal(data string, objRef interface{}) error
}

var json = jsoniter.Config{EscapeHTML: false, Decapitalize: true, ObjectFieldMustBeSimpleString: true}.Froze()

type JsonSerializer struct {
}

func (s *JsonSerializer) Marshal(obj interface{}) (string, error) {
	return json.MarshalToString(obj)
}
func (s *JsonSerializer) Unmarshal(data string, objRef interface{}) error {
	return json.UnmarshalFromString(data, objRef)
}

type RedisJson[T any] struct {
	*redis.Client
	serializer Serializer
	ctx        context.Context
	ttl        time.Duration
}

func NewRedisJson[T any](client *redis.Client, ttl time.Duration) *RedisJson[T] {
	return &RedisJson[T]{
		Client:     client,
		serializer: &JsonSerializer{},
		ctx:        context.Background(),
		ttl:        ttl,
	}
}

func (s *RedisJson[T]) GetJson(key string) (T, bool, error) {
	var r T
	y, err := s.Get(s.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return r, false, nil
		}
		return r, false, err
	}
	err = s.serializer.Unmarshal(y, &r)
	return r, true, err
}

func (s *RedisJson[T]) SetJson(key string, obj T) error {
	y, err := s.serializer.Marshal(obj)
	if err != nil {
		return err
	}
	return s.SetEX(s.ctx, key, y, s.ttl).Err()
}

func (s *RedisJson[T]) MSetJson(objMap map[string]interface{}) error {
	if len(objMap) == 0 {
		return nil
	}
	objJsonMap := make(map[string]string, len(objMap))
	var err error
	keys := make([]string, len(objMap))
	i := 0
	for k, v := range objMap {
		objJsonMap[k], err = s.serializer.Marshal(v)
		if err != nil {
			return err
		}
		keys[i] = k
		i++
	}
	err = s.MSet(s.ctx, objJsonMap).Err()
	if err != nil {
		return err
	}
	return s.Expires(keys...)
}

func (s *RedisJson[T]) Expires(keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	p := s.Pipeline()
	var err error
	for _, v := range keys {
		err = p.Expire(s.ctx, v, s.ttl).Err()
		if err != nil {
			return err
		}
	}
	_, err = p.Exec(s.ctx)
	return err

}

func (s *RedisJson[T]) SetNull(key string) error {
	return s.SetEX(s.ctx, key, "null", s.ttl).Err()
}

func (s *RedisJson[T]) MSetNull(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	p := s.Pipeline()
	var err error
	for _, v := range keys {
		err = p.SetEX(s.ctx, v, "null", s.ttl).Err()
		if err != nil {
			return err
		}
	}
	_, err = p.Exec(s.ctx)
	return err
}

func (s *RedisJson[T]) MGetJson(keys []string) ([]T, []int, error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}
	vs, err := s.MGet(s.ctx, keys...).Result()
	fmt.Printf("MGetJson: %#v\n", vs)
	if err != nil {
		return nil, nil, err
	}
	var missedIndexes []int
	r := make([]T, len(keys))
	for i, v := range vs {
		var t T
		if v == nil {
			missedIndexes = append(missedIndexes, i)
			r[i] = t
			continue
		}

		err = s.serializer.Unmarshal(v.(string), &t)
		if err != nil {
			return nil, missedIndexes, err
		}
		r[i] = t
	}
	for _, key := range keys {
		err = s.Expire(s.ctx, key, s.ttl).Err()
		if err != nil {
			return r, missedIndexes, err
		}
	}
	return r, missedIndexes, nil

}

type RedisHashJson[T Table[I], I IDType] struct {
	*redis.Client
	serializer Serializer
	ctx        context.Context
	ttl        time.Duration
}

func NewRedisHashJson[T Table[I], I IDType](client *redis.Client, ttl time.Duration) *RedisHashJson[T, I] {
	return &RedisHashJson[T, I]{
		Client:     client,
		serializer: &JsonSerializer{},
		ctx:        context.Background(),
		ttl:        ttl,
	}
}

func (s *RedisHashJson[T, I]) HGetJson(key string, id I) (T, bool, error) {
	idStr := Stringify(id, "")
	var r T
	raw, err := s.HGet(s.ctx, key, idStr).Result()
	if err != nil {
		if err == redis.Nil {
			return r, false, nil
		}
		return r, false, err
	}
	err = s.serializer.Unmarshal(raw, &r)
	return r, true, err
}

func (s *RedisHashJson[T, I]) HGetAllJson(key string) ([]T, error) {
	var r []T
	raw, err := s.HGetAll(s.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return r, nil
		}
		return r, err
	}
	for _, v := range raw {
		var t T
		err = s.serializer.Unmarshal(v, &t)
		if err != nil {
			return r, nil
		}
		r = append(r, t)
	}
	return r, nil
}

func (s *RedisHashJson[T, I]) HMGetJson(key string, ids ...I) ([]T, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	idStrs := make([]string, len(ids))
	for i, v := range ids {
		idStrs[i] = Stringify(v, "")
	}
	var r []T
	raw, err := s.HMGet(s.ctx, key, idStrs...).Result()
	if err != nil {
		if err == redis.Nil {
			return r, nil
		}
		return r, err
	}
	for _, v := range raw {
		var t T
		err = s.serializer.Unmarshal(v.(string), &t)
		if err != nil {
			return r, nil
		}
		r = append(r, t)
	}
	return r, nil
}

func (s *RedisHashJson[T, I]) HSetJson(key string, objs ...T) error {
	if len(objs) == 0 {
		return nil
	}
	var err error
	args := make([]string, len(objs)*2)
	for k, v := range objs {
		args[2*k] = Stringify(v.GetID(), "")
		args[2*k+1], err = s.serializer.Marshal(v)
		if err != nil {
			return err
		}
	}
	return s.HSet(s.ctx, key, args).Err()
}

func (s *RedisHashJson[T, I]) HDelJson(key string, ids ...I) error {
	if len(ids) == 0 {
		return nil
	}
	idStrs := make([]string, len(ids))
	for i, v := range ids {
		idStrs[i] = Stringify(v, "")
	}
	return s.HDel(s.ctx, key, idStrs...).Err()
}
