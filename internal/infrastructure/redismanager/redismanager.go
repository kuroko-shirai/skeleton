package redismanager

import (
	"context"
	"skeleton/internal/configuration"

	"github.com/redis/go-redis/v9"
)

type REDISManager struct {
	storage *redis.Client
}

func New(
	ctx context.Context,
	cfg *configuration.Configuration,
) (*REDISManager, error) {
	return &REDISManager{
		storage: redis.NewClient(
			&redis.Options{
				Addr:     cfg.REDIS.Hosts[0],
				Password: cfg.REDIS.Password,
				DB:       0,
			},
		),
	}, nil
}

func (it *REDISManager) Up(ctx context.Context) error {
	if err := it.ping(ctx); err != nil {
		return err
	}

	return nil
}

func (it *REDISManager) Down(ctx context.Context) error {
	return it.storage.Close()
}

func (it *REDISManager) GetDB() *redis.Client {
	return it.storage
}

func (it *REDISManager) ping(ctx context.Context) error {
	if err := it.storage.Ping(
		ctx,
	).Err(); err != nil {
		return err
	}

	return nil
}
