package factories

import (
	"context"
	"skeleton/internal/configuration"
	"skeleton/internal/infrastructure/redismanager"
)

type REDISManagerFactory struct{}

func (it *REDISManagerFactory) New(
	ctx context.Context,
	cfg *configuration.Configuration,
) (*redismanager.REDISManager, error) {
	return redismanager.New(ctx, cfg)
}
