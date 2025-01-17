package factories

import (
	"context"
	"skeleton/internal/configuration"
	"skeleton/internal/infrastructure/userservice"
	"skeleton/internal/repositories/mssqlmanager"
	"skeleton/internal/repositories/service"
)

type UserServiceFactory struct{}

func (it *UserServiceFactory) New(
	ctx context.Context,
	cfg *configuration.Configuration,
	dm mssqlmanager.MSSQLManagerRepo,
) (service.Service, error) {
	return userservice.New(ctx, cfg, dm)
}
