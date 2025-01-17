package factories

import (
	"context"
	"skeleton/internal/configuration"
	"skeleton/internal/infrastructure/mssqlmanager"
)

type MSSQLManagerFactory struct{}

func (it *MSSQLManagerFactory) New(
	ctx context.Context,
	cfg *configuration.Configuration,
) (*mssqlmanager.MSSQLManager, error) {
	return mssqlmanager.New(ctx, cfg)
}
