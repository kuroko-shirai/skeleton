package factories

import (
	"context"
	"skeleton/internal/configuration"
	"skeleton/internal/infrastructure/statsanalyzer"
	"skeleton/internal/repositories/service"
)

type StatsAnalyzerFactory struct{}

func (it *StatsAnalyzerFactory) New(
	ctx context.Context,
	cfg *configuration.Configuration,
) (service.Service, error) {
	return statsanalyzer.New(ctx, cfg)
}
