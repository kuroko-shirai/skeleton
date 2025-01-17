package statsanalyzer

import (
	"context"
	"skeleton/internal/configuration"
	"skeleton/internal/infrastructure/statsanalyzer"
)

type Factory struct{}

func (it *Factory) New(
	ctx context.Context,
	cfg *configuration.Configuration,
) (StatsAnalyzerRepo, error) {
	return statsanalyzer.New(ctx, cfg)
}
