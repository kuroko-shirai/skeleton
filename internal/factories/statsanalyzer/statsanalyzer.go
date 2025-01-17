package statsanalyzer

import (
	"context"
	"skeleton/internal/configuration"
	"skeleton/internal/infrastructure/statsanalyzer"
	repo "skeleton/internal/repositories/statsanalyzer"
)

type Factory struct{}

func (it *Factory) New(
	ctx context.Context,
	cfg *configuration.Configuration,
) (repo.StatsAnalyzerRepo, error) {
	return statsanalyzer.New(ctx, cfg)
}
