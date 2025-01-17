package main

import (
	"context"
	"log"

	"skeleton/internal/infra"
)

func main() {
	ctx := context.Background()

	app, err := infra.New(ctx)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer app.Down(ctx)

	app.Run(ctx)
}
