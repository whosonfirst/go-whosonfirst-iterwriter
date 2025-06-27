package main

import (
	"context"
	"log"

	"github.com/whosonfirst/go-whosonfirst-iterwriter/v4/app/iterwriter"
)

func main() {
	ctx := context.Background()
	err := iterwriter.Run(ctx)

	if err != nil {
		log.Fatalf("Failed to run iterwriter, %v", err)
	}
}
