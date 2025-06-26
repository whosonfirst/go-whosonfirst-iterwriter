package iterwriter

import (
	"context"
	"fmt"
	// "io"
	"log/slog"

	"github.com/sfomuseum/go-timings"
	"github.com/whosonfirst/go-whosonfirst-iterate/v3"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"github.com/whosonfirst/go-writer/v3"
)

// IterateWithWriterAndCallback will process all files emitted by 'iterator_uri' and 'iterator_paths' passing each to 'iter_cb'
// (which it is assumed will eventually pass the file to 'wr').
func Iterate(ctx context.Context, wr writer.Writer, monitor timings.Monitor, iterator_uri string, iterator_paths ...string) error {

	forgiving := false

	iter, err := iterate.NewIterator(ctx, iterator_uri)

	if err != nil {
		return fmt.Errorf("Failed to create new iterator, %w", err)
	}

	defer iter.Close()

	for rec, err := range iter.Iterate(ctx, iterator_paths...) {

		if err != nil {
			return err
		}

		defer rec.Body.Close()

		logger := slog.Default()
		logger = logger.With("path", rec.Path)

		// See this? It's important. We are rewriting path to a normalized WOF relative path
		// That means this will only work with WOF documents

		id, uri_args, err := uri.ParseURI(rec.Path)

		if err != nil {
			slog.Error("Failed to parse URI", "error", err)
			return fmt.Errorf("Unable to parse %s, %w", rec.Path, err)
		}

		logger = logger.With("id", id)

		rel_path, err := uri.Id2RelPath(id, uri_args)

		if err != nil {
			slog.Error("Failed to derive URI", "error", err)
			return fmt.Errorf("Unable to derive relative (WOF) path for %s, %w", rec.Path, err)
		}

		logger = logger.With("rel_path", rel_path)

		_, err = wr.Write(ctx, rel_path, rec.Body)

		if err != nil {

			slog.Error("Failed to write record", "error", err)

			if !forgiving {
				return fmt.Errorf("Failed to write record for %s, %w", rel_path, err)
			}
		}

		go monitor.Signal(ctx)
	}

	err = wr.Close(ctx)

	if err != nil {
		return fmt.Errorf("Failed to close writer, %w", err)
	}

	return nil
}
