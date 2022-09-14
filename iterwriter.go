package iterwriter

import (
	"context"
	"fmt"
	"github.com/sfomuseum/go-timings"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"github.com/whosonfirst/go-writer/v2"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
)

func IterateWithWriter(ctx context.Context, wr writer.Writer, monitor timings.Monitor, iterator_uri string, iterator_paths ...string) error {

	iter_cb := func(ctx context.Context, path string, r io.ReadSeeker, args ...interface{}) error {

		// See this? It's important. We are rewriting path to a normalized WOF relative path
		// That means this will only work with WOF documents

		id, uri_args, err := uri.ParseURI(path)

		if err != nil {
			return fmt.Errorf("Unable to parse %s, %w", path, err)
		}

		rel_path, err := uri.Id2RelPath(id, uri_args)

		if err != nil {
			return fmt.Errorf("Unable to derive relative (WOF) path for %s, %w", path, err)
		}

		_, err = wr.Write(ctx, rel_path, r)

		if err != nil {
			return fmt.Errorf("Failed to write %s, %v", path, err)
		}

		go monitor.Signal(ctx)
		return nil
	}

	iter, err := iterator.NewIterator(ctx, iterator_uri, iter_cb)

	if err != nil {
		return fmt.Errorf("Failed to create new iterator, %w", err)
	}

	err = iter.IterateURIs(ctx, iterator_paths...)

	if err != nil {
		return fmt.Errorf("Failed to iterate paths, %w", err)
	}

	err = wr.Close(ctx)

	if err != nil {
		return fmt.Errorf("Failed to close ES writer, %w", err)
	}

	return nil
}
