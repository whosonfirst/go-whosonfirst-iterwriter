# go-whosonfirst-iterwriter

Opinionated Go package to dispatch `whosonfirst/go-whosonfirst-iterate/v3` events to `whosonfirst/go-writer/v3` instances.


## Deprecation notice

This package has been deprecated. Its functionality has been moved in to the [whosonfirst/go-whosonfirst](https://github.com/whosonfirst/go-whosonfirst) package. Efforts have been made to keep changes as minimal as possible. In most (but not all) cases update should be as simple as replacing:

```
import "github.com/whosonfirst/go-whosonfirst-iterwriter/v4`
```

With:

```
import iterwriter "github.com/whosonfirst/go-whosonfirst/v4/iterate/writer"
```

## Documentation

Documentation is incomplete at this time.

For a "real world" example of how to use this package have a look at the code in the [whosonfirst/go-whosonfirst-tippecanoe](https://github.com/whosonfirst/go-whosonfirst-tippecanoe) package.

## See also

* https://github.com/whosonfirst/go-whosonfirst-iterate
* https://github.com/whosonfirst/go-whosonfirst-writer
