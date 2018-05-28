package observability

import (
	"context"

	"go.opencensus.io/tag"
)

func TagKeyValuesIntoContext(ctx context.Context, key tag.Key, values ...string) (context.Context, error) {
	insertions := make([]tag.Mutator, len(values))
	for i, value := range values {
		insertions[i] = tag.Insert(key, value)
	}
	return tag.New(ctx, insertions...)
}
