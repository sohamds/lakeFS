package catalog

import (
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"
)

const (
	prefix        = "prefix1"
	defaultBranch = "main"
	anotherBranch = "lost-not-found"
)

func TestConfiguration_GetConfiguration(t *testing.T) {
	const (
		branchID        = 17
		anotherBranchID = 29
	)
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, prefix, defaultBranch)

	cfg := ExportConfiguration{
		Path:                   "/path/to/export",
		StatusPath:             "/path/to/status",
		LastKeysInPrefixRegexp: "*&@!#$",
	}

	if err := c.PutExportConfiguration(repo, defaultBranch, &cfg); err != nil {
		t.Fatal(err)
	}

	gotCfg, err := c.GetExportConfigurationForBranch(repo, anotherBranch)
	if !errors.Is(err, ErrBranchNotFound) {
		t.Errorf("get configuration for unconfigured branch failed: expected ErrBranchNotFound but got %s (and %+v)", err, gotCfg)
	}

	gotCfg, err = c.GetExportConfigurationForBranch(repo, defaultBranch)
	if err != nil {
		t.Errorf("get configuration for configured branch failed: %s", err)
	}
	if diffs := deep.Equal(cfg, gotCfg); diffs != nil {
		t.Errorf("got other configuration than expected: %s", diffs)
	}
}
