package catalog

import (
	"fmt"

	"github.com/treeverse/lakefs/db"
)

// ExportConfiguration describes the export configuration of a branch, as passed on wire, used
// internally, and stored in DB.
type ExportConfiguration struct {
	Path                   string `db:"export_path" json:"exportPath"`
	StatusPath             string `db:"export_status_path" json:"exportStatusPath"`
	LastKeysInPrefixRegexp string `db:"last_keys_in_prefix_regexp" json:"lastKeysInPrefixRegexp"`
}

// ExportConfigurationForBranch describes how to export BranchID.  It is stored in the database.
type ExportConfigurationForBranch struct {
	ExportConfiguration
	Repository string `db:"repository"`
	Branch     string `db:"branch"`
}

func (c *cataloger) GetExportConfigurationForBranch(repository string, branch string) (ExportConfiguration, error) {
	ret, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		var ret ExportConfiguration
		if err != nil {
			fmt.Println("[DEBUG] err 1", err)
			return nil, err
		}
		err = c.db.Get(&ret,
			`SELECT export_path, export_status_path, last_keys_in_prefix_regexp
                         FROM catalog_branches_export
                         WHERE branch_id = $1`, branchID)
		fmt.Println("[DEBUG] ret", ret, "err", err)
		return &ret, err
	})
	if ret == nil {
		return ExportConfiguration{}, err
	}
	return *ret.(*ExportConfiguration), err
}

func (c *cataloger) GetExportConfigurations() ([]ExportConfigurationForBranch, error) {
	ret := make([]ExportConfigurationForBranch, 0)
	rows, err := c.db.Query(
		`SELECT r.name repository, b.name branch,
                     e.export_path export_path, e.export_status_path export_status_path,
                     e.last_keys_in_prefix_regexp last_keys_in_prefix_regexp
                 FROM catalog_branches_export e JOIN catalog_branches b ON e.branch_id = b.branch_id
                      JOIN catalog_repositories r ON b.repository_id = r.id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var rec ExportConfigurationForBranch
		if err = rows.Scan(&rec); err != nil {
			return nil, fmt.Errorf("scan configuration %+v: %w", rows, err)
		}
		ret = append(ret, rec)
	}
	return ret, nil
}

func (c *cataloger) PutExportConfiguration(repository string, branch string, conf *ExportConfiguration) error {
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}
		fmt.Printf("[DEBUG] put export cfg branchID %d repo %s branch %s conf %+v\n", branchID, repository, branch, *conf)
		_, err = c.db.Exec(
			`INSERT INTO catalog_branches_export (
                             branch_id, export_path, export_status_path, last_keys_in_prefix_regexp)
                         VALUES ($1, $2, $3, $4)
                         ON CONFLICT (branch_id)
                         DO UPDATE SET (branch_id, export_path, export_status_path, last_keys_in_prefix_regexp) =
                             (EXCLUDED.branch_id, EXCLUDED.export_path, EXCLUDED.export_status_path, EXCLUDED.last_keys_in_prefix_regexp)`,
			branchID, conf.Path, conf.StatusPath, conf.LastKeysInPrefixRegexp)
		return nil, err
	})
	return err
}
