package user

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/datablast-analytics/blast/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	blastHomeDir       = ".blast"
	homeDirPermissions = 0o755
	virtualEnvsPath    = "virtualenvs"
)

type ConfigManager struct {
	fs afero.Fs

	lock sync.Mutex

	userHomeDir  string
	blastHomeDir string
}

func NewConfigManager(fs afero.Fs) *ConfigManager {
	return &ConfigManager{
		fs: fs,
	}
}

func (c *ConfigManager) EnsureHomeDirExists() error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	blastConfigPath := filepath.Join(homeDir, blastHomeDir)
	if !path.DirExists(c.fs, blastConfigPath) {
		err = c.fs.MkdirAll(blastConfigPath, homeDirPermissions)
		if err != nil {
			return errors.Wrap(err, "failed to create blast home directory")
		}
	}

	c.userHomeDir = homeDir
	c.blastHomeDir = blastConfigPath

	return nil
}

func (c *ConfigManager) makePathUnderConfig(dirName string) string {
	return filepath.Join(c.blastHomeDir, dirName)
}

func (c *ConfigManager) MakeVirtualenvPath(dirName string) string {
	return filepath.Join(c.blastHomeDir, virtualEnvsPath, dirName)
}

func (c *ConfigManager) EnsureVirtualenvDirExists() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.EnsureHomeDirExists()
	if err != nil {
		return err
	}

	venvPath := c.makePathUnderConfig(virtualEnvsPath)
	if !path.DirExists(c.fs, venvPath) {
		err = c.fs.MkdirAll(venvPath, homeDirPermissions)
		if err != nil {
			return errors.Wrap(err, "failed to create virtualenvs directory under blast home")
		}
	}

	return nil
}
