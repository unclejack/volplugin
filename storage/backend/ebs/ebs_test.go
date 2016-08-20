package ebs

import (
	"path/filepath"
	. "testing"
	"time"

	"github.com/contiv/volplugin/storage"

	. "gopkg.in/check.v1"
)

const myMountpath = "/mnt/ebs"

var filesystems = map[string]storage.FSOptions{
	"ext4": {
		Type:          "ext4",
		CreateCommand: "mkfs.ext4 -m0 %",
	},
}

var volumeSpec = storage.Volume{
	Name: "test/pithos",
	Size: 4 * 1024 * 1024 * 1024,
	Params: storage.Params{"region": "eu-central-1",
		"availabilityzone": "eu-central-1b"},
}

var volumeSpecTestPool = storage.Volume{
	Name: "test/pithos",
	Size: 4 * 1024 * 1024 * 1024,
	Params: storage.Params{"region": "eu-central-1",
		"availabilityzone": "eu-central-1b"},
}

type ebsSuite struct{}

var _ = Suite(&ebsSuite{})

func TestEBS(t *T) { TestingT(t) }

func (s *ebsSuite) TestName(c *C) {
	crudDriver, err := NewCRUDDriver()
	c.Assert(err, IsNil)
	c.Assert(crudDriver.Name(), Equals, BackendName)
}

func (s *ebsSuite) TestCreateVolume(c *C) {
	crudDriver, err := NewCRUDDriver()
	c.Assert(err, IsNil)
	driverOpts := storage.DriverOptions{
		Volume:    volumeSpec,
		FSOptions: filesystems["ext4"],
		Timeout:   10 * time.Second,
	}
	c.Assert(crudDriver.Create(driverOpts), IsNil)
	c.Assert(crudDriver.Destroy(driverOpts), IsNil)
}

func (s *ebsSuite) TestValidate(c *C) {
	crudDriver, _ := NewCRUDDriver()
	driverOpts := storage.DriverOptions{
		Volume:    volumeSpec,
		FSOptions: filesystems["ext4"],
		Timeout:   60 * time.Second,
	}
	c.Assert(crudDriver.Validate(&driverOpts), IsNil)
}

func (s *ebsSuite) TestMkfsVolume(c *C) {
	driver := Driver{mountpath: myMountpath}

	err := driver.mkfsVolume("echo %s; sleep 1", "fake-fake-fake", 3*time.Second)
	c.Assert(err, IsNil)

	err = driver.mkfsVolume("echo %s; sleep 2", "fake-fake-fake", 1*time.Second)
	c.Assert(err, NotNil)
}

func (s *ebsSuite) TestFormatVolume(c *C) {
	crudDriver, err := NewCRUDDriver()
	c.Assert(err, IsNil)
	driverOpts := storage.DriverOptions{
		Volume:    volumeSpec,
		FSOptions: filesystems["ext4"],
		Timeout:   60 * time.Second,
	}
	c.Assert(crudDriver.Create(driverOpts), IsNil)
	c.Assert(crudDriver.Format(driverOpts), IsNil)
	c.Assert(crudDriver.Destroy(driverOpts), IsNil)
}

func (s *ebsSuite) TestVolumeExists(c *C) {
	crudDriver, err := NewCRUDDriver()
	c.Assert(err, IsNil)
	driverOpts := storage.DriverOptions{
		Volume:    volumeSpec,
		FSOptions: filesystems["ext4"],
		Timeout:   10 * time.Second,
	}
	c.Assert(crudDriver.Create(driverOpts), IsNil)
	exists, err := crudDriver.Exists(driverOpts)
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, true)
	c.Assert(crudDriver.Destroy(driverOpts), IsNil)

	exists, err = crudDriver.Exists(driverOpts)
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, false)
}

func (s *ebsSuite) TestMountVolumeMountPath(c *C) {
	crudDriver, err := NewCRUDDriver()
	mountDriver, err := NewMountDriver(myMountpath)
	c.Assert(err, IsNil)
	driverOpts := storage.DriverOptions{
		Volume:    volumeSpec,
		FSOptions: filesystems["ext4"],
		Timeout:   20 * time.Second,
	}
	c.Assert(crudDriver.Create(driverOpts), IsNil)
	c.Assert(crudDriver.Format(driverOpts), IsNil)
	_, err = mountDriver.Mount(driverOpts)
	c.Assert(err, IsNil)
	mountPath, err := mountDriver.MountPath(driverOpts)
	c.Assert(err, IsNil)
	c.Assert(mountPath, Equals, filepath.Join(myMountpath, "test.pithos"))
	c.Assert(mountDriver.Unmount(driverOpts), IsNil)
	c.Assert(crudDriver.Destroy(driverOpts), IsNil)
}
