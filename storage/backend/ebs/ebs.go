package ebs

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/contiv/errored"
	"github.com/contiv/executor"
	"github.com/contiv/volplugin/errors"
	"github.com/contiv/volplugin/storage"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"golang.org/x/net/context"
	"golang.org/x/sys/unix"
)

const (
	BackendName = "ebs"
	// the minimum size is one gigabyte
	gigabyteAsBytes = 1024 * 1024 * 1024
)

type Driver struct {
	mountpath string
}

// NewMountDriver is a generator for Driver structs. It is used by the storage
// framework to yield new drivers on every creation.
func NewMountDriver(mountpath string) (storage.MountDriver, error) {
	return &Driver{mountpath: mountpath}, nil
}

// NewCRUDDriver is a generator for Driver structs. It is used by the storage
// framework to yield new drivers on every creation.
func NewCRUDDriver() (storage.CRUDDriver, error) {
	return &Driver{}, nil
}

func (c *Driver) Name() string {
	return BackendName
}

func getVolumeSizeInGB(size uint64) (int64, error) {
	if size < gigabyteAsBytes {
		return 0, errored.Errorf("the minimum volume size is 1 GB")
	}

	if (size % gigabyteAsBytes) != 0 {
		return 0, errored.Errorf("volume size can only be provided as whole gigabytes")
	}

	finalSize := size / gigabyteAsBytes
	return int64(finalSize), nil
}

func runWithTimeout(cmd *exec.Cmd, timeout time.Duration) (*executor.ExecResult, error) {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	return executor.NewCapture(cmd).Run(ctx)
}

// FIXME maybe this belongs in storage/ as it's more general?
func templateFSCmd(fscmd, devicePath string) string {
	for idx := 0; idx < len(fscmd); idx++ {
		if fscmd[idx] == '%' {
			if idx < len(fscmd)-1 && fscmd[idx+1] == '%' {
				idx++
				continue
			}
			var lhs, rhs string

			switch {
			case idx == 0:
				lhs = ""
				rhs = fscmd[1:]
			case idx == len(fscmd)-1:
				lhs = fscmd[:idx]
				rhs = ""
			default:
				lhs = fscmd[:idx]
				rhs = fscmd[idx+1:]
			}

			fscmd = fmt.Sprintf("%s%s%s", lhs, devicePath, rhs)
		}
	}

	return fscmd
}

// InternalName translates a volplugin `tenant/volume` name to an internal
// name suitable for the driver. Yields an error if impossible.
func (c *Driver) internalName(s string) (string, error) {
	strs := strings.SplitN(s, "/", 2)
	if len(strs) != 2 {
		return "", errored.Errorf("Invalid volume name %q, must be two parts", s)
	}

	if strings.Contains(strs[0], ".") {
		return "", errored.Errorf("Invalid policy name %q, cannot contain '.'", strs[0])
	}

	if strings.Contains(strs[1], "/") {
		return "", errored.Errorf("Invalid volume name %q, cannot contain '/'", strs[1])
	}

	return strings.Join(strs, "."), nil
}

func (c *Driver) mkfsVolume(fscmd, devicePath string, timeout time.Duration) error {
	cmd := exec.Command("/bin/sh", "-c", templateFSCmd(fscmd, devicePath))
	er, err := runWithTimeout(cmd, timeout)
	if err != nil || er.ExitStatus != 0 {
		return errored.Errorf("Error creating filesystem on %s with cmd: %q. Error: %v (%v) (%v) (%v)", devicePath, fscmd, er, err, strings.TrimSpace(er.Stdout), strings.TrimSpace(er.Stderr))
	}

	return nil
}

func (c *Driver) mkMountPath(intName string) (string, error) {
	// Directory to mount the volume
	volumePath := filepath.Join(c.mountpath, intName)
	rel, err := filepath.Rel(c.mountpath, volumePath)
	if err != nil || strings.Contains(rel, "..") {
		return "", errors.MountFailed.Combine(errored.Errorf("Calculated volume path would escape subdir jail: %v", volumePath))
	}

	return volumePath, nil
}

func (c *Driver) Create(do storage.DriverOptions) error {
	region := do.Volume.Params["region"]
	availabilityZone := do.Volume.Params["availabilityzone"]

	svc := ec2.New(session.New(), &aws.Config{Region: aws.String(region)})

	sizeInGB, err := getVolumeSizeInGB(do.Volume.Size)
	if err != nil {
		return err
	}
	vc := &volumeConfig{
		availabilityZone: availabilityZone,
		size:             sizeInGB,
		volumeType:       "gp2",
	}

	resp, err := createVolumeSynchronously(vc, svc, do.Timeout)
	if err != nil {
		return err
	}

	err = setVolumeNameTag(*resp.VolumeId, do.Volume.Name, svc)
	if err != nil {
		return errored.Errorf("encountered error while storing volume name in tag: %v", err)
	}

	return nil
}

func (c *Driver) Format(do storage.DriverOptions) error {
	region := do.Volume.Params["region"]
	// TODO: defer detach if attach failed
	instanceID, err := getInstanceID()
	if err != nil {
		return err
	}

	svc := ec2.New(session.New(), &aws.Config{Region: aws.String(region)})

	awsVolume, err := getVolumeWithName(do.Volume.Name, svc)
	if err != nil {
		return err
	}

	instance, err := getInstanceInfo(instanceID, svc)
	if err != nil {
		return errored.Errorf("failed to get information about the instance: %v", err)
	}

	device, err := findFreeBlockDevice(instance.BlockDeviceMappings)
	if err != nil {
		return err
	}

	_, err = attachVolumeSynchronously(awsVolume, instanceID, device, svc, do.Timeout)
	if err != nil {
		return errored.Errorf("failed to attach volume to instance: %v", err)
	}

	if err := c.mkfsVolume(do.FSOptions.CreateCommand, device, do.Timeout); err != nil {
		if _, err := detachVolumeSynchronously(awsVolume, instanceID, device, true, svc, do.Timeout); err != nil {
			log.Errorf("failed to detach volume after failing to create filesystem: %v", err)
		}
		return err
	}
	if _, err := detachVolumeSynchronously(awsVolume, instanceID, device, true, svc, do.Timeout); err != nil {
		log.Errorf("failed to detach volume after failing to create filesystem: %v", err)
	}

	return nil
}

func (c *Driver) Destroy(do storage.DriverOptions) error {
	region := do.Volume.Params["region"]
	svc := ec2.New(session.New(), &aws.Config{Region: aws.String(region)})

	awsVolume, err := getVolumeWithName(do.Volume.Name, svc)
	if err != nil {
		return errored.Errorf("failed to retrieve the AWS EBS volume name: %v", err)
	}

	err = deleteVolumeNameTag(awsVolume, do.Volume.Name, svc)
	if err != nil {
		return errored.Errorf("failed to untag the volume: %v", err)
	}

	if err = deleteVolumeSynchronously(awsVolume, svc, do.Timeout); err != nil {
		return errored.Errorf("failed to destroy the AWS EBS volume: %v", err)
	}

	return nil
}

func (c *Driver) Exists(do storage.DriverOptions) (bool, error) {
	volumes, err := c.List(storage.ListOptions{Params: do.Volume.Params})
	if err != nil {
		return false, err
	}

	for _, vol := range volumes {
		if vol.Name == do.Volume.Name {
			return true, nil
		}
	}

	return false, nil
}

func (c *Driver) List(lo storage.ListOptions) ([]storage.Volume, error) {
	list := []storage.Volume{}
	region := lo.Params["region"]

	svc := ec2.New(session.New(), &aws.Config{Region: aws.String(region)})

	volumes, err := listVolumes(svc)
	if err != nil {
		return []storage.Volume{}, err
	}

	for _, v := range volumes {
		list = append(list, storage.Volume{Name: *v.Value})
	}
	return list, nil
}

// prefer that to `ext4` which is the default.
func (c *Driver) Mount(do storage.DriverOptions) (*storage.Mount, error) {
	instanceID, err := getInstanceID()
	if err != nil {
		return nil, err
	}
	region := do.Volume.Params["region"]

	svc := ec2.New(session.New(), &aws.Config{Region: aws.String(region)})

	awsVolume, err := getVolumeWithName(do.Volume.Name, svc)
	if err != nil {
		return nil, err
	}

	instance, err := getInstanceInfo(instanceID, svc)
	if err != nil {
		return nil, errored.Errorf("failed to get information about the instance: %v", err)
	}

	device, err := findFreeBlockDevice(instance.BlockDeviceMappings)
	if err != nil {
		return nil, err
	}

	_, err = attachVolumeSynchronously(awsVolume, instanceID, device, svc, do.Timeout)
	if err != nil {
		return nil, errored.Errorf("failed to attach volume to instance: %v", err)
	}

	intName, err := c.internalName(do.Volume.Name)
	if err != nil {
		return nil, err
	}

	volumePath, err := c.mkMountPath(intName)
	if err != nil {
		return nil, err
	}

	// Create directory to mount
	if err := os.MkdirAll(c.mountpath, 0700); err != nil && !os.IsExist(err) {
		return nil, errored.Errorf("error creating %q directory: %v", c.mountpath, err)
	}

	if err := os.MkdirAll(volumePath, 0700); err != nil && !os.IsExist(err) {
		return nil, errored.Errorf("error creating %q directory: %v", volumePath, err)
	}

	// Obtain the major and minor node information about the device we're mounting.
	// This is critical for tuning cgroups and obtaining metrics for this device only.
	fi, err := os.Stat(device)
	if err != nil {
		return nil, errored.Errorf("Failed to stat EBS device %q: %v", device, err)
	}

	rdev := fi.Sys().(*syscall.Stat_t).Rdev

	major := rdev >> 8
	minor := rdev & 0xFF

	// Mount the EBS volume
	if err := unix.Mount(device, volumePath, do.FSOptions.Type, 0, ""); err != nil {
		return nil, errored.Errorf("Failed to mount EBS dev %q: %v", device, err)
	}

	return &storage.Mount{
		Device:   device,
		Path:     volumePath,
		Volume:   do.Volume,
		DevMajor: uint(major),
		DevMinor: uint(minor),
	}, nil
}

func (c *Driver) MountPath(do storage.DriverOptions) (string, error) {
	intName, err := c.internalName(do.Volume.Name)
	if err != nil {
		return "", err
	}

	volumePath, err := c.mkMountPath(intName)
	if err != nil {
		return "", err
	}
	return volumePath, nil
}

func (c *Driver) Unmount(do storage.DriverOptions) error {
	instanceID, err := getInstanceID()
	if err != nil {
		return err
	}
	region := do.Volume.Params["region"]

	intName, err := c.internalName(do.Volume.Name)
	if err != nil {
		return err
	}

	volumeDir, err := c.mkMountPath(intName)
	if err != nil {
		return err
	}

	// Unmount the EBS volume
	var retries int
	var lastErr error

	svc := ec2.New(session.New(), &aws.Config{Region: aws.String(region)})

	awsVolume, err := getVolumeWithName(do.Volume.Name, svc)
	if err != nil {
		return err
	}

	instance, err := getInstanceInfo(instanceID, svc)
	if err != nil {
		return errored.Errorf("failed to get information about the instance: %v", err)
	}

	device, err := findBlockVolumeBlockDevice(instance.BlockDeviceMappings, awsVolume)
	if err != nil {
		return errored.Errorf("failed to find block device for attached volume")
	}

retry:
	if retries < 3 {
		if err := unix.Unmount(volumeDir, 0); err != nil && err != unix.ENOENT && err != unix.EINVAL {
			lastErr = errored.Errorf("Failed to unmount %q (retrying): %v", volumeDir, err)
			log.Error(lastErr)
			retries++
			time.Sleep(100 * time.Millisecond)
			goto retry
		}
	} else {
		return errored.Errorf("Failed to umount after 3 retries").Combine(lastErr.(*errored.Error))
	}

	// Remove the mounted directory
	// FIXME remove all, but only after the FIXME above.
	if err := os.Remove(volumeDir); err != nil && !os.IsNotExist(err) {
		log.Error(errored.Errorf("error removing %q directory: %v", volumeDir, err))
		goto retry
	}

	_, err = detachVolumeSynchronously(awsVolume, instanceID, device, false, svc, do.Timeout)
	if err != nil {
		return errored.Errorf("failed to detach volume from instance: %v", err)
	}

	return nil
}

/*
func (c *Driver) CreateSnapshot(snapName string, do storage.DriverOptions) error {
	return nil
}

func (c *Driver) RemoveSnapshot(snapName string, do storage.DriverOptions) error {
	return nil
}

func (c *Driver) ListSnapshots(do storage.DriverOptions) ([]string, error) {
	return []string{}, nil
}

func (c *Driver) CopySnapshot(do storage.DriverOptions, snapName, newName string) error {
	return nil
}
*/

func (c *Driver) Mounted(timeout time.Duration) ([]*storage.Mount, error) {
	return []*storage.Mount{}, nil
}

func (c *Driver) Validate(do *storage.DriverOptions) error {
	if err := do.Validate(); err != nil {
		return err
	}

	if _, err := getVolumeSizeInGB(do.Volume.Size); err != nil {
		return err
	}

	if do.Volume.Params["region"] == "" {
		return errored.Errorf("AWS region is missing in ebs storage driver.")
	}

	// AWS credentials are picked up automatically from ~/.aws/credentials
	return nil
}
