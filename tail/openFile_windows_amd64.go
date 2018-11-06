package tail

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func tailFileOpen(path string) (*os.File, error) {
	fd, err := open(path, syscall.O_RDONLY, 0444)
	if err != nil {
		return nil, err
	}

	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		return nil, fmt.Errorf("unable to create file pointer from path: %s", path)
	}

	return f, nil
}

func open(path string, mode int, perm uint32) (fd syscall.Handle, err error) {
	if len(path) == 0 {
		return syscall.InvalidHandle, syscall.ERROR_FILE_NOT_FOUND
	}

	pathp, err := syscall.UTF16PtrFromString(path)

	if err != nil {
		return syscall.InvalidHandle, err
	}

	var access uint32

	switch mode & (syscall.O_RDONLY | syscall.O_WRONLY | syscall.O_RDWR) {
	case syscall.O_RDONLY:
		access = syscall.GENERIC_READ
	case syscall.O_WRONLY:
		access = syscall.GENERIC_WRITE
	case syscall.O_RDWR:
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	}

	if mode&syscall.O_CREAT != 0 {
		access |= syscall.GENERIC_WRITE
	}

	if mode&syscall.O_APPEND != 0 {
		access &^= syscall.GENERIC_WRITE
		access |= syscall.FILE_APPEND_DATA
	}

	sharemode := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_DELETE)

	var sa *syscall.SecurityAttributes

	if mode&syscall.O_CLOEXEC == 0 {
		sa = makeInheritSa()
	}

	var createmode uint32

	switch {
	case mode&(syscall.O_CREAT|syscall.O_EXCL) == (syscall.O_CREAT | syscall.O_EXCL):
		createmode = syscall.CREATE_NEW
	case mode&(syscall.O_CREAT|syscall.O_TRUNC) == (syscall.O_CREAT | syscall.O_TRUNC):
		createmode = syscall.CREATE_ALWAYS
	case mode&syscall.O_CREAT == syscall.O_CREAT:
		createmode = syscall.OPEN_ALWAYS
	case mode&syscall.O_TRUNC == syscall.O_TRUNC:
		createmode = syscall.TRUNCATE_EXISTING
	default:
		createmode = syscall.OPEN_EXISTING
	}

	h, e := syscall.CreateFile(pathp, access, sharemode, sa, createmode, syscall.FILE_ATTRIBUTE_NORMAL, 0)

	return h, e
}

func makeInheritSa() *syscall.SecurityAttributes {
	var sa syscall.SecurityAttributes

	sa.Length = uint32(unsafe.Sizeof(sa))
	sa.InheritHandle = 1

	return &sa
}
