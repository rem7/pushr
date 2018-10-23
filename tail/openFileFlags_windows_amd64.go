package tail

import (
	"os"
	"syscall"
)

const openFileFlag = os.O_RDONLY | syscall.FILE_SHARE_DELETE | syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_READ
