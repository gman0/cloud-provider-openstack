/*
Copyright 2020 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package compatibility

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"

	"k8s.io/klog/v2"
)

type rsyncJobArgs struct {
	src string
	dst string

	// If set to true, stderr is logged into `dst/rsync-errors-<JOB ID>.txt`.
	// os.Stderr is used by default.
	stderrToFile bool
	// I/O timeout (rsync --timeout)
	ioTimeout int
}

func rsyncJob(args *jobArgs) {
	var err error

	defer func() {
		klog.V(3).Infof("Finished rsync job %s, error: %v", args.res.jobID, err)
		args.res.done <- err
	}()

	// Setup logging

	var logWriter io.Writer

	if args.rsyncArgs.stderrToFile {
		logFilename := fmt.Sprintf("rsync-errors-%s.txt", args.res.jobID)
		logFilepath := path.Join(args.rsyncArgs.dst, logFilename)

		f, err := os.OpenFile(logFilepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0655)
		if err != nil {
			err = fmt.Errorf("couldn't create log file in %s: %v", logFilepath, err)
			return
		}

		defer func() {
			pos, _ := f.Seek(0, io.SeekCurrent)
			f.Close()

			if pos == 0 {
				// We haven't wrote anything, error log is empty and may be deleted
				os.Remove(logFilepath)
			} else {
				// There was some error output, mention it in the error message
				err = fmt.Errorf("rsync reported errors, please see %s in destination for more details: %v", logFilename, err)
			}
		}()

		logWriter = bufio.NewWriter(f)
	} else {
		logWriter = os.Stderr
	}

	// Run rsync

	cmd := exec.Command("rsync",
		"--recursive",
		"--perms",
		"--links",
		"--acls",
		"--partial-dir=.rsync-partial",
		fmt.Sprintf("--timeout=%d", args.rsyncArgs.ioTimeout),
		// Trailing slash at the end of the source path makes rsync
		// copy directory contents only rather than that directory.
		args.rsyncArgs.src+"/",
		args.rsyncArgs.dst,
	)

	cmd.Stderr = logWriter

	klog.V(3).Infof("Starting rsync job %s: running rsync %v", args.res.jobID, cmd.Args)

	if err = cmd.Run(); err != nil {
		err = fmt.Errorf("rsync failed: %v", err)
	}
}
