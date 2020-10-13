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
	"sync"
)

type (
	jobArgs struct {
		res *jobResult

		rsyncArgs *rsyncJobArgs
	}

	jobFunc func(args *jobArgs)

	jobResult struct {
		jobID string
		done  chan error
	}
)

var (
	jobs    = make(map[string]*jobResult) // job ID -> jobResult
	jobsMtx sync.Mutex
)

func runJob(jobID string, job jobFunc, args *jobArgs) *jobResult {
	jobsMtx.Lock()
	defer jobsMtx.Unlock()

	if res, ok := jobs[jobID]; ok {
		return res
	}

	res := &jobResult{
		jobID: jobID,
		done:  make(chan error, 1),
	}

	jobs[jobID] = res
	args.res = res

	go job(args)

	return res
}

func removeJob(jobID string) {
	jobsMtx.Lock()
	defer jobsMtx.Unlock()

	delete(jobs, jobID)
}
