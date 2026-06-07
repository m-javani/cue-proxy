// Copyright 2026 M. Javani
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"time"
)

// ListPeersAddrServerName resolves and returns the list of peers and their IP:port strings
func (a *ClusterAgent) ListPeersAddrServerName() []PeerResolvedInfo {
	a.topologyMu.RLock()
	voters := make([]string, len(a.cueTopology.Voters))
	copy(voters, a.cueTopology.Voters)
	a.topologyMu.RUnlock()

	resolved := make([]PeerResolvedInfo, 0, len(voters))

	for _, peer := range voters {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		addr, err := a.addressResolver.Resolve(ctx, peer)
		cancel()
		if err != nil {
			continue
		}
		resolved = append(resolved, PeerResolvedInfo{
			NodeID:     peer,
			Addr:       addr,
			ServerName: peer,
		})
	}

	if len(resolved) == 0 {
		a.logger.Sugar().Warnf("%s: the resolved peers list is 0/%d", a.proxyID, len(voters))
	}

	return resolved
}
