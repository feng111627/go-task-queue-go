package taskqueue

import "sync/atomic"

func int32Add(p *int32, d int32) int32 { return atomic.AddInt32(p, d) }
