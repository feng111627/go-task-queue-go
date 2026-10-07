package taskqueue

import "errors"

var (
	errBoom    = errors.New("模拟可重试失败: 下游超时")
	errInvalid = errors.New("模拟不可重试失败: 参数非法")
)
