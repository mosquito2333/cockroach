// Code generated by "stringer -type=ErrorCode"; DO NOT EDIT.

package sqlproxyccl

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[CodeAuthFailed-1]
	_ = x[CodeBackendReadFailed-2]
	_ = x[CodeBackendWriteFailed-3]
	_ = x[CodeClientReadFailed-4]
	_ = x[CodeClientWriteFailed-5]
	_ = x[CodeUnexpectedInsecureStartupMessage-6]
	_ = x[CodeSNIRoutingFailed-7]
	_ = x[CodeUnexpectedStartupMessage-8]
	_ = x[CodeParamsRoutingFailed-9]
	_ = x[CodeBackendDown-10]
	_ = x[CodeBackendRefusedTLS-11]
	_ = x[CodeBackendDisconnected-12]
	_ = x[CodeClientDisconnected-13]
	_ = x[CodeProxyRefusedConnection-14]
	_ = x[CodeExpiredClientConnection-15]
	_ = x[CodeIdleDisconnect-16]
}

const _ErrorCode_name = "CodeAuthFailedCodeBackendReadFailedCodeBackendWriteFailedCodeClientReadFailedCodeClientWriteFailedCodeUnexpectedInsecureStartupMessageCodeSNIRoutingFailedCodeUnexpectedStartupMessageCodeParamsRoutingFailedCodeBackendDownCodeBackendRefusedTLSCodeBackendDisconnectedCodeClientDisconnectedCodeProxyRefusedConnectionCodeExpiredClientConnectionCodeIdleDisconnect"

var _ErrorCode_index = [...]uint16{0, 14, 35, 57, 77, 98, 134, 154, 182, 205, 220, 241, 264, 286, 312, 339, 357}

func (i ErrorCode) String() string {
	i -= 1
	if i < 0 || i >= ErrorCode(len(_ErrorCode_index)-1) {
		return "ErrorCode(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _ErrorCode_name[_ErrorCode_index[i]:_ErrorCode_index[i+1]]
}
