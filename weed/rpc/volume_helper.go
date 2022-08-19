package rpc

func (m *RemoteFile) BackendName() string {
	return m.BackendType + "." + m.BackendId
}
