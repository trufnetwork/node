package driver

import "time"

type ConnOpt func(*Connection)

func WithPath(path string) ConnOpt {
	return func(c *Connection) {
		c.path = path
	}
}

func ReadOnly() ConnOpt {
	return func(c *Connection) {
		c.readOnly = true
	}
}

func WithLockWaitTime(t time.Duration) ConnOpt {
	return func(c *Connection) {
		c.lockWaitTime = t
	}
}

func WithInjectableVars(vars []*InjectableVar) ConnOpt {
	return func(c *Connection) {
		c.injectables = vars
	}
}