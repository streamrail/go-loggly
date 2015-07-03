package loggly

import . "github.com/visionmedia/go-debug"
import . "encoding/json"
import "io/ioutil"
import "net/http"
import "strings"
import "bytes"
import "time"
import "sync"
import "fmt"
import "os"
import "io"

const Version = "0.4.3"

const api = "https://logs-01.loggly.com/bulk/{token}"

var debug = Debug("loggly")

var nl = []byte{'\n'}

type Level int

const (
	DEBUG Level = iota
	INFO
	NOTICE
	WARNING
	ERROR
	CRITICAL
	ALERT
	EMERGENCY
)

// Loggly client.
type Client struct {
	// Optionally output logs to the given writer.
	Writer io.Writer

	// Log level defaulting to INFO.
	Level Level

	// Size of buffer before flushing [100]
	BufferSize int

	// Flush interval regardless of size [5s]
	FlushInterval time.Duration

	// Loggly end-point.
	Endpoint string

	// Token string.
	Token string

	// Default properties.
	Defaults   map[string]interface{}
	buffer     map[string][][]byte
	tags       []string
	MinimalLog bool
	sync.Mutex
}

// New returns a new loggly client with the given `token`.
// Optionally pass `tags` or set them later with `.Tag()`.
func New(token string, bufferSize int, minLog bool, tags ...string) *Client {
	host, err := os.Hostname()
	defaults := map[string]interface{}{}

	if err == nil {
		defaults["hostname"] = host
	}

	c := &Client{
		Level:         INFO,
		BufferSize:    bufferSize,
		FlushInterval: 5 * time.Second,
		Token:         token,
		Endpoint:      strings.Replace(api, "{token}", token, 1),
		buffer:        make(map[string][][]byte),
		MinimalLog:    minLog,
		Defaults:      defaults,
	}

	c.Tag(tags...)

	go c.start()

	return c
}

// Send buffers `msg` for async sending.
func (c *Client) Send(msg map[string]interface{}) error {
	if c.MinimalLog {
		delete(msg, "filename")
		delete(msg, "func")
		delete(msg, "hostname")
		delete(msg, "line")
	} else {
		if _, exists := msg["timestamp"]; !exists {
			msg["timestamp"] = time.Now().UnixNano() / int64(time.Millisecond)
		}
		merge(msg, c.Defaults)
	}

	var tagbuffer string
	if val, ok := msg["partnerID"]; ok {
		tagbuffer = val.(string)
	} else {
		tagbuffer = "notag"
	}
	json, err := Marshal(msg)
	if err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()

	if c.Writer != nil {
		fmt.Fprintf(c.Writer, "%s\n", string(json))
	}

	c.buffer[tagbuffer] = append(c.buffer[tagbuffer], json)

	debug("buffer (%d/%d) %v", len(c.buffer[tagbuffer]), c.BufferSize, msg)

	if len(c.buffer) >= c.BufferSize {
		go c.Flush()
	}

	return nil
}

// Write raw data to loggly.
func (c *Client) Write(b []byte) (int, error) {
	c.Lock()
	defer c.Unlock()

	if c.Writer != nil {
		fmt.Fprintf(c.Writer, "%s", b)
	}

	c.buffer["notag"] = append(c.buffer["notag"], b)

	debug("buffer (%d/%d) %q", len(c.buffer), c.BufferSize, b)

	if len(c.buffer) >= c.BufferSize {
		go c.Flush()
	}

	return len(b), nil
}

// Debug log.
func (c *Client) Debug(t string, props ...map[string]interface{}) error {
	if c.Level > DEBUG {
		return nil
	}
	msg := map[string]interface{}{"level": "debug", "component": t}
	merge(msg, props...)
	return c.Send(msg)
}

// Info log.
func (c *Client) Info(t string, props ...map[string]interface{}) error {
	if c.Level > INFO {
		return nil
	}
	msg := map[string]interface{}{"level": "info", "component": t}
	merge(msg, props...)
	return c.Send(msg)
}

// Notice log.
func (c *Client) Notice(t string, props ...map[string]interface{}) error {
	if c.Level > NOTICE {
		return nil
	}
	msg := map[string]interface{}{"level": "notice", "component": t}
	merge(msg, props...)
	return c.Send(msg)
}

// Warning log.
func (c *Client) Warn(t string, props ...map[string]interface{}) error {
	if c.Level > WARNING {
		return nil
	}
	msg := map[string]interface{}{"level": "warning", "component": t}
	merge(msg, props...)
	return c.Send(msg)
}

// Error log.
func (c *Client) Error(t string, props ...map[string]interface{}) error {
	if c.Level > ERROR {
		return nil
	}
	msg := map[string]interface{}{"level": "error", "component": t}
	merge(msg, props...)
	return c.Send(msg)
}

// Critical log.
func (c *Client) Critical(t string, props ...map[string]interface{}) error {
	if c.Level > CRITICAL {
		return nil
	}
	msg := map[string]interface{}{"level": "critical", "component": t}
	merge(msg, props...)
	return c.Send(msg)
}

// Alert log.
func (c *Client) Alert(t string, props ...map[string]interface{}) error {
	if c.Level > ALERT {
		return nil
	}
	msg := map[string]interface{}{"level": "alert", "component": t}
	merge(msg, props...)
	return c.Send(msg)
}

// Emergency log.
func (c *Client) Emergency(t string, props ...map[string]interface{}) error {
	if c.Level > EMERGENCY {
		return nil
	}
	msg := map[string]interface{}{"level": "emergency", "component": t}
	merge(msg, props...)
	return c.Send(msg)
}

// Flush the buffered messages.
func (c *Client) Flush() error {
	for k, _ := range c.buffer {
		if len(c.buffer[k]) == 0 {
			debug("no messages to flush")
			continue
		}
		//Lock mutex per buffer in map
		c.Lock()
		debug("flushing %d messages", len(c.buffer[k]))
		body := bytes.Join(c.buffer[k], nl)
		//release mutex after buffer emptyed
		c.Unlock()
		c.buffer[k] = nil

		client := &http.Client{}
		debug("POST %s with %d bytes", c.Endpoint, len(body))
		req, err := http.NewRequest("POST", c.Endpoint, bytes.NewBuffer(body))
		if err != nil {
			debug("error: %v", err)
			return err
		}

		req.Header.Add("User-Agent", "go-loggly (version: "+Version+")")
		req.Header.Add("Content-Type", "text/plain")
		req.Header.Add("Content-Length", string(len(body)))

		tags := k
		if tags != "notag" {
			req.Header.Add("X-Loggly-Tag", tags)
		}

		res, err := client.Do(req)
		if err != nil {
			debug("error: %v", err)
			return err
		}

		defer res.Body.Close()

		debug("%d response", res.StatusCode)
		if res.StatusCode >= 400 {
			resp, _ := ioutil.ReadAll(res.Body)
			debug("error: %s", string(resp))
			return err
		}

	}
	return nil
}

// Tag adds the given `tags` for all logs.
func (c *Client) Tag(tags ...string) {
	c.Lock()
	defer c.Unlock()

	for _, tag := range tags {
		c.tags = append(c.tags, tag)
	}
}

// Return a comma-delimited tag list string.
func (c *Client) tagsList() string {
	c.Lock()
	defer c.Unlock()

	return strings.Join(c.tags, ",")
}

// Start flusher.
func (c *Client) start() {
	for {
		time.Sleep(c.FlushInterval)
		debug("interval %v reached", c.FlushInterval)
		c.Flush()
	}
}

// Merge others into a.
func merge(a map[string]interface{}, others ...map[string]interface{}) {
	for _, msg := range others {
		for k, v := range msg {
			a[k] = v
		}
	}
}
