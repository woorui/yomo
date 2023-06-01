package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"time"

	"github.com/yomorun/yomo/core/auth"
	"github.com/yomorun/yomo/core/frame"
	"github.com/yomorun/yomo/core/ylog"
	"golang.org/x/exp/slog"
)

// PersistentStream defines the struct of persistent stream.
// The PersistentStream holds a DataStream and keep the DataStream alive unless Close() is called.
type PersistentStream struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	streamCreater StreamCreater
	handler       StreamHandler

	logger *slog.Logger
	opt    PersistentStreamOption

	frameWriteChan   chan frame.Frame
	frameReadChan    chan frameReadResult
	ReconnectionChan chan error
	done             chan struct{}
}

// PersistentStreamOption is a struct representing options for a persistent stream.
type PersistentStreamOption struct {
	// Logger is the logger to use.
	Logger *slog.Logger

	// NoBlockWrite specifies whether WriteFrame should block or not.
	NoBlockWrite bool

	// ConnectUntilSucceed specifies whether to keep trying to connect until a successful connection is made.
	ConnectUntilSucceed bool
}

// StreamCreater is an interface for creating streams.
type StreamCreater interface {
	// StreamInfo returns information about the stream.
	// The information of the data stream be created is from here.
	StreamInfo() StreamInfo

	// Credential returns the credential used to authenticate the stream.
	Credential() *auth.Credential

	// Opener opens ControlStream, ControlStream opens DataStream.
	Opener() ClientControlStreamOpener
}

// StreamHandler defines an interface for handling stream frames and errors.
type StreamHandler interface {
	// HandleFrame handles frames read from the stream.
	HandleFrame(frame.Frame)

	// HandleError handles an error that occurred during stream read and write.
	HandleError(error)
}

// NewPersistentStream returns a new persistent stream,
// The stream don't hold a real connection unless Connect() is called.
func NewPersistentStream(
	ctx context.Context,
	creater StreamCreater,
	handler StreamHandler,
	opt PersistentStreamOption,
) *PersistentStream {
	ctx, ctxCancel := context.WithCancel(ctx)

	logger := opt.Logger
	if logger == nil {
		logger = ylog.Default()
	}

	ps := &PersistentStream{
		ctx:              ctx,
		ctxCancel:        ctxCancel,
		streamCreater:    creater,
		handler:          handler,
		opt:              opt,
		logger:           logger,
		frameWriteChan:   make(chan frame.Frame),
		frameReadChan:    make(chan frameReadResult),
		ReconnectionChan: make(chan error),
		done:             make(chan struct{}),
	}

	return ps
}

// Connect connects to reomte.
// if ConnectUntilSucceed is true, it will keep trying to connect until a successful connection is made.
func (ps *PersistentStream) Connect(ctx context.Context, addr string) error {
	if ps.opt.ConnectUntilSucceed {
		return ps.connectUntilSucceed(ctx, addr)
	}
	return ps.connect(ctx, addr)
}

func (ps *PersistentStream) connect(ctx context.Context, addr string) error {
	stream, cleanFn, err := ps.openStream(ctx, addr)
	if err != nil {
		return err
	}

	go ps.precessStream(ctx, addr, stream, cleanFn)

	return nil
}

func (ps *PersistentStream) connectUntilSucceed(ctx context.Context, addr string) error {
	stream, cleanFn := OpenStreamUntilSucceed(
		ctx,
		addr,
		ps.openStream,
		isStreamBeRejected,
		ps.logger,
	)
	go ps.precessStream(ctx, addr, stream, cleanFn)

	return nil
}

// Close closes persistent stream.
func (ps *PersistentStream) Close() error {
	select {
	case <-ps.ctx.Done():
		return nil
	default:
	}
	ps.ctxCancel()

	// wait for the stream to be closed.
	<-ps.done

	return nil
}

// WriteFrame writes a frame to the persistent stream.
func (ps *PersistentStream) WriteFrame(f frame.Frame) error {
	if ps.opt.NoBlockWrite {
		return ps.noBlockWriteFrame(f)
	}
	return ps.blockWriteFrame(f)
}

func (ps *PersistentStream) blockWriteFrame(f frame.Frame) error {
	ps.frameWriteChan <- f
	return nil
}

func (ps *PersistentStream) noBlockWriteFrame(f frame.Frame) error {
	select {
	case ps.frameWriteChan <- f:
	default:
	}
	return errors.New("yomo: failed write to stream")
}

func (ps *PersistentStream) openStream(ctx context.Context, addr string) (DataStream, func(string), error) {
	cleanFn := func(string) {}

	controls, err := ps.streamCreater.Opener().Open(ctx, addr)
	if err != nil {
		return nil, cleanFn, err
	}

	err = controls.Authenticate(ps.streamCreater.Credential())
	if err != nil {
		return nil, cleanFn, err
	}

	err = controls.RequestStream(&frame.HandshakeFrame{
		Name:            ps.streamCreater.StreamInfo().Name(),
		ID:              ps.streamCreater.StreamInfo().ID(),
		StreamType:      byte(ps.streamCreater.StreamInfo().StreamType()),
		ObserveDataTags: ps.streamCreater.StreamInfo().ObserveDataTags(),
	})

	if err != nil {
		return nil, cleanFn, err
	}

	stream, err := controls.AcceptStream(ctx)
	if err != nil {
		return nil, cleanFn, err
	}

	cleanFn = func(s string) {
		stream.Close()
		controls.CloseWithError(s)
	}

	return stream, cleanFn, nil
}

func isStreamBeRejected(err error) bool {
	if err == nil {
		return false
	}
	return errors.As(err, new(ErrAuthenticateFailed))
}

type frameReadResult struct {
	frame frame.Frame
	err   error
}

func (ps *PersistentStream) readFrame(stream DataStream) {
	for {
		f, err := stream.ReadFrame()

		readResult := frameReadResult{
			frame: f,
			err:   err,
		}

		ps.frameReadChan <- readResult

		// if stream is closed, return it.
		if err != nil {
			return
		}
	}
}

func (ps *PersistentStream) precessStream(ctx context.Context, addr string, stream DataStream, cleanFn func(string)) {
	defer func() {
		cleanFn("stream closed")
		close(ps.done)
	}()

	// start read from stream.
	go ps.readFrame(stream)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ps.ctx.Done():
			return
		case rerr := <-ps.ReconnectionChan:
			// clean last stream with reconnect reason.
			cleanFn(fmt.Sprintf("reconnect for %+v", rerr))
			stream, cleanFn = OpenStreamUntilSucceed(
				ctx,
				addr,
				ps.openStream,
				isStreamBeRejected,
				ps.logger,
			)
			go ps.readFrame(stream)
		case result := <-ps.frameReadChan:
			if result.err != nil {
				ps.handleError(result.err)
				continue
			}
			func() {
				defer func() {
					if e := recover(); e != nil {
						const size = 64 << 10
						buf := make([]byte, size)
						buf = buf[:runtime.Stack(buf, false)]

						perr := fmt.Errorf("%v", e)
						ps.handleError(fmt.Errorf("yomo: stream panic: %v\n%s", perr, buf))
					}
				}()
				ps.handler.HandleFrame(result.frame)
			}()
		}
	}
}

// handleError handles errors that occur during frame reading and writing by performing the following actions:
// Sending the error to the error function (handler.HandleError).
// Closing the client if the data stream has been closed.
// Always attempting to reconnect if an error is encountered.
func (ps *PersistentStream) handleError(err error) {
	if err == nil {
		return
	}

	ps.handler.HandleError(err)

	// exit client program if stream has be closed.
	if err == io.EOF {
		ps.ctxCancel()
		return
	}

	// always attempting to reconnect if an error is encountered,
	// the error is mostly network error.
	select {
	case ps.ReconnectionChan <- err:
	default:
	}
}

// OpenStreamUntilSucceed opens a data stream until it succeeds and returns a clean function.
func OpenStreamUntilSucceed(
	ctx context.Context,
	addr string,
	openFunc func(context.Context, string) (DataStream, func(string), error),
	isFatalError func(error) bool,
	logger *slog.Logger,
) (stream DataStream, cleanFn func(string)) {
	var err error

	for {
		stream, cleanFn, err = openFunc(ctx, addr)
		if err == nil {
			return stream, cleanFn
		}

		if isFatalError(err) {
			return nil, cleanFn
		}

		cleanFn(err.Error())

		logger.Debug("failed to connect. attempting to reconnect", "error", err)

		time.Sleep(time.Second)
	}
}
