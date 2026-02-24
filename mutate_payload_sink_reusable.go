package lql

// ReusableMutatePayloadSinkFactoryOptions configures reusable caller-managed
// mutate payload sinks.
type ReusableMutatePayloadSinkFactoryOptions = ReusableQueryPayloadSinkFactoryOptions

// ReusableMutatePayloadSinkFactory reuses in-memory buffers and spill files
// across MutateStream callback payloads.
type ReusableMutatePayloadSinkFactory struct {
	queryFactory *ReusableQueryPayloadSinkFactory
}

// NewReusableMutatePayloadSinkFactory builds a reusable caller-managed sink
// factory for MutateStreamRequest.PayloadSinkFactory.
func NewReusableMutatePayloadSinkFactory(opts ReusableMutatePayloadSinkFactoryOptions) *ReusableMutatePayloadSinkFactory {
	return &ReusableMutatePayloadSinkFactory{
		queryFactory: NewReusableQueryPayloadSinkFactory(ReusableQueryPayloadSinkFactoryOptions(opts)),
	}
}

// Factory returns a MutateStream payload sink factory function.
func (f *ReusableMutatePayloadSinkFactory) Factory() MutateStreamPayloadSinkFactory {
	return f.NewSink
}

// NewSink implements MutateStreamPayloadSinkFactory.
func (f *ReusableMutatePayloadSinkFactory) NewSink(req MutateStreamPayloadSinkRequest) (MutateStreamPayloadSink, error) {
	return f.queryFactory.NewSink(QueryStreamPayloadSinkRequest(req))
}

// Close releases all reusable sink resources including any temp files.
func (f *ReusableMutatePayloadSinkFactory) Close() error {
	return f.queryFactory.Close()
}
