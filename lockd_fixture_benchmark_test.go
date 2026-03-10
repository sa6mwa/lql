package lql

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

func BenchmarkLockdFixtures(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping lockd fixture benchmark in short mode")
	}
	datasets, err := loadQueryRealworldDatasets()
	if err != nil {
		b.Skipf("skip lockd fixture benchmark: %v", err)
	}

	selector, err := ParseSelectorString(`/event="session_sync"`)
	if err != nil {
		b.Fatalf("parse selector: %v", err)
	}
	queryPlan, err := NewQueryStreamPlan(selector)
	if err != nil {
		b.Fatalf("query plan: %v", err)
	}
	muts, err := ParseMutations([]string{`/processed=true`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		b.Fatalf("parse mutate plan: %v", err)
	}
	mutatePlan, err := NewMutateStreamPlan(muts)
	if err != nil {
		b.Fatalf("new mutate plan: %v", err)
	}

	for _, ds := range datasets {
		ds := ds
		b.Run(ds.name, func(b *testing.B) {
			b.SetBytes(int64(len(ds.payload)))

			b.Run("query_only/decision_plan", func(b *testing.B) {
				reader := bytes.NewReader(nil)
				runBenchmarkModes(b, func() error {
					reader.Reset(ds.payload)
					_, err := QueryStreamWithResult(QueryStreamRequest{
						Reader: reader,
						Plan:   queryPlan,
						Mode:   QueryDecisionOnly,
						OnDecision: func(QueryStreamDecision) error {
							return nil
						},
					})
					return err
				})
			})

			b.Run("query_to_mutate_handoff/reusable_sink", func(b *testing.B) {
				reader := bytes.NewReader(nil)
				sinkFactory := NewReusableQueryPayloadSinkFactory(ReusableQueryPayloadSinkFactoryOptions{
					SpoolMemoryBytes: 8 * 1024,
				})
				defer func() {
					if err := sinkFactory.Close(); err != nil {
						b.Fatalf("close reusable sink factory: %v", err)
					}
				}()

				runBenchmarkModes(b, func() error {
					reader.Reset(ds.payload)
					pipeR, pipeW := io.Pipe()
					type queryOutcome struct {
						result QueryStreamResult
						err    error
					}
					queryDone := make(chan queryOutcome, 1)
					go func() {
						res, err := QueryStreamWithResult(QueryStreamRequest{
							Reader:               reader,
							Plan:                 queryPlan,
							Mode:                 QueryDecisionPlusValue,
							MatchedOnly:          true,
							DisableInternalSpool: true,
							PayloadSinkFactory:   sinkFactory.Factory(),
							OnValue: func(v QueryStreamValue) error {
								rc, err := v.OpenJSON()
								if err != nil {
									return err
								}
								defer rc.Close()
								if _, err := io.Copy(pipeW, rc); err != nil {
									return err
								}
								_, err = pipeW.Write(jsonNewline)
								return err
							},
						})
						if err != nil {
							_ = pipeW.CloseWithError(err)
						} else {
							_ = pipeW.Close()
						}
						queryDone <- queryOutcome{result: res, err: err}
					}()

					mutateResult, mutateErr := MutateStreamWithResult(MutateStreamRequest{
						Reader: pipeR,
						Writer: io.Discard,
						Plan:   mutatePlan,
					})
					query := <-queryDone
					if query.err != nil {
						return query.err
					}
					if mutateErr != nil {
						return mutateErr
					}
					if mutateResult.CandidatesSeen != query.result.CandidatesMatched {
						return errors.New("lockd handoff candidate mismatch")
					}
					b.ReportMetric(float64(query.result.CandidatesMatched), "matched/op")
					b.ReportMetric(float64(query.result.BytesCaptured), "query-captured-bytes/op")
					b.ReportMetric(float64(mutateResult.CandidatesSeen), "mutated-docs/op")
					b.ReportMetric(float64(mutateResult.BytesRead), "mutate-bytes/op")
					if mutateResult.CandidatesSeen > 0 {
						b.ReportMetric(float64(mutateResult.BytesRead)/float64(mutateResult.CandidatesSeen), "mutate-bytes/doc")
					}
					return nil
				})
			})

			b.Run("query_to_mutate_fused/reusable_sink", func(b *testing.B) {
				reader := bytes.NewReader(nil)
				sinkFactory := NewReusableQueryPayloadSinkFactory(ReusableQueryPayloadSinkFactoryOptions{
					SpoolMemoryBytes: 8 * 1024,
				})
				defer func() {
					if err := sinkFactory.Close(); err != nil {
						b.Fatalf("close reusable sink factory: %v", err)
					}
				}()

				runBenchmarkModes(b, func() error {
					reader.Reset(ds.payload)
					result, err := QueryMutateStreamWithResult(QueryMutateStreamRequest{
						Reader:                  reader,
						Writer:                  io.Discard,
						QueryPlan:               queryPlan,
						MutatePlan:              mutatePlan,
						QueryDisableSpool:       true,
						QueryPayloadSinkFactory: sinkFactory.Factory(),
					})
					if err != nil {
						return err
					}
					if result.Mutate.CandidatesSeen != result.Query.CandidatesMatched {
						return errors.New("lockd fused candidate mismatch")
					}
					b.ReportMetric(float64(result.Query.CandidatesMatched), "matched/op")
					b.ReportMetric(float64(result.Query.BytesCaptured), "query-captured-bytes/op")
					b.ReportMetric(float64(result.Mutate.CandidatesSeen), "mutated-docs/op")
					b.ReportMetric(float64(result.Mutate.BytesRead), "mutate-bytes/op")
					if result.Mutate.CandidatesSeen > 0 {
						b.ReportMetric(float64(result.Mutate.BytesRead)/float64(result.Mutate.CandidatesSeen), "mutate-bytes/doc")
					}
					return nil
				})
			})
		})
	}
}
