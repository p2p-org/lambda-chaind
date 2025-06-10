// Copyright © 2025 Weald Technology Trading.
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

package postgresql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// ConsolidationRequests provides consolidation requests according to the filter.
func (s *Service) ConsolidationRequests(ctx context.Context, filter *chaindb.ConsolidationRequestFilter) ([]*chaindb.ConsolidationRequest, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ConsolidationRequests")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		defer s.CommitROTx(ctx)
		tx = s.tx(ctx)
	}

	// Build the query.
	queryBuilder := strings.Builder{}
	queryVals := make([]any, 0)

	queryBuilder.WriteString(`
SELECT f_block_root
      ,f_slot
      ,f_index
      ,f_source_address
      ,f_source_pubkey
      ,f_target_pubkey
FROM t_block_consolidation_requests`)

	conditions := make([]string, 0)

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		conditions = append(conditions, fmt.Sprintf("f_slot >= $%d", len(queryVals)))
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		conditions = append(conditions, fmt.Sprintf("f_slot <= $%d", len(queryVals)))
	}

	if len(filter.SourcePubkeys) > 0 {
		sourcePubkeysBytes := make([][]byte, len(filter.SourcePubkeys))
		for i, pubkey := range filter.SourcePubkeys {
			sourcePubkeysBytes[i] = pubkey[:]
		}
		queryVals = append(queryVals, sourcePubkeysBytes)
		conditions = append(conditions, fmt.Sprintf("f_source_pubkey = ANY($%d)", len(queryVals)))
	}

	if len(filter.TargetPubkeys) > 0 {
		targetPubkeysBytes := make([][]byte, len(filter.TargetPubkeys))
		for i, pubkey := range filter.TargetPubkeys {
			targetPubkeysBytes[i] = pubkey[:]
		}
		queryVals = append(queryVals, targetPubkeysBytes)
		conditions = append(conditions, fmt.Sprintf("f_target_pubkey = ANY($%d)", len(queryVals)))
	}

	if len(filter.BlockRoots) > 0 {
		queryVals = append(queryVals, filter.BlockRoots)
		queryBuilder.WriteString(fmt.Sprintf("f_block_root = ANY($%d)", len(queryVals)))
	}

	if len(conditions) > 0 {
		queryBuilder.WriteString("\nWHERE ")
		queryBuilder.WriteString(strings.Join(conditions, " AND "))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_slot, f_index`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_slot DESC, f_index DESC`)
	default:
		return nil, errors.New("no order specified")
	}

	if filter.Limit > 0 {
		queryVals = append(queryVals, filter.Limit)
		queryBuilder.WriteString(fmt.Sprintf(`
LIMIT $%d`, len(queryVals)))
	}

	if e := log.Trace(); e.Enabled() {
		params := make([]string, len(queryVals))
		for i := range queryVals {
			params[i] = fmt.Sprintf("%v", queryVals[i])
		}
		e.Str("query", strings.ReplaceAll(queryBuilder.String(), "\n", " ")).Strs("params", params).Msg("SQL query")
	}

	rows, err := tx.Query(ctx,
		queryBuilder.String(),
		queryVals...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	requests := make([]*chaindb.ConsolidationRequest, 0)
	for rows.Next() {
		request := &chaindb.ConsolidationRequest{}
		var blockRoot []byte
		var sourceAddress []byte
		var sourcePubkey []byte
		var targetPubkey []byte
		err := rows.Scan(
			&blockRoot,
			&request.InclusionSlot,
			&request.InclusionIndex,
			&sourceAddress,
			&sourcePubkey,
			&targetPubkey,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(request.InclusionBlockRoot[:], blockRoot)
		copy(request.SourceAddress[:], sourceAddress)
		copy(request.SourcePubkey[:], sourcePubkey)
		copy(request.TargetPubkey[:], targetPubkey)
		requests = append(requests, request)
	}

	// Always return order of slot then index.
	sort.Slice(requests, func(i int, j int) bool {
		if requests[i].InclusionSlot != requests[j].InclusionSlot {
			return requests[i].InclusionSlot < requests[j].InclusionSlot
		}
		return requests[i].InclusionIndex < requests[j].InclusionIndex
	})

	return requests, nil
}
