package tracer

import (
	"encoding/hex"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	ptypes "github.com/ethereum/go-ethereum/debank/types"
)

type callFrame struct {
	Type         vm.OpCode
	From         common.Address
	To           common.Address
	Input        []byte
	Gas          uint64
	Value        *big.Int
	Output       []byte
	Error        string
	GasUsed      uint64
	Calls        []*callFrame
	TraceAddress []uint64
}

type callTracer struct {
	env      *vm.EVM
	callstack []*callFrame
	tx       *types.Transaction
	from     common.Address
	events   []ptypes.Event
	traces   []ptypes.Trace
}

func newCallTracerRaw() *callTracer {
	return &callTracer{
		callstack: make([]*callFrame, 0),
		events:    make([]ptypes.Event, 0),
		traces:    make([]ptypes.Trace, 0),
	}
}

func (t *callTracer) OnTxStart(tx *types.Transaction, from common.Address) {
	t.tx = tx
	t.from = from
	t.callstack = make([]*callFrame, 0)
	t.events = make([]ptypes.Event, 0)
	t.traces = make([]ptypes.Trace, 0)
}

func (t *callTracer) OnTxEnd(receipt *types.Receipt, err error) {
	// Collect traces from callstack
	if len(t.callstack) > 0 {
		t.collectTraces(t.callstack[0], []uint64{}, receipt.TransactionIndex)
	}

	// Add events and traces to block context
	BlockCtx.BlockFile.Events = append(BlockCtx.BlockFile.Events, t.events...)
	BlockCtx.BlockFile.Traces = append(BlockCtx.BlockFile.Traces, t.traces...)
}

func (t *callTracer) collectTraces(frame *callFrame, traceAddr []uint64, txIndex uint) {
	trace := ptypes.Trace{
		Type:         frame.Type.String(),
		From:         strings.ToLower(frame.From.Hex()),
		To:           strings.ToLower(frame.To.Hex()),
		Value:        frame.Value,
		Gas:          frame.Gas,
		GasUsed:      frame.GasUsed,
		Input:        "0x" + hex.EncodeToString(frame.Input),
		Output:       "0x" + hex.EncodeToString(frame.Output),
		Error:        frame.Error,
		TxIndex:      uint64(txIndex),
		TraceAddress: traceAddr,
	}
	t.traces = append(t.traces, trace)

	for i, child := range frame.Calls {
		childAddr := append([]uint64{}, traceAddr...)
		childAddr = append(childAddr, uint64(i))
		t.collectTraces(child, childAddr, txIndex)
	}
}

func (t *callTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
	t.env = env
	typ := vm.CALL
	if create {
		typ = vm.CREATE
	}
	frame := &callFrame{
		Type:  typ,
		From:  from,
		To:    to,
		Input: input,
		Gas:   gas,
		Value: value,
	}
	t.callstack = append(t.callstack, frame)
}

func (t *callTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if len(t.callstack) > 0 {
		t.callstack[len(t.callstack)-1].Output = output
		t.callstack[len(t.callstack)-1].GasUsed = gasUsed
		if err != nil {
			t.callstack[len(t.callstack)-1].Error = err.Error()
		}
	}
}

func (t *callTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	frame := &callFrame{
		Type:  typ,
		From:  from,
		To:    to,
		Input: input,
		Gas:   gas,
		Value: value,
	}
	if len(t.callstack) > 0 {
		parent := t.callstack[len(t.callstack)-1]
		parent.Calls = append(parent.Calls, frame)
	}
	t.callstack = append(t.callstack, frame)
}

func (t *callTracer) CaptureExit(output []byte, gasUsed uint64, err error) {
	if len(t.callstack) > 0 {
		t.callstack[len(t.callstack)-1].Output = output
		t.callstack[len(t.callstack)-1].GasUsed = gasUsed
		if err != nil {
			t.callstack[len(t.callstack)-1].Error = err.Error()
		}
		// Pop from stack but keep in parent's Calls
		if len(t.callstack) > 1 {
			t.callstack = t.callstack[:len(t.callstack)-1]
		}
	}
}

func (t *callTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	// Track storage changes for contracts
	if op == vm.SSTORE {
		stack := scope.Stack
		if len(stack.Data()) >= 2 {
			addr := scope.Contract.Address()
			BlockCtx.ChangeContracts[addr] = struct{}{}
		}
	}
}

func (t *callTracer) CaptureStateAfter(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}

func (t *callTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

func (t *callTracer) OnLog(log *types.Log) {
	topics := make([]string, len(log.Topics))
	for i, topic := range log.Topics {
		topics[i] = topic.Hex()
	}
	event := ptypes.Event{
		Address:     strings.ToLower(log.Address.Hex()),
		Topics:      topics,
		Data:        "0x" + hex.EncodeToString(log.Data),
		TxIndex:     uint64(log.TxIndex),
		LogIndex:    uint64(log.Index),
		BlockNumber: log.BlockNumber,
	}
	t.events = append(t.events, event)
}
