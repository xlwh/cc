package fsm

import (
	"errors"
	"fmt"
)

var (
	ErrEmptyStateModel     = errors.New("fsm: empty state model")
	ErrEmptyStateTrasition = errors.New("fsm: empty state transition")
	ErrStateNotExist       = errors.New("fsm: state not exist")
)

//状态
type State struct {
	Name    string                //状态名
	OnEnter func(ctx interface{}) //事件进入回调
	OnLeave func(ctx interface{}) //状态结束回调
}

//状态变换
type Transition struct {
	From       string
	To         string
	Input      Input                      //输入五元组:
	Priority   int                        //优先级
	Constraint func(ctx interface{}) bool //约束函数
	Apply      func(ctx interface{})      //某个处理函数
}

//判断Input是否等价，ANY为通配
type Input interface {
	Eq(Input) bool
}

/// StateModel

type StateModel struct {
	States     map[string]*State        //状态集合
	TransTable map[string][]*Transition //变换集合
}

//创建一个空的StateModel
func NewStateModel() *StateModel {
	m := &StateModel{
		States:     map[string]*State{},
		TransTable: map[string][]*Transition{},
	}
	return m
}

//新增一个状态
func (m *StateModel) AddState(s *State) {
	//状态名字:state
	m.States[s.Name] = s
}

//新增一个状态变换
func (m *StateModel) AddTransition(t *Transition) {
	_, ok := m.States[t.From]
	if !ok {
		panic("no such state")
	}

	ts, ok := m.TransTable[t.From]
	if !ok {
		m.TransTable[t.From] = []*Transition{t}
		return
	}

	// sort by priority, e.g. 1 > 0
	pos := len(ts)
	for i, u := range ts {
		if t.Priority > u.Priority {
			pos = i
			break
		}
	}
	// insert
	m.TransTable[t.From] = append(ts[:pos], append([]*Transition{t}, ts[pos:]...)...)
}

func (m *StateModel) DumpTransitions() {
	for state, transArray := range m.TransTable {
		fmt.Printf("%s(%d):\n", state, len(transArray))
		for _, t := range transArray {
			fmt.Printf("  |____%v____> %s\n", t.Input, t.To)
		}
	}
}

/// StateMachine

type StateMachine struct {
	model   *StateModel
	current string
}

func NewStateMachine(initalState string, model *StateModel) *StateMachine {
	m := &StateMachine{
		model:   model,
		current: initalState,
	}

	return m
}

//返回当前的状态
func (m *StateMachine) CurrentState() string {
	return m.current
}

//提前
func (m *StateMachine) Advance(ctx interface{}, input Input) (string, error) {
	model := m.model
	if model == nil {
		return "", ErrEmptyStateModel
	}

	//当前状态
	curr := m.CurrentState()
	_, ok := m.model.States[curr]
	if !ok {
		return "", ErrStateNotExist
	}

	//状态是否允许变换
	ts, ok := m.model.TransTable[curr]
	if !ok {
		return "", ErrEmptyStateTrasition
	}

	// 按状态转换函数的优先级，顺序检查是否可进行状态变换
	for _, t := range ts {
		if t.Input.Eq(input) && (t.Constraint == nil || t.Constraint(ctx)) {
			// 状态转换
			m.model.States[t.From].OnLeave(ctx)
			m.current = t.To
			m.model.States[t.To].OnEnter(ctx)

			if t.Apply != nil {
				t.Apply(ctx)
			}
			return m.CurrentState(), nil
		}
	}

	return m.CurrentState(), nil
}
