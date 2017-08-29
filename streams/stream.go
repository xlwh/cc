package streams

import (
	"sync"
)

// 如回调函数返回false，则删除注册的回调
//回调函数定义
type HandlerFunc func(data interface{}) bool

// 需要有个结构，不能只用一个回调函数，因为func不允许比较
type Handler struct {
	handle HandlerFunc //回调函数
	quitCh chan bool   //回调函数是否执行完毕的状态位
}

type Stream struct {
	Name     string           //Stream名字
	C        chan interface{} //chan
	MaxLen   int              //最大长度
	mutex    *sync.Mutex      //Stream锁
	handlers []*Handler       //回调函数数组
}

//创建Stream
func NewStream(name string, maxlen int) *Stream {
	stream := &Stream{
		Name:     name,
		MaxLen:   maxlen,
		C:        make(chan interface{}, maxlen), //对channel的长度进行限制
		handlers: []*Handler{},
		mutex:    &sync.Mutex{},
	}
	return stream
}

//发布数据
//如果超过最大的channel长度限制，则不往channel中写入数据了
func (s *Stream) Pub(data interface{}) {
	if len(s.C) < s.MaxLen {
		//往channel中写入数据
		s.C <- data
	}
}

//数据订阅：注册回调函数
func (s *Stream) Sub(fun HandlerFunc) <-chan bool {
	//订阅数据需要加锁
	s.mutex.Lock()
	defer s.mutex.Unlock()

	h := &Handler{handle: fun, quitCh: make(chan bool)}

	//把回调函数添加到数组中去
	s.handlers = append(s.handlers, h)

	// 结束信号
	return h.quitCh
}

//启动Stream
func (s *Stream) Run() {
	//处理channel中的数据
	//处理完成就从handler队列中删除
	for {
		data := <-s.C

		s.mutex.Lock()
		for _, handler := range s.handlers {
			if !handler.handle(data) {
				s.removeHandlerFunc(handler)
			}
		}
		s.mutex.Unlock()
	}
}

func (s *Stream) removeHandlerFunc(handler *Handler) {
	handlers := []*Handler{}
	for _, h := range s.handlers {
		if h != handler {
			handlers = append(handlers, h)
		}
	}
	handler.quitCh <- true
	s.handlers = handlers
}
