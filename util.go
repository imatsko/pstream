package pstream

import (
	_ "fmt"
	"math"
	"math/rand"
	"time"
)

type Selectable interface {
	Measure() float64
}

func SelectRandomItemProportionally(a []Selectable) Selectable {
	return SelectRandomProportionally(a, 1)[0]
}

func SelectRandomProportionally(a []Selectable, count int) []Selectable {
	if count == 0 {
		return make([]Selectable, 0)
	}

	list := make([]Selectable, len(a))
	copy(list, a)

	var sum float64

	measures := make([]float64, len(list))

	for i, s := range list {
		measures[i] = s.Measure()
		sum = sum + measures[i]
	}
	//fmt.Printf("sum measure %v \n", sum)

	results := make([]Selectable, 0, count)

	if len(measures) == 0 {
		return results
	}

	for try := 0; float64(try) < math.Min(float64(count), float64(len(measures))); try += 1 {

		r := rand.Intn(int(math.Floor(sum)) + 1)

		var n_sum float64
		//fmt.Printf("measures %v list %v \n", measures, list)
		for i, m := range measures {
			n_sum = n_sum + m
			if n_sum >= float64(r) {
				//fmt.Printf("found  %v measure %v, sum %v, rand %v \n",i, list[i], n_sum, r)
				results = append(results, list[i])
				sum -= measures[i]
				measures = append(measures[:i], measures[i+1:]...)
				list = append(list[:i], list[i+1:]...)
				break
			}
		}
	}
	return results
}

//type prop int
//func (i prop) Measure() float64 {
//	//return math.Sqrt(float64(i))
//	return float64(i)
//}
//func main() {
//
//rand.Seed(time.Now().UTC().UnixNano())
//items := make([]Selectable, 10)
//
//for i, _ := range items {
//items[i] = prop(rand.Intn(50) + 1)
//}
//
//res_counter := make(map[int]int)
//
//fmt.Printf("items %v \n", items)
//for i := 0; i < 30; i = i + 1 {
//
//res := SelectRandomProportionally(items)
//
//v := int(res.(prop))
//p := res_counter[v]
//res_counter[v] = p + 1
//
//fmt.Printf("rand res %v \n", res)
//}
//
//fmt.Println(res_counter)
//
//}

type Semaphore struct {
	c   chan interface{}
	len int
}

func NewSemaphore(l int) *Semaphore {
	s := new(Semaphore)
	s.c = make(chan interface{}, l)
	s.len = l

	return s
}

func (s *Semaphore) TryAcquireOne() bool {
	select {
	case s.c <- nil:
		return true
	default:
		return false
	}
}

func (s *Semaphore) Acquire(n int) {
	for i := 0; i < n; i += 1 {
		s.c <- nil
	}
}

func (s *Semaphore) Release(n int) {
	for i := 0; i < n; i += 1 {
		<-s.c
	}
}

func SinceDayStart() time.Duration {
	t := time.Now()

	year, month, day := t.Date()

	day_start := time.Date(year, month, day, 0,0,0,0, t.Location())

	return t.Sub(day_start)
}