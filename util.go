package pstream

import (
	_ "fmt"
	"math"
	"math/rand"
	_ "time"
)

type Selectable interface {
	Measure() float64
}

func SelectRandomProportionally(a []Selectable) Selectable {
	var sum float64

	measures := make([]float64, len(a))

	for i, s := range a {
		measures[i] = s.Measure()
		sum = sum + measures[i]
	}
	//fmt.Printf("sum measure %v \n", sum)

	r := rand.Intn(int(math.Floor(sum)) + 1)

	var n_sum float64
	for i, m := range measures {
		n_sum = n_sum + m
		if n_sum >= float64(r) {
			//fmt.Printf("found measure %v, sum %v, rand %v \n", a[i], n_sum, r)
			return a[i]
		}
	}
	return a[len(a)-1]

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
