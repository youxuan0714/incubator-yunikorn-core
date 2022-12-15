package topsis

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"math"
)

func Normalized(q []resources.Quantity) []resources.Quantity {
	result := make([]resources.Quantity, 0)
	sum := float64(0)
	for _, element := range q {
		sum += math.Pow(float64(element), float64(2))
	}
	base := math.Sqrt(sum)
	for _, element := range q {
		tmp := float64(int64(element)) / base
		result = append(result, resources.Quantity(tmp))
	}
	return result
}

func Weight(objectNames []string, migs, deviations []resources.Quantity, w float64) []*resources.Resource {
	result := make([]*resources.Resource, 0)
	number := len(migs)
	for i := 0; i < number; i++ {
		tmp := resources.NewResource()
		mig := float64(int64(migs[i])) / w
		deviation := float64(int64(deviations[i])) / w
		tmp.Resources[objectNames[0]] = resources.Quantity(mig)
		tmp.Resources[objectNames[1]] = resources.Quantity(deviation)
		result = append(result, tmp)
	}
	return result
}

func APlus(objectNames []string, q []*resources.Resource) *resources.Resource {
	min := q[0].Clone()
	for _, element := range q {
		for _, object := range objectNames {
			if min.Resources[object] > element.Resources[object] {
				min.Resources[object] = element.Resources[object]
			}
		}
	}
	return min
}

func AMinus(objectNames []string, q []*resources.Resource) *resources.Resource {
	max := q[0].Clone()
	for _, element := range q {
		for _, object := range objectNames {
			if max.Resources[object] > element.Resources[object] {
				max.Resources[object] = element.Resources[object]
			}
		}
	}
	return max
}

func SM(objectNames []string, weighted []*resources.Resource, AObjects *resources.Resource) []float64 {
	result := make([]float64, 0)
	for _, element := range weighted {
		sum := float64(0)
		for _, object := range objectNames {
			tmp := element.Resources[object] - AObjects.Resources[object]
			power := math.Pow(float64(tmp), float64(2))
			sum += power
		}
		sum = math.Sqrt(sum)
		result = append(result, sum)
	}
	return result
}

func IndexOfMaxRC(SMPlus, SMMinus []float64) int {
	number := len(SMPlus)
	index := 0
	max := SMMinus[index] / (SMPlus[index] + SMMinus[index])
	for i := 0; i < number; i++ {
		tmp := SMMinus[i] / (SMPlus[i] + SMMinus[i])
		if max < tmp {
			max = tmp
			index = i
		}
	}
	return index
}
