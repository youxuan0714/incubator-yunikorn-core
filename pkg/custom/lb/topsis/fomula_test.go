package topsis

import (
	"math"
	"testing"
)

func TestNormalized(t *testing.T) {
	tests := []struct {
		caseName string
		inputs   []float64
		expect   []float64
	}{
		{
			"MIG",
			[]float64{
				20.0,
				35.0,
				22.0,
			},
			[]float64{
				0.43,
				0.76,
				0.47,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			got := Normalized(tt.inputs)
			for index, expect := range tt.expect {
				if tmp := got[index]; int64(tmp*100) != int64(expect*100) {
					t.Errorf("normalize expect %v, got %v", expect, tmp)
				}
			}
		})
	}
}

func TestWeight(t *testing.T) {
	tests := []struct {
		caseName string
		inputs   []float64
		expect   []float64
	}{
		{
			"MIG",
			[]float64{
				0.43,
				0.76,
				0.47,
			},
			[]float64{
				0.215,
				0.38,
				0.235,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			got := Weight(tt.inputs)
			for index, expect := range tt.expect {
				if tmp := got[index]; tmp != expect {
					t.Errorf("normalize expect %v, got %v", expect, tmp)
				}
			}
		})
	}
}

func TestAPlus(t *testing.T) {
	tests := []struct {
		caseName string
		inputs   []float64
		expect   float64
	}{
		{
			"MIG",
			[]float64{
				0.43,
				0.76,
				0.47,
			},
			0.43,
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			if got := APlus(tt.inputs); tt.expect != got {
				t.Errorf("A+ expect %v, got %v", tt.expect, got)
			}
		})
	}
}

func TestAMinus(t *testing.T) {
	tests := []struct {
		caseName string
		inputs   []float64
		expect   float64
	}{
		{
			"MIG",
			[]float64{
				0.43,
				0.76,
				0.47,
			},
			0.76,
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			if got := AMinus(tt.inputs); tt.expect != got {
				t.Errorf("A+ expect %v, got %v", tt.expect, got)
			}
		})
	}
}

func TestSM(t *testing.T) {
	tests := []struct {
		caseName string
		inputs   [][]float64
		AObject  []float64
		expect   []int64
	}{
		{
			"standard",
			[][]float64{
				[]float64{0.012, 0.15, 0.13},
				[]float64{0.66, 0.43, 0.47},
			},
			[]float64{
				0.012,
				0.43,
			},
			[]int64{
				int64(math.Sqrt(math.Pow(0.012-0.012, float64(2))+math.Pow(0.66-0.43, float64(2))) * 100),
				int64(math.Sqrt(math.Pow(0.15-0.012, float64(2))+math.Pow(0.43-0.43, float64(2))) * 100),
				int64(math.Sqrt(math.Pow(0.13-0.012, float64(2))+math.Pow(0.47-0.43, float64(2))) * 100),
			},
		},
		{
			"standard",
			[][]float64{
				[]float64{0.012, 0.15, 0.13},
				[]float64{0.66, 0.43, 0.47},
			},
			[]float64{
				0.15,
				0.66,
			},
			[]int64{
				int64(math.Sqrt(math.Pow(0.012-0.15, float64(2))+math.Pow(0.66-0.66, float64(2))) * 100),
				int64(math.Sqrt(math.Pow(0.15-0.15, float64(2))+math.Pow(0.43-0.66, float64(2))) * 100),
				int64(math.Sqrt(math.Pow(0.13-0.15, float64(2))+math.Pow(0.47-0.66, float64(2))) * 100),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			got := SM(tt.inputs, tt.AObject)
			if len(got) != len(tt.expect) {
				t.Errorf("length expect %v, got %v", len(tt.expect), len(got))
			} else {
				for index, SMValue := range got {
					if expect := tt.expect[index]; expect != int64(SMValue*100) {
						t.Errorf("SM %v expect %v, got %v", index, expect, SMValue)
					}
				}
			}
		})
	}
}

func TestIndexOfMaxRC(t *testing.T) {
	tests := []struct {
		caseName string
		SMPlus   []float64
		SMMinus  []float64
		expect   int
	}{
		{
			"standard",
			[]float64{0.23, 0.13, 0.12},
			[]float64{0.13, 0.23, 0.19},
			1, //[]int64{13/36=0.36, 23/63=0.63, 19/31=0.61},
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			if got, debug := IndexOfMaxRC(tt.SMPlus, tt.SMMinus); got != tt.expect {
				t.Errorf("RC %v: index expect %v, got %v", debug, tt.expect, got)
			}
		})
	}
}
