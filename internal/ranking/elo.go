package ranking

import (
	"math"

	"tethys/internal/db"
)

func ComputeBradleyTerryElos(rows []db.PairResult, topElo float64) map[int64]float64 {
	index := make(map[string]int)
	ids := make([]int64, 0)
	for _, row := range rows {
		if _, ok := index[row.EngineA]; !ok {
			index[row.EngineA] = len(index)
			ids = append(ids, row.EngineAID)
		}
		if _, ok := index[row.EngineB]; !ok {
			index[row.EngineB] = len(index)
			ids = append(ids, row.EngineBID)
		}
	}
	if len(index) == 0 {
		return map[int64]float64{}
	}

	n := len(index)
	games := make([][]float64, n)
	wins := make([][]float64, n)
	for i := 0; i < n; i++ {
		games[i] = make([]float64, n)
		wins[i] = make([]float64, n)
	}
	for _, row := range rows {
		i := index[row.EngineA]
		j := index[row.EngineB]
		if i == j {
			continue
		}
		wA := float64(row.WinsA) + 0.5*float64(row.Draws)
		wB := float64(row.WinsB) + 0.5*float64(row.Draws)
		nij := float64(row.WinsA + row.WinsB + row.Draws)
		games[i][j] += nij
		games[j][i] += nij
		wins[i][j] += wA
		wins[j][i] += wB
	}

	strength := make([]float64, n)
	for i := range strength {
		strength[i] = 1.0
	}
	for iter := 0; iter < 200; iter++ {
		maxDelta := 0.0
		for i := 0; i < n; i++ {
			wi := 0.0
			for j := 0; j < n; j++ {
				wi += wins[i][j]
			}
			if wi == 0 {
				strength[i] = 0.0
				continue
			}
			denom := 0.0
			for j := 0; j < n; j++ {
				if i == j {
					continue
				}
				if games[i][j] == 0 {
					continue
				}
				sum := strength[i] + strength[j]
				if sum <= 0 {
					sum = 1
				}
				denom += games[i][j] / sum
			}
			if denom == 0 {
				continue
			}
			newStrength := wi / denom
			delta := math.Abs(newStrength - strength[i])
			if delta > maxDelta {
				maxDelta = delta
			}
			strength[i] = newStrength
		}
		if maxDelta < 1e-6 {
			break
		}
	}

	maxStrength := 0.0
	for _, s := range strength {
		if s > maxStrength {
			maxStrength = s
		}
	}
	if maxStrength == 0 {
		maxStrength = 1
	}
	minStrength := maxStrength * 1e-6
	if minStrength <= 0 {
		minStrength = 1e-6
	}

	elos := make(map[int64]float64, n)
	for i := 0; i < n; i++ {
		id := ids[i]
		if id == 0 {
			continue
		}
		totalGames := 0.0
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			totalGames += games[i][j]
		}
		if totalGames == 0 {
			continue
		}
		s := strength[i]
		if s < minStrength {
			s = minStrength
		}
		elos[id] = topElo + 400*math.Log10(s/maxStrength)
	}
	return elos
}
