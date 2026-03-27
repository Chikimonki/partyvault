package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

type Party struct {
	ID            string  `json:"id"`
	LegalName     string  `json:"legal_name"`
	Country       string  `json:"country"`
	LEI           string  `json:"lei"`
	EntityType    string  `json:"entity_type"`
	Status        string  `json:"status"`
	Fingerprint   string  `json:"fingerprint"`
	KYCLevel      string  `json:"kyc_level"`
	RiskScore     int     `json:"risk_score"`
	RiskClass     string  `json:"risk_class"`
	Trust         float64 `json:"trust"`
}

var (
	parties      map[string]Party
	qualityReport map[string]interface{}
	rdb          *redis.Client
	ctx          context.Context
)

func loadParties() error {
	f, err := os.Open("/app/output/cleansed_parties.csv")
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return err
	}
	if len(records) < 2 {
		return fmt.Errorf("no data")
	}
	headers := records[0]
	parties = make(map[string]Party)
	for _, row := range records[1:] {
		record := make(map[string]string)
		for i, val := range row {
			record[headers[i]] = val
		}
		id := record["id"]
		parties[id] = Party{
			ID:         id,
			LegalName:  record["legal_name"],
			Country:    record["country"],
			LEI:        record["lei"],
			EntityType: record["entity_type"],
			Status:     record["status"],
		}
	}
	return nil
}

func loadFingerprintsAndKYC() error {
	f, err := os.Open("/app/output/fingerprinted_parties.txt")
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comma = '|'
	r.Comment = '#'
	records, err := r.ReadAll()
	if err != nil {
		return err
	}
	for _, rec := range records {
		if len(rec) < 10 || rec[0] != "IDENTITY" {
			continue
		}
		id := rec[1]
		p, ok := parties[id]
		if !ok {
			continue
		}
		p.Fingerprint = rec[4]
		trustField := rec[9]
		if idx := strings.Index(trustField, "="); idx != -1 {
			trustVal, _ := strconv.ParseFloat(trustField[idx+1:], 64)
			p.Trust = trustVal
		}
		parties[id] = p
	}
	f2, err := os.Open("/app/output/classified_parties.txt")
	if err != nil {
		return err
	}
	defer f2.Close()
	r2 := csv.NewReader(f2)
	r2.Comma = '|'
	r2.Comment = '#'
	recs, err := r2.ReadAll()
	if err != nil {
		return err
	}
	for _, rec := range recs {
		if len(rec) < 8 {
			continue
		}
		id := rec[0]
		p, ok := parties[id]
		if !ok {
			continue
		}
		p.KYCLevel = rec[4]
		riskScore, _ := strconv.Atoi(rec[6])
		p.RiskScore = riskScore
		p.RiskClass = rec[7]
		parties[id] = p
	}
	return nil
}

func loadQualityReport() error {
	f, err := os.Open("/app/output/quality_report.json")
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(&qualityReport)
}

func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func partiesHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]Party, 0, len(parties))
	for _, p := range parties {
		list = append(list, p)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

func partyHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/party/")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	p, ok := parties[id]
	if !ok {
		http.Error(w, "party not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(p)
}

func qualityHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(qualityReport)
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing name query", http.StatusBadRequest)
		return
	}
	var matches []Party
	for _, p := range parties {
		if strings.Contains(strings.ToLower(p.LegalName), strings.ToLower(name)) {
			matches = append(matches, p)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(matches)
}

func main() {
	if err := loadParties(); err != nil {
		log.Fatalf("Failed to load parties: %v", err)
	}
	if err := loadFingerprintsAndKYC(); err != nil {
		log.Printf("Warning: could not load fingerprints/KYC: %v", err)
	}
	if err := loadQualityReport(); err != nil {
		log.Printf("Warning: could not load quality report: %v", err)
	}
	log.Println("Data loaded successfully")

	// Connect to Redis
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	ctx = context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Redis not reachable: %v", err)
	} else {
		log.Println("Redis connected")
		// Push all parties to a stream
		for _, p := range parties {
			_, err := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "parties",
				Values: map[string]interface{}{
					"id":          p.ID,
					"name":        p.LegalName,
					"country":     p.Country,
					"lei":         p.LEI,
					"entity_type": p.EntityType,
					"status":      p.Status,
					"fingerprint": p.Fingerprint,
					"kyc_level":   p.KYCLevel,
					"risk_score":  p.RiskScore,
					"risk_class":  p.RiskClass,
					"trust":       p.Trust,
				},
			}).Result()
			if err != nil {
				log.Printf("Failed to push party %s to Redis: %v", p.ID, err)
			} else {
				log.Printf("Pushed party %s to Redis stream", p.ID)
			}
		}
	}

	http.HandleFunc("/health", enableCORS(healthHandler))
	http.HandleFunc("/parties", enableCORS(partiesHandler))
	http.HandleFunc("/party/", enableCORS(partyHandler))
	http.HandleFunc("/quality", enableCORS(qualityHandler))
	http.HandleFunc("/search", enableCORS(searchHandler))

	log.Println("Server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
