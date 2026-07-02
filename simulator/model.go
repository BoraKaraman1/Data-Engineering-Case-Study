package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Wire format. Nested JSON inspired by the OCPI/OCPP-style schema in the brief.
// The simulator emits this NESTED shape to the raw topic (realistic). The Phase-2
// processor flattens it for the analytics store. Optional sub-objects are pointers
// so they serialise as absent when not relevant to an event type.
// ---------------------------------------------------------------------------

type Location struct {
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	City    string  `json:"city"`
	Country string  `json:"country"`
}

type Meter struct {
	PowerKW    float64 `json:"power_kw"`
	EnergyKWh  float64 `json:"energy_kwh"` // CUMULATIVE within the session (like a meter register)
	VoltageV   float64 `json:"voltage_v"`
	CurrentA   float64 `json:"current_a"`
	SocPercent int     `json:"soc_percent"`
}

type Vehicle struct {
	Brand string `json:"brand"`
	Model string `json:"model"`
	EvID  string `json:"ev_id"`
}

type Fault struct {
	ErrorCode string `json:"error_code"`
	Component string `json:"component"`
}

type Event struct {
	EventID      string   `json:"event_id"`
	EventType    string   `json:"event_type"`
	StationID    string   `json:"station_id"`
	ConnectorID  int      `json:"connector_id"`
	SessionID    string   `json:"session_id,omitempty"`
	Timestamp    string   `json:"timestamp"` // RFC3339 with milliseconds, UTC
	OperatorID   string   `json:"operator_id"`
	Location     Location `json:"location"`
	Meter        *Meter   `json:"meter,omitempty"`
	Vehicle      *Vehicle `json:"vehicle,omitempty"`
	TariffID     string   `json:"tariff_id,omitempty"`
	CostEur      float64  `json:"cost_eur,omitempty"`
	Fault        *Fault   `json:"fault,omitempty"`
	Status       string   `json:"status,omitempty"`         // new connector status on STATUS_CHANGE
	IsPeakPriced bool     `json:"is_peak_priced,omitempty"` // SESSION_STOP billed at the peak multiplier (analytics-only)
}

const (
	TypeSessionStart = "SESSION_START"
	TypeMeterUpdate  = "METER_UPDATE"
	TypeStatusChange = "STATUS_CHANGE"
	TypeSessionStop  = "SESSION_STOP"
	TypeHeartbeat    = "HEARTBEAT"
	TypeFaultAlert   = "FAULT_ALERT"
)

// ---------------------------------------------------------------------------
// Domain model.
// ---------------------------------------------------------------------------

type Connector struct {
	ID          int
	PowerRating float64 // kW ceiling for this connector
	Type        string  // "AC" or "DC"

	// runtime state (single goroutine owns a connector, so no locking needed)
	status       string // Available, Charging, Faulted
	session      *Session
	nextMeterAt  time.Time
	lastMeterAt  time.Time
	idleUntil    time.Time
	faultedUntil time.Time
}

type Station struct {
	ID         string
	OperatorID string
	City       string
	Country    string
	Lat        float64
	Lon        float64
	Connectors []*Connector

	nextHeartbeatAt time.Time // station emits HEARTBEAT on this schedule
}

type Session struct {
	ID            string
	Vehicle       Vehicle
	BatteryKWh    float64
	TariffID      string
	StartedAt     time.Time
	EndsAt        time.Time
	EnergyKWh     float64 // cumulative energy delivered so far
	Soc           float64 // 0..100
	TargetPowerKW float64
}

// vehicleModel couples a model with a realistic usable battery and max accept rate.
type vehicleModel struct {
	Brand       string
	Model       string
	BatteryKWh  float64
	MaxAcceptKW float64
}

var vehicleFleet = []struct {
	m      vehicleModel
	weight int
}{
	{vehicleModel{"Tesla", "Model 3", 60, 250}, 22},
	{vehicleModel{"Tesla", "Model Y", 75, 250}, 20},
	{vehicleModel{"Volvo", "EX30", 64, 153}, 8},
	{vehicleModel{"Volvo", "XC40 Recharge", 78, 150}, 7},
	{vehicleModel{"Volkswagen", "ID.4", 77, 135}, 9},
	{vehicleModel{"Hyundai", "IONIQ 5", 77, 233}, 8},
	{vehicleModel{"Kia", "EV6", 77, 233}, 6},
	{vehicleModel{"Renault", "Megane E-Tech", 60, 130}, 5},
	{vehicleModel{"BMW", "i4", 80, 205}, 5},
	{vehicleModel{"Mercedes-Benz", "EQB", 66, 100}, 4},
	{vehicleModel{"Togg", "T10X", 88, 180}, 6}, // local hero
}

// Tariff is one pricing plan. tariffCatalog is the SINGLE source of truth for tariff
// pricing: registry.go seeds these same rows into the Postgres tariffs table, so the
// registry the processor validates against can't drift from the cost math used here.
type Tariff struct {
	ID       string
	Name     string
	Base     float64 // price per kWh, EUR
	PeakMult float64 // multiplier applied to Base during the peak window
}

var tariffCatalog = []Tariff{
	{"standard-v1", "Standard", 0.39, 1.00},
	{"peak-rate-v2", "Peak Rate", 0.49, 1.35},
	{"off-peak-v1", "Off Peak", 0.29, 0.80},
	{"fleet-v1", "Fleet Contract", 0.34, 1.00},
}

// tariffByID indexes tariffCatalog for O(1) pricing lookups in stopSession.
var tariffByID = indexTariffs(tariffCatalog)

func indexTariffs(catalog []Tariff) map[string]Tariff {
	m := make(map[string]Tariff, len(catalog))
	for _, t := range catalog {
		m[t.ID] = t
	}
	return m
}

var faultCodes = []struct {
	code      string
	component string
}{
	{"GroundFailure", "EVSE"},
	{"OverCurrentFailure", "Connector"},
	{"OverVoltage", "PowerModule"},
	{"HighTemperature", "CoolingSystem"},
	{"ConnectorLockFailure", "Connector"},
	{"CommunicationError", "Controller"},
}

// rng is a small helper bundling a seeded PRNG with the config it needs.
type rng struct {
	r   *rand.Rand
	cfg *Config
}

func newRNG(seed int64, cfg *Config) *rng { return &rng{r: rand.New(rand.NewSource(seed)), cfg: cfg} }

func (g *rng) pickCity() City {
	total := 0
	for _, c := range g.cfg.Cities {
		total += c.Weight
	}
	if total <= 0 {
		return g.cfg.Cities[0]
	}
	n := g.r.Intn(total)
	for _, c := range g.cfg.Cities {
		n -= c.Weight
		if n < 0 {
			return c
		}
	}
	return g.cfg.Cities[0]
}

func (g *rng) pickVehicle() vehicleModel {
	total := 0
	for _, v := range vehicleFleet {
		total += v.weight
	}
	n := g.r.Intn(total)
	for _, v := range vehicleFleet {
		n -= v.weight
		if n < 0 {
			return v.m
		}
	}
	return vehicleFleet[0].m
}

// inPeakWindow reports whether hour falls in any configured peak window. Pricing
// (stopSession) and tariff selection (pickTariff) both consult this so "peak"
// means the SAME thing as the arrival weighting in timeWeight: the spec's peak
// hours sourced from config, not a hardcoded literal.
func inPeakWindow(hour int, cfg *Config) bool {
	for _, w := range cfg.Simulator.PeakWindows {
		if hour >= w.Start && hour < w.End {
			return true
		}
	}
	return false
}

func (g *rng) pickTariff(hour int) string {
	// crude: peak hours skew toward the peak tariff
	if inPeakWindow(hour, g.cfg) {
		if g.r.Float64() < 0.6 {
			return "peak-rate-v2"
		}
	}
	switch g.r.Intn(4) {
	case 0:
		return "off-peak-v1"
	case 1:
		return "fleet-v1"
	default:
		return "standard-v1"
	}
}

// truncNormal draws from a normal distribution clamped to [min, max].
func (g *rng) truncNormal(mean, std, lo, hi float64) float64 {
	for i := 0; i < 8; i++ {
		v := g.r.NormFloat64()*std + mean
		if v >= lo && v <= hi {
			return v
		}
	}
	return math.Max(lo, math.Min(hi, mean))
}

// vinLike returns a fake but plausible 17-char VIN-style id.
func (g *rng) vinLike() string {
	const alphabet = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
	b := make([]byte, 17)
	for i := range b {
		b[i] = alphabet[g.r.Intn(len(alphabet))]
	}
	return string(b)
}

// BuildStations generates the station/connector roster deterministically from cfg.
func BuildStations(cfg *Config, g *rng) []*Station {
	stations := make([]*Station, 0, cfg.Simulator.StationCount)
	perCityCounter := map[string]int{}
	loCon, hiCon := cfg.Simulator.ConnectorsPerStation[0], cfg.Simulator.ConnectorsPerStation[1]
	if loCon < 1 {
		loCon = 1
	}
	if hiCon < loCon {
		hiCon = loCon
	}
	// Operators own stations across all cities. Draw from a dedicated PRNG so the
	// assignment is deterministic without shifting the station-roster stream (ids,
	// coordinates and connector ratings stay identical regardless of operator set).
	operators := cfg.Simulator.Operators
	if len(operators) == 0 {
		operators = []string{"ChargeSquare"}
	}
	opRng := rand.New(rand.NewSource(cfg.Simulator.Seed + 101))
	for i := 0; i < cfg.Simulator.StationCount; i++ {
		c := g.pickCity()
		perCityCounter[c.Code]++
		// %04d pads to 4 digits but never truncates, so IDs stay unique past 9999
		id := fmt.Sprintf("TR-%s-%04d", c.Code, perCityCounter[c.Code])
		// jitter coordinates a little so faults/sessions spread across the city
		lat := c.Lat + (g.r.Float64()-0.5)*0.12
		lon := c.Lon + (g.r.Float64()-0.5)*0.12
		nCon := loCon + g.r.Intn(hiCon-loCon+1)
		st := &Station{
			ID: id, OperatorID: operators[opRng.Intn(len(operators))], City: c.Name, Country: "TR",
			Lat: lat, Lon: lon, Connectors: make([]*Connector, 0, nCon),
		}
		for cid := 1; cid <= nCon; cid++ {
			rating, typ := g.pickConnectorRating()
			st.Connectors = append(st.Connectors, &Connector{
				ID: cid, PowerRating: rating, Type: typ, status: "Available",
			})
		}
		stations = append(stations, st)
	}
	return stations
}

func (g *rng) pickConnectorRating() (float64, string) {
	switch g.r.Intn(10) {
	case 0, 1, 2:
		return 22, "AC"
	case 3, 4, 5:
		return 50, "DC"
	case 6, 7:
		return 150, "DC"
	default:
		return 350, "DC"
	}
}

// ---------------------------------------------------------------------------
// Event construction. All events carry station identity + location; only the
// relevant sub-objects are attached per type.
// ---------------------------------------------------------------------------

func (st *Station) baseEvent(eventType string, conn *Connector, now time.Time) Event {
	return Event{
		EventID:     uuid.NewString(),
		EventType:   eventType,
		StationID:   st.ID,
		ConnectorID: conn.ID,
		Timestamp:   now.UTC().Format("2006-01-02T15:04:05.000Z07:00"),
		OperatorID:  st.OperatorID,
		Location:    Location{Lat: st.Lat, Lon: st.Lon, City: st.City, Country: st.Country},
	}
}

// startSession spins up a charging session on an available connector.
func (st *Station) startSession(conn *Connector, now time.Time, g *rng) Event {
	v := g.pickVehicle()
	hour := now.Hour()
	durMin := g.truncNormal(g.cfg.Simulator.SessionMinutes.Mean, g.cfg.Simulator.SessionMinutes.Stddev,
		g.cfg.Simulator.SessionMinutes.Min, g.cfg.Simulator.SessionMinutes.Max)
	sess := &Session{
		ID:            "sess-" + uuid.NewString()[:18],
		Vehicle:       Vehicle{Brand: v.Brand, Model: v.Model, EvID: g.vinLike()},
		BatteryKWh:    v.BatteryKWh,
		TariffID:      g.pickTariff(hour),
		StartedAt:     now,
		EndsAt:        now.Add(time.Duration(durMin * float64(time.Minute))),
		Soc:           10 + g.r.Float64()*40, // arrive with 10-50% charge
		TargetPowerKW: math.Min(conn.PowerRating, v.MaxAcceptKW),
	}
	conn.session = sess
	conn.status = "Charging"
	conn.lastMeterAt = now
	conn.nextMeterAt = now.Add(g.meterInterval())

	e := st.baseEvent(TypeSessionStart, conn, now)
	e.SessionID = sess.ID
	e.TariffID = sess.TariffID
	e.Vehicle = &Vehicle{Brand: v.Brand, Model: v.Model, EvID: sess.Vehicle.EvID}
	e.Meter = &Meter{PowerKW: 0, EnergyKWh: 0, VoltageV: nominalVoltage(conn), CurrentA: 0, SocPercent: int(sess.Soc)}
	return e
}

// meterTick advances an active session by one reading and returns the event.
func (st *Station) meterTick(conn *Connector, now time.Time, g *rng) Event {
	s := conn.session
	dtHours := now.Sub(conn.lastMeterAt).Hours()
	if dtHours <= 0 {
		dtHours = float64(g.meterInterval()) / float64(time.Hour)
	}
	power := chargingPower(s)
	added := power * dtHours
	s.EnergyKWh += added
	s.Soc = math.Min(100, s.Soc+(added/s.BatteryKWh)*100)

	volt := nominalVoltage(conn)
	cur := 0.0
	if volt > 0 {
		cur = power * 1000 / volt
	}
	conn.lastMeterAt = now
	conn.nextMeterAt = now.Add(g.meterInterval())

	e := st.baseEvent(TypeMeterUpdate, conn, now)
	e.SessionID = s.ID
	e.TariffID = s.TariffID
	e.Meter = &Meter{
		PowerKW: round1(power), EnergyKWh: round3(s.EnergyKWh),
		VoltageV: volt, CurrentA: round1(cur), SocPercent: int(s.Soc),
	}
	return e
}

func effectiveStopAt(s *Session, now time.Time) time.Time {
	if s.Soc < 100 && s.EndsAt.Before(now) {
		return s.EndsAt
	}
	return now
}

// stopSession ends a session and emits the SESSION_STOP with totals + cost.
func (st *Station) stopSession(conn *Connector, now time.Time, cfg *Config) Event {
	s := conn.session
	// Advance energy/SoC across the final interval since the last METER_UPDATE
	// (mirrors meterTick) so the stop total and billed cost include the tail.
	// The caller passes the effective stop timestamp: duration stops pass EndsAt,
	// while fault/full-battery stops pass the tick time. A full battery gets no
	// extra tail energy because chargingPower still tapers to >0 at 100% SoC.
	if s.Soc < 100 && now.After(conn.lastMeterAt) {
		added := chargingPower(s) * now.Sub(conn.lastMeterAt).Hours()
		s.EnergyKWh += added
		s.Soc = math.Min(100, s.Soc+(added/s.BatteryKWh)*100)
		conn.lastMeterAt = now
	}

	t := tariffByID[s.TariffID]
	rate := t.Base
	peakPriced := false
	// Window is config-sourced (same as pickTariff / arrival weighting). The
	// multiplier still applies in-window for every tariff, but the flag is gated
	// on a real premium: only PeakMult > 1 is peak-priced. standard/fleet bill at
	// base (1.00) and off-peak is a discount (0.80), so those are NOT peak even
	// in-window. A4's peak-revenue share reads this flag; a clock flag overstated it.
	if inPeakWindow(now.Hour(), cfg) {
		rate = t.Base * t.PeakMult
		peakPriced = t.PeakMult > 1.0
	}
	cost := s.EnergyKWh * rate

	e := st.baseEvent(TypeSessionStop, conn, now)
	e.SessionID = s.ID
	e.TariffID = s.TariffID
	e.CostEur = round2(cost)
	e.IsPeakPriced = peakPriced
	e.Meter = &Meter{
		PowerKW: 0, EnergyKWh: round3(s.EnergyKWh),
		VoltageV: nominalVoltage(conn), CurrentA: 0, SocPercent: int(s.Soc),
	}
	conn.session = nil
	conn.status = "Available"
	return e
}

func (st *Station) heartbeat(now time.Time) Event {
	e := Event{
		EventID:     uuid.NewString(),
		EventType:   TypeHeartbeat,
		StationID:   st.ID,
		ConnectorID: 0, // station-level
		Timestamp:   now.UTC().Format("2006-01-02T15:04:05.000Z07:00"),
		OperatorID:  st.OperatorID,
		Location:    Location{Lat: st.Lat, Lon: st.Lon, City: st.City, Country: st.Country},
	}
	return e
}

func (st *Station) statusChange(conn *Connector, now time.Time, status string) Event {
	e := st.baseEvent(TypeStatusChange, conn, now)
	e.Status = status
	if conn.session != nil {
		e.SessionID = conn.session.ID
	}
	return e
}

func (st *Station) faultAlert(conn *Connector, now time.Time, g *rng) Event {
	f := faultCodes[g.r.Intn(len(faultCodes))]
	e := st.baseEvent(TypeFaultAlert, conn, now)
	e.Fault = &Fault{ErrorCode: f.code, Component: f.component}
	return e
}

// ---------------------------------------------------------------------------
// physics-ish helpers
// ---------------------------------------------------------------------------

// chargingPower models a simple taper: full target up to 80% SoC, then ramps down
// to ~8% of target by 100% SoC (protects the cells, matches real CC/CV curves).
func chargingPower(s *Session) float64 {
	if s.Soc < 80 {
		return s.TargetPowerKW
	}
	frac := (100 - s.Soc) / 20 // 1.0 at 80%, 0.0 at 100%
	return s.TargetPowerKW * (0.08 + 0.92*frac)
}

func nominalVoltage(c *Connector) float64 {
	if c.Type == "DC" {
		return 400
	}
	return 230
}

func (g *rng) meterInterval() time.Duration {
	lo, hi := g.cfg.Simulator.MeterIntervalSec[0], g.cfg.Simulator.MeterIntervalSec[1]
	if hi <= lo {
		hi = lo + 1
	}
	return time.Duration(lo+g.r.Intn(hi-lo+1)) * time.Second
}

func round1(f float64) float64 { return math.Round(f*10) / 10 }
func round2(f float64) float64 { return math.Round(f*100) / 100 }
func round3(f float64) float64 { return math.Round(f*1000) / 1000 }
