package transform

import "time"

// TimeLayout matches the simulator's event timestamp format exactly (RFC3339 ms, UTC).
const TimeLayout = "2006-01-02T15:04:05.000Z07:00"

// ---------------------------------------------------------------------------
// Nested wire format (input, charging-events-raw). Mirrors the simulator's
// model.go: optional sub-objects are pointers so "absent" deserialises to nil.
// ---------------------------------------------------------------------------

type Location struct {
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	City    string  `json:"city"`
	Country string  `json:"country"`
}

type Meter struct {
	PowerKW    float64 `json:"power_kw"`
	EnergyKWh  float64 `json:"energy_kwh"`
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
	EventID     string   `json:"event_id"`
	EventType   string   `json:"event_type"`
	StationID   string   `json:"station_id"`
	ConnectorID int      `json:"connector_id"`
	SessionID   string   `json:"session_id,omitempty"`
	Timestamp   string   `json:"timestamp"`
	OperatorID  string   `json:"operator_id"`
	Location    Location `json:"location"`
	Meter       *Meter   `json:"meter,omitempty"`
	Vehicle     *Vehicle `json:"vehicle,omitempty"`
	TariffID    string   `json:"tariff_id,omitempty"`
	CostEur     float64  `json:"cost_eur,omitempty"`
	Fault       *Fault   `json:"fault,omitempty"`
	Status      string   `json:"status,omitempty"`
}

// ---------------------------------------------------------------------------
// Flat output (charging-events-clean). Field tags MUST match the ev.events_queue
// columns 1:1 (ClickHouse JSONEachRow matches by name). NO omitempty: every column
// is present on every row so ClickHouse never sees a missing field.
// ---------------------------------------------------------------------------

type CleanEvent struct {
	EventID      string  `json:"event_id"`
	EventType    string  `json:"event_type"`
	StationID    string  `json:"station_id"`
	ConnectorID  int     `json:"connector_id"`
	SessionID    string  `json:"session_id"`
	Timestamp    string  `json:"timestamp"`
	IngestedAt   string  `json:"ingested_at"`
	OperatorID   string  `json:"operator_id"`
	Lat          float64 `json:"lat"`
	Lon          float64 `json:"lon"`
	City         string  `json:"city"`
	Country      string  `json:"country"`
	PowerKW      float64 `json:"power_kw"`
	EnergyKWh    float64 `json:"energy_kwh"`
	VoltageV     float64 `json:"voltage_v"`
	CurrentA     float64 `json:"current_a"`
	SocPercent   int     `json:"soc_percent"`
	VehicleBrand string  `json:"vehicle_brand"`
	VehicleModel string  `json:"vehicle_model"`
	EvID         string  `json:"ev_id"`
	TariffID     string  `json:"tariff_id"`
	CostEur      float64 `json:"cost_eur"`
	ErrorCode    string  `json:"error_code"`
	Component    string  `json:"component"`
	Status       string  `json:"status"`
}

// Flatten lifts the nested sub-objects into the flat analytics row and stamps the
// processing time. A nil sub-object leaves its fields at their zero value, which is
// exactly what ClickHouse should store when an event type omits them.
func Flatten(e Event, ingestedAt time.Time) CleanEvent {
	ce := CleanEvent{
		EventID:     e.EventID,
		EventType:   e.EventType,
		StationID:   e.StationID,
		ConnectorID: e.ConnectorID,
		SessionID:   e.SessionID,
		Timestamp:   e.Timestamp,
		IngestedAt:  ingestedAt.UTC().Format(TimeLayout),
		OperatorID:  e.OperatorID,
		Lat:         e.Location.Lat,
		Lon:         e.Location.Lon,
		City:        e.Location.City,
		Country:     e.Location.Country,
		TariffID:    e.TariffID,
		CostEur:     e.CostEur,
		Status:      e.Status,
	}
	if e.Meter != nil {
		ce.PowerKW = e.Meter.PowerKW
		ce.EnergyKWh = e.Meter.EnergyKWh
		ce.VoltageV = e.Meter.VoltageV
		ce.CurrentA = e.Meter.CurrentA
		ce.SocPercent = e.Meter.SocPercent
	}
	if e.Vehicle != nil {
		ce.VehicleBrand = e.Vehicle.Brand
		ce.VehicleModel = e.Vehicle.Model
		ce.EvID = e.Vehicle.EvID
	}
	if e.Fault != nil {
		ce.ErrorCode = e.Fault.ErrorCode
		ce.Component = e.Fault.Component
	}
	return ce
}
