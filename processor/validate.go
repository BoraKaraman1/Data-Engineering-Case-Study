package main

import (
	"encoding/json"
	"fmt"
	"time"

	"chargesquare/processor/transform"
)

var validEventTypes = map[string]bool{
	"SESSION_START": true,
	"METER_UPDATE":  true,
	"STATUS_CHANGE": true,
	"SESSION_STOP":  true,
	"HEARTBEAT":     true,
	"FAULT_ALERT":   true,
}

// validStatuses is the closed connector-status vocabulary. STATUS_CHANGE must carry one
// of these (an allowlist, not just "non-empty"), so A2's uptime timeline is trustworthy.
var validStatuses = map[string]bool{
	"Available": true,
	"Charging":  true,
	"Faulted":   true,
}

// ValidationError carries a machine rule name (for the metric label) and a human
// message (for the dead-letter record).
type ValidationError struct {
	Rule string
	Msg  string
}

func (e *ValidationError) Error() string { return e.Msg }

func verr(rule, format string, args ...any) *ValidationError {
	return &ValidationError{Rule: rule, Msg: fmt.Sprintf(format, args...)}
}

// Decode parses raw JSON into the nested event shape.
func Decode(raw []byte) (transform.Event, *ValidationError) {
	var e transform.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return e, verr("invalid_json", "invalid json: %v", err)
	}
	return e, nil
}

// Validate enforces schema + referential rules. First failure wins; the returned rule
// drives processor_validation_errors_total{rule} and the dead-letter record. The rules
// are aligned to exactly what the simulator emits per type, so they reject corrupt rows
// without false-positiving on well-formed ones.
func Validate(e transform.Event, reg *Registry) *ValidationError {
	if e.EventID == "" {
		return verr("missing_event_id", "missing event_id")
	}
	if !validEventTypes[e.EventType] {
		return verr("unknown_event_type", "unknown event_type %q", e.EventType)
	}
	numConn, ok := reg.Station(e.StationID)
	if !ok {
		return verr("unknown_station", "unknown station_id %q", e.StationID)
	}
	if _, err := time.Parse(time.RFC3339, e.Timestamp); err != nil {
		return verr("bad_timestamp", "unparseable timestamp %q", e.Timestamp)
	}
	if e.OperatorID == "" {
		return verr("missing_operator", "missing operator_id")
	}
	if e.Location.City == "" || e.Location.Country == "" {
		return verr("missing_location", "missing location city/country")
	}
	if e.Location.Lat < -90 || e.Location.Lat > 90 || e.Location.Lon < -180 || e.Location.Lon > 180 {
		return verr("bad_geo", "lat/lon out of range")
	}

	// HEARTBEAT is station-level (connector 0); everything else must name a real
	// connector on that station (1..numConn), which also keeps the UInt8 cast safe.
	if e.EventType == "HEARTBEAT" {
		if e.ConnectorID != 0 {
			return verr("bad_connector", "HEARTBEAT connector_id must be 0, got %d", e.ConnectorID)
		}
	} else if e.ConnectorID < 1 || e.ConnectorID > numConn {
		return verr("connector_out_of_range", "connector_id %d exceeds station connector count %d", e.ConnectorID, numConn)
	}

	// A tariff, whenever present, must be a real one.
	if e.TariffID != "" && !reg.TariffKnown(e.TariffID) {
		return verr("unknown_tariff", "unknown tariff_id %q", e.TariffID)
	}

	switch e.EventType {
	case "SESSION_START":
		if e.SessionID == "" {
			return verr("missing_session", "SESSION_START missing session_id")
		}
		if e.Vehicle == nil || e.Vehicle.Brand == "" {
			return verr("missing_vehicle", "SESSION_START missing vehicle brand")
		}
		if e.TariffID == "" {
			return verr("missing_tariff", "SESSION_START missing tariff_id")
		}
		if e.Meter == nil {
			return verr("missing_meter", "SESSION_START missing meter")
		}
	case "METER_UPDATE":
		if e.SessionID == "" {
			return verr("missing_session", "METER_UPDATE missing session_id")
		}
		if e.TariffID == "" {
			return verr("missing_tariff", "METER_UPDATE missing tariff_id")
		}
		if e.Meter == nil {
			return verr("missing_meter", "METER_UPDATE missing meter")
		}
	case "SESSION_STOP":
		if e.SessionID == "" {
			return verr("missing_session", "SESSION_STOP missing session_id")
		}
		if e.TariffID == "" {
			return verr("missing_tariff", "SESSION_STOP missing tariff_id")
		}
		if e.Meter == nil {
			return verr("missing_meter", "SESSION_STOP missing meter")
		}
		if e.CostEur < 0 {
			return verr("bad_cost", "negative cost_eur %v", e.CostEur)
		}
	case "STATUS_CHANGE":
		if !validStatuses[e.Status] {
			return verr("bad_status", "invalid status %q", e.Status)
		}
	case "FAULT_ALERT":
		if e.Fault == nil || e.Fault.ErrorCode == "" {
			return verr("missing_fault", "FAULT_ALERT missing fault error_code")
		}
	}

	if e.Meter != nil {
		if err := validateMeter(e.Meter); err != nil {
			return err
		}
	}
	return nil
}

func validateMeter(m *transform.Meter) *ValidationError {
	if m.SocPercent < 0 || m.SocPercent > 100 {
		return verr("soc_out_of_range", "soc_percent %d out of range [0,100]", m.SocPercent)
	}
	if m.PowerKW < 0 || m.PowerKW > 1000 {
		return verr("bad_power", "power_kw %v out of range [0,1000]", m.PowerKW)
	}
	if m.EnergyKWh < 0 {
		return verr("bad_energy", "negative energy_kwh %v", m.EnergyKWh)
	}
	if m.VoltageV < 0 {
		return verr("bad_voltage", "negative voltage_v %v", m.VoltageV)
	}
	if m.CurrentA < 0 {
		return verr("bad_current", "negative current_a %v", m.CurrentA)
	}
	return nil
}
