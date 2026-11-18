package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type ToolCall struct {
	Tool      string         `json:"tool"`
	Arguments map[string]any `json:"arguments"`
}

var toolSchemas = []map[string]any{
	{"name": "calculator", "description": "Evaluate a bounded arithmetic expression", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"expression": map[string]any{"type": "string", "maxLength": 256}}, "required": []string{"expression"}, "additionalProperties": false}},
	{"name": "search", "description": "Search documents with caller permissions", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string", "maxLength": 8000}}, "required": []string{"query"}, "additionalProperties": false}},
	{"name": "write_note", "description": "Persist a note; requires configured approval", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"title": map[string]any{"type": "string", "maxLength": 100}, "content": map[string]any{"type": "string", "maxLength": 10000}}, "required": []string{"title", "content"}, "additionalProperties": false}},
}

func validTool(name string) bool {
	return name == "calculator" || name == "search" || name == "write_note"
}
func validateCall(c ToolCall) error {
	if !validTool(c.Tool) {
		return errors.New("Tool not approved")
	}
	keys := []string{}
	switch c.Tool {
	case "calculator":
		keys = []string{"expression"}
		if len(str(c.Arguments["expression"])) < 1 || len(str(c.Arguments["expression"])) > 256 {
			return errors.New("expression must be 1–256 bytes")
		}
	case "search":
		keys = []string{"query"}
		if len(str(c.Arguments["query"])) < 1 || len(str(c.Arguments["query"])) > 8000 {
			return errors.New("query must be 1–8000 bytes")
		}
	case "write_note":
		keys = []string{"title", "content"}
		if len(str(c.Arguments["title"])) < 1 || len(str(c.Arguments["title"])) > 100 || len(str(c.Arguments["content"])) < 1 || len(str(c.Arguments["content"])) > 10000 {
			return errors.New("title/content required within limits")
		}
	}
	if len(c.Arguments) != len(keys) {
		return errors.New("Unexpected tool arguments")
	}
	for _, k := range keys {
		if _, ok := c.Arguments[k].(string); !ok {
			return errors.New("Tool arguments must be strings")
		}
	}
	return nil
}
func calculate(expr string) (float64, error) {
	if len(expr) > 256 {
		return 0, errors.New("Expression too long")
	}
	node, e := parser.ParseExpr(expr)
	if e != nil {
		return 0, e
	}
	return evalNode(node, 0)
}
func evalNode(node ast.Expr, depth int) (float64, error) {
	if depth > 32 {
		return 0, errors.New("Expression too deep")
	}
	switch n := node.(type) {
	case *ast.BasicLit:
		if n.Kind != gotoken.INT && n.Kind != gotoken.FLOAT {
			return 0, errors.New("Only numbers allowed")
		}
		return strconv.ParseFloat(n.Value, 64)
	case *ast.ParenExpr:
		return evalNode(n.X, depth+1)
	case *ast.UnaryExpr:
		v, e := evalNode(n.X, depth+1)
		if e != nil {
			return 0, e
		}
		if n.Op == gotoken.SUB {
			return -v, nil
		}
		if n.Op == gotoken.ADD {
			return v, nil
		}
	case *ast.BinaryExpr:
		x, e := evalNode(n.X, depth+1)
		if e != nil {
			return 0, e
		}
		y, e := evalNode(n.Y, depth+1)
		if e != nil {
			return 0, e
		}
		var v float64
		switch n.Op {
		case gotoken.ADD:
			v = x + y
		case gotoken.SUB:
			v = x - y
		case gotoken.MUL:
			v = x * y
		case gotoken.QUO:
			if y == 0 {
				return 0, errors.New("Division by zero")
			}
			v = x / y
		default:
			return 0, errors.New("Unsupported operator")
		}
		if math.IsInf(v, 0) || math.IsNaN(v) {
			return 0, errors.New("Nonfinite result")
		}
		return v, nil
	}
	return 0, errors.New("Only arithmetic operators and literals allowed")
}
func (a *App) agents(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		a.list(w, r, "agent")
		return
	}
	if !writeAccess(w, r) {
		return
	}
	var in struct {
		Name  string   `json:"name"`
		Tools []string `json:"tools"`
	}
	if !decode(w, r, &in) {
		return
	}
	if len(in.Name) < 1 || len(in.Name) > 100 || len(in.Tools) < 1 || len(in.Tools) > 3 {
		fail(w, 422, "Name and 1–3 approved tools required")
		return
	}
	for _, t := range in.Tools {
		if !validTool(t) {
			fail(w, 422, "Unknown tool: "+t)
			return
		}
	}
	row := Record{ID: token()[:24], Kind: "agent", Owner: current(r).ID, Status: "configured", Data: map[string]any{"name": in.Name, "tools": in.Tools, "schemas": toolSchemas}}
	if e := a.store.Put(r.Context(), row); e != nil {
		fail(w, 500, e.Error())
		return
	}
	a.store.Audit(r.Context(), current(r).ID, "agent.create", row.ID, row.Data)
	respond(w, 201, row)
}
func (a *App) agentRun(w http.ResponseWriter, r *http.Request) {
	if !writeAccess(w, r) {
		return
	}
	agent, e := a.store.Access(r.Context(), r.PathValue("id"), "agent", current(r), false)
	if e != nil {
		accessError(w, e)
		return
	}
	var in struct {
		Calls []ToolCall `json:"calls"`
	}
	if !decode(w, r, &in) {
		return
	}
	if len(in.Calls) < 1 || len(in.Calls) > 8 {
		fail(w, 422, "1–8 calls required")
		return
	}
	a.newRun(w, r, agent, in.Calls)
}
func (a *App) newRun(w http.ResponseWriter, r *http.Request, agent Record, calls []ToolCall) {
	sensitive := false
	for _, c := range calls {
		if e := validateCall(c); e != nil {
			fail(w, 422, e.Error())
			return
		}
		allowed := false
		for _, tool := range array(agent.Data["tools"]) {
			if tool == c.Tool {
				allowed = true
			}
		}
		if !allowed {
			fail(w, 403, "Tool not configured for this agent")
			return
		}
		if c.Tool == "write_note" {
			sensitive = true
		}
	}
	data := map[string]any{"agent": agent.ID, "calls": calls, "logs": []any{}, "caller_role": current(r).Role}
	status := "running"
	if sensitive && a.cfg.WriteApproval {
		status = "awaiting_approval"
	}
	row := Record{ID: token()[:24], Kind: "run", Owner: current(r).ID, Status: status, Data: data}
	if e := a.store.Put(r.Context(), row); e != nil {
		fail(w, 500, e.Error())
		return
	}
	a.store.Audit(r.Context(), current(r).ID, "agent.run", row.ID, map[string]any{"calls": calls})
	if status == "running" {
		row = a.executeRun(r.Context(), row, current(r), calls)
	}
	respond(w, 201, row)
}
func (a *App) executeRun(ctx context.Context, row Record, u User, calls []ToolCall) Record {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	logs := []any{}
	row.Status = "running"
	for _, call := range calls {
		start := time.Now()
		result, e := a.tool(ctx, u, call)
		entry := map[string]any{"tool": call.Tool, "input": call.Arguments, "output": result, "duration_ms": time.Since(start).Milliseconds()}
		if e != nil {
			entry["error"] = e.Error()
			row.Status = "failed"
			row.Error = e.Error()
		}
		logs = append(logs, entry)
		row.Data["logs"] = logs
		save, cancelSave := context.WithTimeout(context.Background(), 5*time.Second)
		a.store.Put(save, row)
		a.store.Audit(save, u.ID, "tool.execute", row.ID, entry)
		cancelSave()
		if e != nil {
			break
		}
	}
	if row.Status == "running" {
		row.Status = "succeeded"
	}
	save, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	a.store.Put(save, row)
	return row
}
func (a *App) tool(ctx context.Context, u User, c ToolCall) (any, error) {
	if err := ctx.Err(); err != nil { return nil, err }
	if e := validateCall(c); e != nil {
		return nil, e
	}
	switch c.Tool {
	case "calculator":
		return calculate(str(c.Arguments["expression"]))
	case "search":
		return a.retrieve(ctx, u, str(c.Arguments["query"]), 4)
	case "write_note":
		if u.Role != "administrator" && u.Role != "developer" {
			return nil, errForbidden
		}
		row := Record{ID: token()[:24], Kind: "note", Owner: u.ID, Status: "saved", Data: c.Arguments}
		return row, a.store.PutAudited(ctx, row, u.ID, "tool.write_note", c.Arguments)
	}
	return nil, errors.New("Unknown tool")
}
func (a *App) runs(w http.ResponseWriter, r *http.Request) { a.list(w, r, "run") }
func (a *App) approval(w http.ResponseWriter, r *http.Request) {
	if !require(w, r, "administrator") {
		return
	}
	var in struct {
		Approve bool `json:"approve"`
	}
	if !decode(w, r, &in) {
		return
	}
	row, e := a.store.Get(r.Context(), r.PathValue("id"))
	if e != nil || row.Kind != "run" {
		fail(w, 404, "Run not found")
		return
	}
	row, e = a.store.ResolveApproval(r.Context(), row.ID, current(r).ID, in.Approve)
	if errors.Is(e, errConflict) {
		fail(w, 409, "Approval already resolved")
		return
	}
	if e != nil { fail(w, 500, "Cannot persist approval audit"); return }
	if in.Approve {
		u := User{ID: row.Owner, Role: str(row.Data["caller_role"])}
		if u.ID != "bootstrap-admin" {
			resolved, e := a.store.KeyByID(r.Context(), u.ID)
			if e != nil {
				row.Status = "rejected"
				row.Error = "Caller key revoked"
				a.store.Put(r.Context(), row)
				respond(w, 200, row)
				return
			}
			u.Role = resolved.Role
		}
		b, _ := json.Marshal(row.Data["calls"])
		var calls []ToolCall
		json.Unmarshal(b, &calls)
		row = a.executeRun(r.Context(), row, u, calls)
	}
	respond(w, 200, row)
}
func (a *App) mcp(w http.ResponseWriter, r *http.Request) {
	if !writeAccess(w, r) {
		return
	}
	var in struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      any             `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}
	if !decode(w, r, &in) {
		return
	}
	reply := func(result any) { respond(w, 200, map[string]any{"jsonrpc": "2.0", "id": in.ID, "result": result}) }
	if in.JSONRPC != "2.0" {
		fail(w, 422, "JSON-RPC 2.0 required")
		return
	}
	switch in.Method {
	case "initialize":
		reply(map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "tensorforge", "version": "0.1.0"}})
	case "notifications/initialized":
		w.WriteHeader(202)
	case "tools/list":
		reply(map[string]any{"tools": toolSchemas})
	case "tools/call":
		var p struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if json.Unmarshal(in.Params, &p) != nil {
			fail(w, 422, "Invalid tool call")
			return
		}
		if p.Name == "write_note" {
			a.store.Audit(r.Context(), current(r).ID, "mcp.write_rejected", requestID(r), map[string]any{"tool": p.Name, "input": p.Arguments})
			reply(map[string]any{"isError": true, "content": []any{map[string]string{"type": "text", "text": "Write actions require an agent run and explicit approval via /api/runs/{id}/approval"}}})
			return
		}
		out, e := a.tool(r.Context(), current(r), ToolCall{p.Name, p.Arguments})
		entry := map[string]any{"tool": p.Name, "input": p.Arguments, "output": out}
		if e != nil { entry["error"] = e.Error() }
		a.store.Audit(r.Context(), current(r).ID, "mcp.tool", requestID(r), entry)
		text := ""
		if e != nil {
			text = e.Error()
		} else {
			b, _ := json.Marshal(out)
			text = string(b)
		}
		reply(map[string]any{"isError": e != nil, "content": []any{map[string]string{"type": "text", "text": text}}})
	default:
		respond(w, 200, map[string]any{"jsonrpc": "2.0", "id": in.ID, "error": map[string]any{"code": -32601, "message": "Unknown method: " + strings.TrimSpace(fmt.Sprint(in.Method))}})
	}
}
