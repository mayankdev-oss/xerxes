package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func setupTestApp(t *testing.T) (*App, func()) {
	// Automatically load .env for test runs
	_ = godotenv.Load()

	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		t.Skip("Skipping integration test: MONGO_URI not set in .env or environment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	testDBName := "kinetic_backup_test_db"
	app, err := NewApp(ctx, uri, testDBName)
	if err != nil {
		cancel()
		t.Fatalf("Failed to connect to MongoDB Atlas: %v", err)
	}

	cleanup := func() {
		_ = app.DB.Drop(context.Background())
		_ = app.Client.Disconnect(context.Background())
		cancel()
	}
	return app, cleanup
}

func TestBackupVaultLifecycleAndEdgeCases(t *testing.T) {
	app, cleanup := setupTestApp(t)
	defer cleanup()

	ts := httptest.NewServer(app.Routes())
	defer ts.Close()

	// 1. Edge Case: Invalid folder name (Path Traversal attempt)
	resp, _ := http.Post(ts.URL+"/api/folders", "application/json", strings.NewReader(`{"name":"../etc"}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Expected 400 for path traversal folder name, got %d", resp.StatusCode)
	}

	// 2. Create valid folder in /root
	resp, _ = http.Post(ts.URL+"/api/folders", "application/json", strings.NewReader(`{"name":"invoices_2026"}`))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Expected 201 for folder creation, got %d", resp.StatusCode)
	}

	// 3. Edge Case: Folder name conflict in /root
	resp, _ = http.Post(ts.URL+"/api/folders", "application/json", strings.NewReader(`{"name":"invoices_2026"}`))
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("Expected 409 Conflict for duplicate folder name, got %d", resp.StatusCode)
	}

	// 4. Edge Case: Upload exceeding 25MB limit (25MB + 1KB)
	var largeBuf bytes.Buffer
	mwLarge := multipart.NewWriter(&largeBuf)
	_ = mwLarge.WriteField("folder", "invoices_2026")
	fwLarge, _ := mwLarge.CreateFormFile("file", "oversized.bin")
	_, _ = fwLarge.Write(make([]byte, MaxUploadSize+1024))
	mwLarge.Close()

	resp, _ = http.Post(ts.URL+"/api/upload", mwLarge.FormDataContentType(), &largeBuf)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("Expected 413 for >25MB payload, got %d", resp.StatusCode)
	}

	// 5. Valid File Upload
	var validBuf bytes.Buffer
	mw := multipart.NewWriter(&validBuf)
	_ = mw.WriteField("folder", "invoices_2026")
	fw, _ := mw.CreateFormFile("file", "q1_backup.txt")
	fileContent := "Kinetic MongoDB Atlas Backup Content 2026"
	_, _ = fw.Write([]byte(fileContent))
	mw.Close()

	resp, _ = http.Post(ts.URL+"/api/upload", mw.FormDataContentType(), &validBuf)
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 201 for valid upload, got %d: %s", resp.StatusCode, string(body))
	}

	// 6. GET File Read & Download
	resp, _ = http.Get(ts.URL + "/api/files/invoices_2026/q1_backup.txt?download=1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 on GET file, got %d", resp.StatusCode)
	}
	downloaded, _ := io.ReadAll(resp.Body)
	if string(downloaded) != fileContent {
		t.Fatalf("Downloaded content mismatch: got %q, want %q", string(downloaded), fileContent)
	}

	// 7. DELETE Type 2: Wipe specific folder
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/api/folders/invoices_2026", nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 on folder wipe, got %d", resp.StatusCode)
	}

	// Verify file is gone after folder wipe
	resp, _ = http.Get(ts.URL + "/api/files/invoices_2026/q1_backup.txt")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 after folder wipe, got %d", resp.StatusCode)
	}

	// 8. DELETE Type 1: Drop entire DB
	_, _ = http.Post(ts.URL+"/api/folders", "application/json", strings.NewReader(`{"name":"temp_folder"}`))
	reqDrop, _ := http.NewRequest(http.MethodDelete, ts.URL+"/api/db", nil)
	resp, _ = http.DefaultClient.Do(reqDrop)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 on DB drop, got %d", resp.StatusCode)
	}

	// Verify state is empty
	resp, _ = http.Get(ts.URL + "/api/state")
	var state struct {
		Folders []Folder   `json:"folders"`
		Files   []FileMeta `json:"files"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&state)
	if len(state.Folders) != 0 || len(state.Files) != 0 {
		t.Fatalf("Expected empty DB after drop, got %d folders, %d files", len(state.Folders), len(state.Files))
	}
}
